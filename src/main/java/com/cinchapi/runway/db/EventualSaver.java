/*
 * Copyright (c) 2013-2026 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cinchapi.runway.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.CommandGroup;
import com.cinchapi.concourse.lang.Criteria;
import com.google.common.base.Preconditions;

/**
 * A {@link Saver} that batches save-time database interaction into two
 * {@link CommandGroup} submissions: one carrying {@link #stage()} and all
 * validation reads, the other carrying all writes and the terminal commit.
 * <p>
 * Recording calls touch only client-side state. Server-side work happens
 * inside {@link #commit()}, which:
 * <ol>
 * <li>Submits the reads {@link CommandGroup} as one round trip,</li>
 * <li>Drains queued {@link Consumer validator} callbacks against the result
 * list &mdash; any throw propagates out and prevents the second submit,</li>
 * <li>Appends a commit command to the writes {@link CommandGroup} and submits
 * it as the second round trip,</li>
 * <li>Returns the commit's boolean from the last entry in the writes
 * result.</li>
 * </ol>
 * <p>
 * If no validation reads are recorded the reads submission still runs
 * because it carries {@link #stage()} &mdash; the round trip is required to
 * open the staged transaction on the server before the writes submission
 * can commit it.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class EventualSaver implements Saver {

    /**
     * The {@link Concourse} connection used to submit the read and write
     * groups and against which any {@link #abort()} executes.
     */
    private final Concourse concourse;

    /**
     * The {@link CommandGroup} accumulating {@link #stage()} and all
     * validation reads; submitted as the first round trip of
     * {@link #commit()}.
     */
    private final CommandGroup reads;

    /**
     * The {@link CommandGroup} accumulating writes and the terminal commit;
     * submitted as the second round trip of {@link #commit()}.
     */
    private final CommandGroup writes;

    /**
     * Validator callbacks queued by {@link #audit audit} and {@link #find
     * find}, in recording order. Each callback receives the full reads-
     * submission result list and extracts its own slot.
     */
    private final List<Consumer<List<Object>>> validations;

    /**
     * Whether the reads {@link CommandGroup} has been submitted to the
     * server. Used by {@link #abort()} to decide whether a server-side
     * staged transaction exists that needs rolling back.
     */
    private boolean readsSubmitted;

    /**
     * Construct a new {@link EventualSaver} that submits against
     * {@code concourse}.
     *
     * @param concourse the {@link Concourse} connection that hosts the
     *            staged transaction; must not be {@code null}
     */
    public EventualSaver(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
        this.reads = concourse.prepare();
        this.writes = concourse.prepare();
        this.validations = new ArrayList<>();
        this.readsSubmitted = false;
    }

    @Override
    public Concourse concourse() {
        return concourse;
    }

    @Override
    public void stage() {
        reads.stage();
    }

    @Override
    public void audit(long record, Consumer<Map<Timestamp, String>> validator) {
        Preconditions.checkNotNull(validator);
        int slot = reads.commands().size();
        reads.audit(record);
        validations.add(results -> {
            @SuppressWarnings("unchecked")
            Map<Timestamp, String> result = (Map<Timestamp, String>) results
                    .get(slot);
            validator.accept(result);
        });
    }

    @Override
    public void find(Criteria criteria, Consumer<Set<Long>> validator) {
        Preconditions.checkNotNull(validator);
        int slot = reads.commands().size();
        reads.find(criteria);
        validations.add(results -> {
            @SuppressWarnings("unchecked")
            Set<Long> result = (Set<Long>) results.get(slot);
            validator.accept(result);
        });
    }

    @Override
    public void set(String key, Object value, long record) {
        writes.set(key, value, record);
    }

    @Override
    public void clear(String key, long record) {
        writes.clear(key, record);
    }

    @Override
    public void clear(long record) {
        writes.clear(record);
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        writes.verifyOrSet(key, value, record);
    }

    @Override
    public void reconcile(String key, long record, Object[] values) {
        writes.reconcile(key, record, values);
    }

    @Override
    public boolean commit() {
        if(reads.commands().size() > 0) {
            List<Object> readResults = concourse.submit(reads);
            readsSubmitted = true;
            for (Consumer<List<Object>> validation : validations) {
                validation.accept(readResults);
            }
        }
        writes.commit();
        List<Object> writeResults = concourse.submit(writes);
        return (Boolean) writeResults.get(writeResults.size() - 1);
    }

    @Override
    public void abort() {
        if(readsSubmitted) {
            concourse.abort();
        }
    }

}
