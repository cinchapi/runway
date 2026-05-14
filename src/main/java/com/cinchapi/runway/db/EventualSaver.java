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
import java.util.Arrays;
import java.util.Collection;
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
 * A {@link Saver} that batches save-pipeline interaction into the smallest
 * number of {@link CommandGroup} submissions a given save permits.
 * <p>
 * Recording calls accumulate deferred operations against this {@link Saver}
 * rather than touching the server. Server work happens at one of two points:
 * </p>
 * <ul>
 * <li><strong>Inside {@link #commit()}.</strong> If only writes were recorded,
 * one {@link CommandGroup} carries {@link #stage()} plus every write plus the
 * terminal commit and submits in a single round trip. If validation reads were
 * also recorded, two {@link CommandGroup CommandGroups} are used: the first
 * carries {@link #stage()} plus the recorded {@link #audit audits} and
 * {@link #find finds}, runs every queued {@link Consumer validator} against the
 * result, and only then is the writes-plus-commit {@link CommandGroup}
 * submitted.</li>
 * <li><strong>Inside {@link #select(String, Criteria, Consumer)
 * select}.</strong> Save-time {@link #select select} reads drive control flow
 * rather than a throw/no-throw validation, so the {@link Consumer} must observe
 * its result before the recording call returns. Calling
 * {@link #select(String, Criteria, Consumer) select} therefore submits any
 * reads accumulated so far &mdash; {@link #stage()}, queued {@link #audit
 * audits}, queued {@link #find finds}, and this {@link #select select} itself
 * &mdash; runs all the queued {@link Consumer Consumers} in recording order,
 * and then resets the read-side state so subsequent recordings start a fresh
 * batch.</li>
 * </ul>
 * <p>
 * If any {@link Consumer Consumer} throws during a flush, the corresponding
 * submission has already opened the staged transaction on the server; the
 * caller is responsible for calling {@link #abort()} after handling the
 * exception.
 * </p>
 * <p>
 * This {@link Saver} is <strong>not thread-safe</strong>.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class EventualSaver implements Saver {

    /**
     * The {@link Concourse} connection used to submit and against which
     * {@link #abort()} executes.
     */
    private final Concourse concourse;

    /**
     * Whether {@link #stage()} has been called.
     */
    private boolean stageRequested;

    /**
     * Whether {@link #stage()} has been included in a submitted
     * {@link CommandGroup}; once {@code true} subsequent flushes and the writes
     * submission do not re-record it.
     */
    private boolean stageSubmitted;

    /**
     * Deferred read recordings in the active batch. Each entry records its own
     * slot position when it runs against the active read {@link CommandGroup}.
     */
    private final List<Consumer<CommandGroup>> deferredReadOps;

    /**
     * Validator {@link Consumer Consumers} paired with the deferred reads in
     * the active batch. Run in recording order against the submitted result
     * list.
     */
    private final List<Consumer<List<Object>>> pendingValidators;

    /**
     * Deferred write recordings accumulated for the writes submission.
     */
    private final List<Consumer<CommandGroup>> deferredWriteOps;

    /**
     * Construct a new {@link EventualSaver} that submits against
     * {@code concourse}.
     *
     * @param concourse the {@link Concourse} connection that hosts the staged
     *            transaction; must not be {@code null}
     */
    public EventualSaver(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
        this.stageRequested = false;
        this.stageSubmitted = false;
        this.deferredReadOps = new ArrayList<>();
        this.pendingValidators = new ArrayList<>();
        this.deferredWriteOps = new ArrayList<>();
    }

    @Override
    public Concourse concourse() {
        return concourse;
    }

    @Override
    public void stage() {
        stageRequested = true;
    }

    @Override
    public void audit(long record, Consumer<Map<Timestamp, String>> validator) {
        Preconditions.checkNotNull(validator);
        int[] slot = new int[1];
        deferredReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.audit(record);
        });
        pendingValidators.add(results -> {
            @SuppressWarnings("unchecked") Map<Timestamp, String> result = (Map<Timestamp, String>) results
                    .get(slot[0]);
            validator.accept(result);
        });
    }

    @Override
    public void find(Criteria criteria, Consumer<Set<Long>> validator) {
        Preconditions.checkNotNull(validator);
        int[] slot = new int[1];
        deferredReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.find(criteria);
        });
        pendingValidators.add(results -> {
            @SuppressWarnings("unchecked") Set<Long> result = (Set<Long>) results
                    .get(slot[0]);
            validator.accept(result);
        });
    }

    @Override
    public void select(String key, Criteria criteria,
            Consumer<Map<Long, Set<Object>>> consumer) {
        Preconditions.checkNotNull(consumer);
        int[] slot = new int[1];
        deferredReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.select(key, criteria);
        });
        pendingValidators.add(results -> {
            @SuppressWarnings("unchecked") Map<Long, Set<Object>> result = (Map<Long, Set<Object>>) results
                    .get(slot[0]);
            consumer.accept(result);
        });
        flushReads();
    }

    @Override
    public void set(String key, Object value, long record) {
        deferredWriteOps.add(group -> group.set(key, value, record));
    }

    @Override
    public void clear(String key, long record) {
        deferredWriteOps.add(group -> group.clear(key, record));
    }

    @Override
    public void clear(long record) {
        deferredWriteOps.add(group -> group.clear(record));
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        deferredWriteOps.add(group -> group.verifyOrSet(key, value, record));
    }

    @Override
    public void reconcile(String key, long record, Collection<?> values) {
        @SuppressWarnings({ "rawtypes",
                "unchecked" }) Collection<Object> casted = (Collection) values;
        deferredWriteOps.add(group -> group.reconcile(key, record, casted));
    }

    @Override
    public void reconcile(String key, long record, Object[] values) {
        deferredWriteOps.add(
                group -> group.reconcile(key, record, Arrays.asList(values)));
    }

    @Override
    public boolean commit() {
        flushReads();
        CommandGroup writes = concourse.prepare();
        if(stageRequested && !stageSubmitted) {
            writes.stage();
            stageSubmitted = true;
        }
        for (Consumer<CommandGroup> op : deferredWriteOps) {
            op.accept(writes);
        }
        writes.commit();
        List<Object> results = concourse.submit(writes);
        return (Boolean) results.get(results.size() - 1);
    }

    @Override
    public void abort() {
        if(stageSubmitted) {
            concourse.abort();
        }
    }

    /**
     * Submit any reads accumulated in the active batch and run every queued
     * {@link Consumer Consumer} against the result list in recording order.
     * Clears the batch so subsequent recordings start fresh. No-op when no
     * reads have been recorded since the previous flush.
     */
    private void flushReads() {
        if(deferredReadOps.isEmpty()) {
            return;
        }
        CommandGroup reads = concourse.prepare();
        if(stageRequested && !stageSubmitted) {
            reads.stage();
        }
        for (Consumer<CommandGroup> op : deferredReadOps) {
            op.accept(reads);
        }
        List<Object> results = concourse.submit(reads);
        stageSubmitted = true;
        try {
            for (Consumer<List<Object>> validator : pendingValidators) {
                validator.accept(results);
            }
        }
        finally {
            deferredReadOps.clear();
            pendingValidators.clear();
        }
    }

}
