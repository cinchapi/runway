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
 * number of {@link CommandGroup} submissions a given save permits. Recording
 * calls accumulate deferred operations rather than touching the server;
 * submissions happen at {@link #commit()}, at {@link #flush()} and whenever a
 * save-time {@link #select(String, Criteria, Consumer) select} requires its
 * result inline.
 * <p>
 * If any queued {@link Consumer} throws during a submission, the staged
 * transaction has already been opened on the server; the caller is responsible
 * for calling {@link #abort()} after handling the exception.
 * </p>
 * <p>
 * This {@link Saver} is <strong>not thread-safe</strong>.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class BatchSaver implements Saver {

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
     * Whether {@link #stage()} has been bundled into a submitted
     * {@link CommandGroup}; once {@code true} subsequent flushes and the writes
     * submission do not re-record it.
     */
    private boolean stageBundled;

    /**
     * Deferred read recordings that must observe the pre-save snapshot. Applied
     * to the active {@link CommandGroup} before the deferred writes so the
     * audit-based stale-write check sees only state that existed before this
     * save began. Each entry records its own slot position when it runs.
     */
    private final List<Consumer<CommandGroup>> preWriteReadOps;

    /**
     * Deferred read recordings that must observe the writes accumulated in this
     * save. Applied to the active {@link CommandGroup} after the deferred
     * writes so uniqueness and cascade-delete lookups see the in-flight
     * updates. Each entry records its own slot position when it runs.
     */
    private final List<Consumer<CommandGroup>> postWriteReadOps;

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
     * Construct a new {@link BatchSaver} that submits against
     * {@code concourse}.
     *
     * @param concourse the {@link Concourse} connection that hosts the staged
     *            transaction; must not be {@code null}
     */
    public BatchSaver(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
        this.stageRequested = false;
        this.stageBundled = false;
        this.preWriteReadOps = new ArrayList<>();
        this.postWriteReadOps = new ArrayList<>();
        this.pendingValidators = new ArrayList<>();
        this.deferredWriteOps = new ArrayList<>();
    }

    @Override
    public void abort() {
        concourse.abort();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void audit(long record,
            Consumer<Map<Timestamp, List<String>>> validator) {
        Preconditions.checkNotNull(validator);
        int[] slot = new int[1];
        preWriteReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.audit(record);
        });
        pendingValidators.add(results -> {
            Map<Timestamp, List<String>> result = (Map<Timestamp, List<String>>) results
                    .get(slot[0]);
            validator.accept(result);
        });
    }

    @Override
    public void clear(long record) {
        deferredWriteOps.add(group -> group.clear(record));
    }

    @Override
    public void clear(String key, long record) {
        deferredWriteOps.add(group -> group.clear(key, record));
    }

    @Override
    public boolean commit() {
        flushReads();
        CommandGroup writes = prepareGroup();
        drainWrites(writes);
        int commitSlot = writes.commands().size();
        writes.commit();
        List<Object> results = concourse.submit(writes);
        return (Boolean) results.get(commitSlot);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void find(Criteria criteria, Consumer<Set<Long>> validator) {
        Preconditions.checkNotNull(validator);
        int[] slot = new int[1];
        postWriteReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.find(criteria);
        });
        pendingValidators.add(results -> {
            Set<Long> result = (Set<Long>) results.get(slot[0]);
            validator.accept(result);
        });
    }

    @Override
    public void flush() {
        while (!preWriteReadOps.isEmpty() || !postWriteReadOps.isEmpty()
                || !deferredWriteOps.isEmpty()) {
            if(preWriteReadOps.isEmpty() && postWriteReadOps.isEmpty()) {
                CommandGroup writes = prepareGroup();
                drainWrites(writes);
                concourse.submit(writes);
            }
            else {
                // A queued validator may record further operations, so loop
                // until a pass leaves nothing pending.
                flushReads();
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void reconcile(String key, long record, Collection<?> values) {
        if(values.isEmpty()) {
            deferredWriteOps.add(group -> group.clear(key, record));
        }
        else {
            Collection<Object> casted = (Collection) values;
            deferredWriteOps.add(group -> group.reconcile(key, record, casted));
        }
    }

    @Override
    public void reconcile(String key, long record, Object[] values) {
        if(values.length == 0) {
            deferredWriteOps.add(group -> group.clear(key, record));
        }
        else {
            deferredWriteOps.add(group -> group.reconcile(key, record,
                    Arrays.asList(values)));
        }
    }

    @Override
    public void remove(String key, Object value, long record) {
        deferredWriteOps.add(group -> group.remove(key, value, record));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void select(String key, Criteria criteria,
            Consumer<Map<Long, Set<Object>>> consumer) {
        Preconditions.checkNotNull(consumer);
        int[] slot = new int[1];
        postWriteReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.select(key, criteria);
        });
        pendingValidators.add(results -> {
            Map<Long, Set<Object>> result = (Map<Long, Set<Object>>) results
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
    public void stage() {
        stageRequested = true;
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        deferredWriteOps.add(group -> group.verifyOrSet(key, value, record));
    }

    /**
     * Append every deferred write to {@code group} and clear the queue.
     *
     * @param group the {@link CommandGroup} that receives the deferred writes
     */
    private void drainWrites(CommandGroup group) {
        for (Consumer<CommandGroup> op : deferredWriteOps) {
            op.accept(group);
        }
        deferredWriteOps.clear();
    }

    /**
     * Submit any reads accumulated in the active batch and run every queued
     * {@link Consumer Consumer} against the result list in recording order.
     * No-op when no reads have been recorded since the previous flush.
     * <p>
     * Reads are split around the deferred writes by what they need to see:
     * pre-write reads (the stale-write {@link #audit audit}) run against the
     * pre-save snapshot, then the deferred writes apply within the same staged
     * transaction, then post-write reads ({@link #find find} and {@link #select
     * select}) run so uniqueness and cascade-delete lookups observe the
     * in-flight updates.
     * </p>
     */
    private void flushReads() {
        if(!preWriteReadOps.isEmpty() || !postWriteReadOps.isEmpty()) {
            CommandGroup group = prepareGroup();
            for (Consumer<CommandGroup> op : preWriteReadOps) {
                op.accept(group);
            }
            preWriteReadOps.clear();
            drainWrites(group);
            for (Consumer<CommandGroup> op : postWriteReadOps) {
                op.accept(group);
            }
            List<Object> results = concourse.submit(group);
            // Snapshot and clear before dispatching so nested saver
            // recordings made by a validator/consumer accumulate into a
            // fresh batch.
            List<Consumer<List<Object>>> active = new ArrayList<>(
                    pendingValidators);
            postWriteReadOps.clear();
            pendingValidators.clear();
            for (Consumer<List<Object>> validator : active) {
                validator.accept(results);
            }
        }
    }

    /**
     * Prepare a new {@link CommandGroup} for a submission, bundling the
     * requested {@link #stage()} into it if no prior submission has already
     * carried it.
     *
     * @return the {@link CommandGroup}
     */
    private CommandGroup prepareGroup() {
        CommandGroup group = concourse.prepare();
        if(stageRequested && !stageBundled) {
            // NOTE: STAGE inside this CommandGroup relies on the driver
            // to adopt the resulting TransactionToken and to clear it
            // when a later submission's COMMIT runs, so every submission
            // shares one staged transaction (cinchapi/concourse#735).
            group.stage();
            stageBundled = true;
        }
        return group;
    }

}
