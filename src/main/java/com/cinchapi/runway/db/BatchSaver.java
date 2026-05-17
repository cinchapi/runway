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
import java.util.HashMap;
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
 * submissions happen at {@link #commit()} and whenever a save-time
 * {@link #select(String, Criteria, Consumer) select} requires its result
 * inline.
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
     * Intra-batch uniqueness intents declared via
     * {@link #declareUniqueIntent(Object, long, String)}, keyed by canonical
     * constraint identifier and valued by the declaring record id.
     */
    private final Map<Object, Long> uniqueIntents;

    /**
     * Exceptions raised by intra-batch uniqueness conflicts detected during
     * {@link #declareUniqueIntent(Object, long, String)}.
     */
    private final List<RuntimeException> intentConflicts;

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
        this.deferredReadOps = new ArrayList<>();
        this.pendingValidators = new ArrayList<>();
        this.deferredWriteOps = new ArrayList<>();
        this.uniqueIntents = new HashMap<>();
        this.intentConflicts = new ArrayList<>();
    }

    @Override
    public Concourse concourse() {
        return concourse;
    }

    @Override
    public void stage() {
        stageRequested = true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void audit(long record,
            Consumer<Map<Timestamp, List<String>>> validator) {
        Preconditions.checkNotNull(validator);
        int[] slot = new int[1];
        deferredReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.audit(record);
        });
        pendingValidators.add(results -> {
            Map<Timestamp, List<String>> result = (Map<Timestamp, List<String>>) results
                    .get(slot[0]);
            validator.accept(result);
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public void find(Criteria criteria, Consumer<Set<Long>> validator) {
        Preconditions.checkNotNull(validator);
        int[] slot = new int[1];
        deferredReadOps.add(group -> {
            slot[0] = group.commands().size();
            group.find(criteria);
        });
        pendingValidators.add(results -> {
            Set<Long> result = (Set<Long>) results.get(slot[0]);
            validator.accept(result);
        });
    }

    @SuppressWarnings("unchecked")
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
    public void declareUniqueIntent(Object canonical, long record,
            String errorMessage) {
        Long prior = uniqueIntents.putIfAbsent(canonical, record);
        if(prior != null && prior.longValue() != record) {
            // Surfaced from commit() only after the read pipeline has
            // run, so any staleness or database-uniqueness exception
            // takes precedence and the throw fires regardless of whether
            // the caller also recorded a paired find.
            intentConflicts.add(new IllegalStateException(errorMessage));
        }
    }

    @Override
    public boolean commit() {
        flushReads();
        if(intentConflicts.isEmpty()) {
            CommandGroup writes = concourse.prepare();
            if(stageRequested && !stageBundled) {
                writes.stage();
                stageBundled = true;
            }
            for (Consumer<CommandGroup> op : deferredWriteOps) {
                op.accept(writes);
            }
            int commitSlot = writes.commands().size();
            writes.commit();
            List<Object> results = concourse.submit(writes);
            return (Boolean) results.get(commitSlot);
        }
        else {
            throw intentConflicts.get(0);
        }
    }

    @Override
    public void abort() {
        concourse.abort();
    }

    /**
     * Submit any reads accumulated in the active batch and run every queued
     * {@link Consumer Consumer} against the result list in recording order.
     * No-op when no reads have been recorded since the previous flush.
     * <p>
     * Deferred writes accumulated up to this point are submitted in the same
     * {@link CommandGroup} immediately before the reads, so reads observe those
     * writes in the staged transaction.
     * </p>
     */
    private void flushReads() {
        if(!deferredReadOps.isEmpty()) {
            CommandGroup group = concourse.prepare();
            if(stageRequested && !stageBundled) {
                // NOTE: STAGE inside this CommandGroup relies on the driver
                // to adopt the resulting TransactionToken and to clear it
                // when a later submission's COMMIT runs, so the
                // reads-then-writes pair shares one staged transaction
                // (cinchapi/concourse#735).
                group.stage();
                stageBundled = true;
            }
            for (Consumer<CommandGroup> op : deferredWriteOps) {
                op.accept(group);
            }
            deferredWriteOps.clear();
            for (Consumer<CommandGroup> op : deferredReadOps) {
                op.accept(group);
            }
            List<Object> results = concourse.submit(group);
            // Snapshot and clear before dispatching so nested saver
            // recordings made by a validator/consumer accumulate into a
            // fresh batch.
            List<Consumer<List<Object>>> active = new ArrayList<>(
                    pendingValidators);
            deferredReadOps.clear();
            pendingValidators.clear();
            for (Consumer<List<Object>> validator : active) {
                validator.accept(results);
            }
        }
    }

}
