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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.CommandGroup;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.google.common.collect.ImmutableList;

/**
 * A {@link Reader} that batches recorded reads into a {@link CommandGroup} and
 * submits them via {@link Concourse#submit(CommandGroup)} in a single round
 * trip.
 *
 * <h2>Batch lifecycle</h2>
 * <p>
 * This {@link Reader} maintains a current <em>batch</em> &mdash; the
 * {@link CommandGroup} that subsequent recording calls append to. Each
 * recording call (e.g. {@link #select(Criteria)}, {@link #find(Criteria)},
 * {@link #count(String, Criteria)}) appends a single command to the current
 * batch and returns a {@link Supplier} bound to that batch. The recording call
 * itself does not contact the database; it only mutates the batch's command
 * list.
 * </p>
 * <p>
 * The first {@link Supplier#get()} call on any {@link Supplier} bound to the
 * current batch <em>flushes</em> that batch via
 * {@link Concourse#submit(CommandGroup)} &mdash; a single round trip that
 * executes every command appended so far. The flushed submission result is
 * shared with every {@link Supplier} bound to that batch, so any subsequent
 * {@code get()} call on a sibling {@link Supplier} returns its value from the
 * already-submitted result without issuing further database work.
 * </p>
 * <p>
 * Any recording call made after the current batch has been flushed opens a
 * fresh batch with a new {@link CommandGroup}. The boundary between batches is
 * therefore controlled by the caller: callers that want N reads issued in a
 * single round trip must record all N reads before invoking
 * {@link Supplier#get()} on any of the returned {@link Supplier Suppliers}.
 * </p>
 *
 * <h2>Supplier guarantees</h2>
 * <p>
 * The {@link Supplier} returned by each recording method is bound to the batch
 * that was current when the read was recorded. Calling its
 * {@link Supplier#get() get()} guarantees that, by the time the call returns,
 * the underlying read has been issued and its result is available. The
 * {@link Supplier} is idempotent: repeated invocations return the same value
 * without further database work.
 * </p>
 * <p>
 * If {@link Concourse#submit(CommandGroup)} throws during a flush, the
 * exception is latched onto the batch and rethrown on every subsequent
 * {@link Supplier#get()} call against any {@link Supplier} bound to that batch.
 * This preserves the supplier-idempotence contract: sibling {@link Supplier
 * Suppliers} cannot observe divergent outcomes (one succeeding and another
 * failing) because the batch is never re-submitted after a failure.
 * </p>
 * <p>
 * This {@link Reader} is <strong>not thread-safe</strong>: every interaction
 * &mdash; recording, resolving any returned {@link Supplier}, or both &mdash;
 * must be performed on a single thread.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class EventualReader extends AbstractReader {

    /**
     * The active recording batch.
     */
    private Batch current;

    /**
     * Construct a new {@link EventualReader}.
     *
     * @param concourse the {@link Concourse} connection against which reads are
     *            submitted; must not be {@code null}
     */
    public EventualReader(Concourse concourse) {
        super(concourse);
        rollover();
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(criteria);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(ImmutableList.copyOf(keys), criteria);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Order order) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(criteria, order);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria, Order order) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(ImmutableList.copyOf(keys), criteria, order);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Page page) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(criteria, page);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria, Page page) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(ImmutableList.copyOf(keys), criteria, page);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Order order, Page page) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(criteria, order, page);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria, Order order, Page page) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(ImmutableList.copyOf(keys), criteria, order, page);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Map<String, Set<Object>>> select(long record) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(record);
        return () -> recordData(batch, slot);
    }

    @Override
    public Supplier<Map<String, Set<Object>>> select(Set<String> keys,
            long record) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(ImmutableList.copyOf(keys), record);
        return () -> recordData(batch, slot);
    }

    @Override
    public Supplier<Set<Object>> select(String key, long record) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.select(key, record);
        return () -> values(batch, slot);
    }

    @Override
    public Supplier<Object> get(String key, long record) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.get(key, record);
        return () -> batch.flush(concourse).get(slot);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> navigate(
            Set<String> keys, long record) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.navigate(ImmutableList.copyOf(keys), record);
        return () -> mapByRecord(batch, slot);
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.find(criteria);
        return () -> ids(batch, slot);
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria, Order order) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.find(criteria, order);
        return () -> ids(batch, slot);
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria, Page page) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.find(criteria, page);
        return () -> ids(batch, slot);
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria, Order order, Page page) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.find(criteria, order, page);
        return () -> ids(batch, slot);
    }

    @Override
    public Supplier<Long> count(String key, Criteria criteria) {
        Batch batch = batch();
        int slot = batch.size();
        batch.group.count(key, criteria);
        return () -> ((Number) batch.flush(concourse).get(slot)).longValue();
    }

    /**
     * Return the {@link Batch} that subsequent recordings should append to,
     * starting a fresh one if the active {@link Batch} has already been
     * flushed.
     *
     * @return the {@link Batch} that should receive the next recording
     */
    private Batch batch() {
        if(current.flushed()) {
            rollover();
        }
        return current;
    }

    /**
     * Cast the entry at {@code slot} from {@code batch}'s flushed result to a
     * record id {@link Set}.
     *
     * @param batch the {@link Batch} whose result holds the recorded read
     * @param slot the index of the recorded read
     * @return the matching record ids
     */
    @SuppressWarnings("unchecked")
    private Set<Long> ids(Batch batch, int slot) {
        return (Set<Long>) batch.flush(concourse).get(slot);
    }

    /**
     * Cast the entry at {@code slot} from {@code batch}'s flushed result to a
     * record-keyed {@link Map} of field data.
     *
     * @param batch the {@link Batch} whose result holds the recorded read
     * @param slot the index of the recorded read
     * @return the matching records' data
     */
    @SuppressWarnings("unchecked")
    private Map<Long, Map<String, Set<Object>>> mapByRecord(Batch batch,
            int slot) {
        return (Map<Long, Map<String, Set<Object>>>) batch.flush(concourse)
                .get(slot);
    }

    /**
     * Cast the entry at {@code slot} from {@code batch}'s flushed result to a
     * single record's field data.
     *
     * @param batch the {@link Batch} whose result holds the recorded read
     * @param slot the index of the recorded read
     * @return the record's data
     */
    @SuppressWarnings("unchecked")
    private Map<String, Set<Object>> recordData(Batch batch, int slot) {
        return (Map<String, Set<Object>>) batch.flush(concourse).get(slot);
    }

    /**
     * Replace the active {@link Batch} with a fresh one backed by a new
     * {@link CommandGroup}.
     */
    private void rollover() {
        current = new Batch(concourse.prepare());
    }

    /**
     * Cast the entry at {@code slot} from {@code batch}'s flushed result to a
     * field's value {@link Set}.
     *
     * @param batch the {@link Batch} whose result holds the recorded read
     * @param slot the index of the recorded read
     * @return the field's values
     */
    @SuppressWarnings("unchecked")
    private Set<Object> values(Batch batch, int slot) {
        return (Set<Object>) batch.flush(concourse).get(slot);
    }

    /**
     * A {@link Batch} groups reads that are submitted together in a single
     * round trip.
     *
     * @author Jeff Nelson
     */
    private static final class Batch {

        /**
         * The {@link CommandGroup} accumulating recorded reads for this
         * {@link Batch}.
         */
        final CommandGroup group;

        /**
         * The submission result; {@code null} until {@link #flush(Concourse)}
         * has been called successfully.
         */
        List<Object> results;

        /**
         * The latched failure from a prior {@link #flush(Concourse)} attempt,
         * or {@code null} if no flush has failed. Once non-{@code null}, every
         * subsequent {@link #flush(Concourse)} call rethrows this exception
         * without re-submitting.
         */
        RuntimeException failure;

        /**
         * Construct a new {@link Batch} that accumulates reads on
         * {@code group}.
         *
         * @param group the {@link CommandGroup} to accumulate reads onto
         */
        Batch(CommandGroup group) {
            this.group = group;
            this.results = null;
            this.failure = null;
        }

        /**
         * Submit {@link #group} via {@code concourse} the first time this is
         * called and return the submission result. Subsequent invocations
         * return the same {@link List} reference without issuing further
         * database calls.
         * <p>
         * If the submission throws, the {@link RuntimeException} is latched
         * onto this {@link Batch} and rethrown on every subsequent invocation;
         * {@link Concourse#submit(CommandGroup)} is never called a second time.
         * This preserves {@link Supplier#get() Supplier#get()}'s idempotence:
         * every {@link Supplier} bound to a failed {@link Batch} sees the same
         * exception rather than racing the network.
         *
         * @param concourse the {@link Concourse} connection to submit against
         * @return the submission result
         * @throws RuntimeException the original failure, if a prior
         *             {@link #flush(Concourse)} invocation against this
         *             {@link Batch} threw
         */
        List<Object> flush(Concourse concourse) {
            if(failure != null) {
                throw failure;
            }
            if(results == null) {
                try {
                    results = concourse.submit(group);
                }
                catch (RuntimeException e) {
                    failure = e;
                    throw e;
                }
            }
            return results;
        }

        /**
         * Return {@code true} if this {@link Batch} has been flushed, either
         * successfully (a result is available) or unsuccessfully (a failure has
         * been latched). In both cases the {@link Batch} is terminal:
         * subsequent recordings must roll over to a fresh {@link Batch}.
         *
         * @return whether this {@link Batch} is terminal
         */
        boolean flushed() {
            return results != null || failure != null;
        }

        /**
         * Return the number of reads currently recorded on this {@link Batch}.
         * Used as the slot index for the next recording.
         *
         * @return the current size of {@link #group}
         */
        int size() {
            return group.commands().size();
        }

    }

}
