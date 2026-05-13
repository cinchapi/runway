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
 * A {@link Reader} that batches recorded reads into a single
 * {@link Concourse#submit(CommandGroup) submission} so that all reads recorded
 * against the same batch share a single round trip.
 * <p>
 * This {@link Reader} is <strong>not thread-safe</strong>.
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
         * The latched failure from a prior submission attempt, or {@code null}
         * if none.
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
         * Submit {@link #group} via {@code concourse} and return the result.
         * Idempotent: subsequent calls return the same {@link List}. If
         * submission fails, the exception is latched and rethrown on every
         * subsequent call.
         *
         * @param concourse the {@link Concourse} connection to submit against
         * @return the submission result
         * @throws RuntimeException if submission fails, or has previously
         *             failed against this {@link Batch}
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
         * Return {@code true} if this {@link Batch} has been submitted
         * (successfully or unsuccessfully).
         *
         * @return whether this {@link Batch} has been submitted
         */
        boolean flushed() {
            return results != null || failure != null;
        }

        /**
         * Return the number of reads currently recorded on this {@link Batch}.
         *
         * @return the number of reads recorded
         */
        int size() {
            return group.commands().size();
        }

    }

}
