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

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Reader} records reads against a database and returns a
 * {@link Supplier} for each one.
 * <p>
 * The first call to {@link Supplier#get()} on any {@link Supplier} returned
 * from this {@link Reader} guarantees that the underlying read has been
 * executed against the database and yields its result. Implementations decide
 * <em>when</em> the underlying read is executed &mdash; eagerly at recording
 * time, batched and submitted on {@link #drain()}, or by some other strategy
 * &mdash; but the contract from a caller's perspective is the same: record,
 * then {@code get()}.
 * </p>
 * <p>
 * Callers that record multiple reads and want to fan out the resolution work
 * register completion {@link Runnable Runnables} via
 * {@link #onDrain(Runnable)}. {@link #drain()} issues any deferred reads and
 * then runs every registered completion in registration order.
 * </p>
 * <p>
 * Callers <strong>must</strong> call {@link #drain()} before discarding a
 * {@link Reader}. Discarding without draining leaves any registered
 * {@link #onDrain(Runnable) completions} unrun and any deferred reads
 * unsubmitted.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Reader {

    /**
     * Record a select for every record matching the {@code criteria}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria);

    /**
     * Record a select for every record matching the {@code criteria}, sorted by
     * {@code order}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria,
            Order order);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, sorted by {@code order}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Order order);

    /**
     * Record a select for every record matching the {@code criteria}, limited
     * to the requested {@code page} of the result set.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria,
            Page page);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, limited to the requested {@code page} of the result
     * set.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Page page);

    /**
     * Record a select for every record matching the {@code criteria}, sorted by
     * {@code order} and limited to the requested {@code page}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @param page the {@link Page} that limits the result set
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria,
            Order order, Page page);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, sorted by {@code order} and limited to the requested
     * {@code page}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @param page the {@link Page} that limits the result set
     * @return a {@link Supplier} that yields the matching records' data
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Order order, Page page);

    /**
     * Record a select for all values stored in {@code record}.
     *
     * @param record the record id
     * @return a {@link Supplier} that yields the record's data
     */
    Supplier<Map<String, Set<Object>>> select(long record);

    /**
     * Record a select for the {@code keys} stored in {@code record}.
     *
     * @param keys the field names whose values should be returned
     * @param record the record id
     * @return a {@link Supplier} that yields the record's data
     */
    Supplier<Map<String, Set<Object>>> select(Set<String> keys, long record);

    /**
     * Record a select for the values stored under {@code key} in
     * {@code record}.
     *
     * @param key the field name whose values should be returned
     * @param record the record id
     * @return a {@link Supplier} that yields the values for {@code key}
     */
    Supplier<Set<Object>> select(String key, long record);

    /**
     * Record a get for the most recent value stored under {@code key} in
     * {@code record}.
     *
     * @param key the field name whose value should be returned
     * @param record the record id
     * @return a {@link Supplier} that yields the most recent value, or
     *         {@code null} if no value exists
     */
    Supplier<Object> get(String key, long record);

    /**
     * Record a navigate that traverses the {@code keys} starting from
     * {@code record}.
     *
     * @param keys the link traversal paths
     * @param record the starting record id
     * @return a {@link Supplier} that yields the navigation result keyed by
     *         destination record id
     */
    Supplier<Map<Long, Map<String, Set<Object>>>> navigate(Set<String> keys,
            long record);

    /**
     * Record a find for the ids of every record matching the {@code criteria}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Supplier} that yields the matching record ids
     */
    Supplier<Set<Long>> find(Criteria criteria);

    /**
     * Record a find for the ids of every record matching the {@code criteria},
     * sorted by {@code order}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @return a {@link Supplier} that yields the matching record ids
     */
    Supplier<Set<Long>> find(Criteria criteria, Order order);

    /**
     * Record a find for the ids of every record matching the {@code criteria},
     * limited to the requested {@code page}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     * @return a {@link Supplier} that yields the matching record ids
     */
    Supplier<Set<Long>> find(Criteria criteria, Page page);

    /**
     * Record a find for the ids of every record matching the {@code criteria},
     * sorted by {@code order} and limited to the requested {@code page}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @param page the {@link Page} that limits the result set
     * @return a {@link Supplier} that yields the matching record ids
     */
    Supplier<Set<Long>> find(Criteria criteria, Order order, Page page);

    /**
     * Record a count of the values stored under {@code key} in every record
     * matching the {@code criteria}.
     *
     * @param key the field name whose values should be counted
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Supplier} that yields the count
     */
    Supplier<Long> count(String key, Criteria criteria);

    /**
     * Return the underlying {@link Concourse} connection that this
     * {@link Reader} wraps.
     *
     * @return the {@link Concourse} connection
     */
    Concourse concourse();

    /**
     * Register a completion {@link Runnable} to run inside {@link #drain()}.
     * Completions run in registration order, after any deferred reads have been
     * issued.
     *
     * @param completion the work to run when this {@link Reader} drains
     */
    void onDrain(Runnable completion);

    /**
     * Issue any deferred reads recorded on this {@link Reader} and run every
     * {@link #onDrain registered completion} in registration order.
     * <p>
     * Once the deferred reads have been issued this {@link Reader} is drained;
     * subsequent calls are no-ops even when a completion threw mid-iteration,
     * in which case the remaining completions are discarded. If the deferred
     * read submission itself fails, this {@link Reader} is not marked drained
     * and a subsequent call may retry.
     * </p>
     *
     * @throws RuntimeException if a deferred submission fails; no completions
     *             run in that case
     */
    void drain();

}
