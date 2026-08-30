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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Reader} records reads against a database and returns a
 * {@link Pending} for each one. Composition operators on {@link Pending}
 * ({@link Pending#map}, {@link Pending#then}, {@link Pending#onResolve}) build
 * pipelines that observe and chain off the recorded reads' results.
 * <p>
 * Callers <strong>must</strong> call {@link #drain()} before discarding a
 * {@link Reader}. Discarding without draining leaves any unresolved
 * {@link Pending Pendings} unresolved.
 * </p>
 * <p>
 * A {@link Reader} is {@link AutoCloseable}: implementations that manage their
 * own {@link ConcourseProvider}-backed {@link Concourse} connection release it
 * on {@link #close()}, making try-with-resources the recommended usage pattern.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Reader extends AutoCloseable {

    /**
     * Record a select for every record matching the {@code criteria}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria);

    /**
     * Record a select for every record matching the {@code criteria}, sorted by
     * {@code order}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria,
            Order order);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, sorted by {@code order}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Order order);

    /**
     * Record a select for every record matching the {@code criteria}, limited
     * to the requested {@code page} of the result set.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria,
            Page page);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, limited to the requested {@code page} of the result
     * set.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Page page);

    /**
     * Record a select for every record matching the {@code criteria}, sorted by
     * {@code order} and limited to the requested {@code page}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @param page the {@link Page} that limits the result set
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Criteria criteria,
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
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Order order, Page page);

    /**
     * Record a select for all values stored in every record in {@code records}.
     *
     * @param records the record ids
     * @return a {@link Pending} of the matching records' data
     */
    Pending<Map<Long, Map<String, Set<Object>>>> select(
            Collection<Long> records);

    /**
     * Record a select for all values stored in {@code record}.
     *
     * @param record the record id
     * @return a {@link Pending} of the record's data
     */
    Pending<Map<String, Set<Object>>> select(long record);

    /**
     * Record a select for the {@code keys} stored in {@code record}.
     *
     * @param keys the field names whose values should be returned
     * @param record the record id
     * @return a {@link Pending} of the record's data
     */
    Pending<Map<String, Set<Object>>> select(Set<String> keys, long record);

    /**
     * Record a navigate that traverses the {@code keys} starting from
     * {@code record}.
     *
     * @param keys the link traversal paths
     * @param record the starting record id
     * @return a {@link Pending} of the navigation result keyed by destination
     *         record id
     */
    Pending<Map<Long, Map<String, Set<Object>>>> navigate(Set<String> keys,
            long record);

    /**
     * Record a navigate that traverses the {@code keys} starting from every
     * record matching the {@code criteria}.
     *
     * @param keys the link traversal paths
     * @param criteria the {@link Criteria} that identifies the starting records
     * @return a {@link Pending} of the navigation result keyed by destination
     *         record id
     */
    Pending<Map<Long, Map<String, Set<Object>>>> navigate(Set<String> keys,
            Criteria criteria);

    /**
     * Record a navigate that traverses the {@code keys} starting from each of
     * the {@code records}.
     *
     * @param keys the link traversal paths
     * @param records the starting record ids
     * @return a {@link Pending} of the navigation result keyed by destination
     *         record id
     */
    Pending<Map<Long, Map<String, Set<Object>>>> navigate(Set<String> keys,
            Collection<Long> records);

    /**
     * Record a find for the ids of every record matching the {@code criteria}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Pending} of the matching record ids
     */
    Pending<Set<Long>> find(Criteria criteria);

    /**
     * Record a count of the values stored under {@code key} in every record
     * matching the {@code criteria}.
     *
     * @param key the field name whose values should be counted
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Pending} of the count
     */
    Pending<Long> count(String key, Criteria criteria);

    /**
     * Issue every deferred read recorded on this {@link Reader} and resolve
     * every {@link Pending} obtained from it. May be called repeatedly: a
     * subsequent call processes any reads recorded since the previous call, and
     * a call with nothing recorded is a no-op.
     */
    void drain();

    /**
     * Release any {@link ConcourseProvider}-backed {@link Concourse} connection
     * that this {@link Reader} acquired. Safe to call when no connection was
     * ever acquired; idempotent on repeated calls. Implementations that wrap an
     * externally-managed {@link Concourse} treat this as a no-op &mdash; the
     * connection lifecycle remains with the caller.
     */
    @Override
    void close();

}
