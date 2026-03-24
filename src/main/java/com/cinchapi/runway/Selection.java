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
package com.cinchapi.runway;

import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Selection} describes a single data retrieval operation against a
 * {@link Runway} instance and holds the result after execution.
 * <p>
 * {@link Selection Selections} are created via static factory methods that
 * correspond to the type of operation:
 * <ul>
 * <li>{@link #find} &mdash; criteria-based queries</li>
 * <li>{@link #load(Class)} &mdash; load all records of a class</li>
 * <li>{@link #load(Class, long)} &mdash; load a single record by ID</li>
 * <li>{@link #count} &mdash; count matching records</li>
 * </ul>
 * Each factory has an {@code Any} variant (e.g., {@link #findAny},
 * {@link #loadAny(Class)}, {@link #countAny}) that includes descendants of the
 * target class.
 * </p>
 * <p>
 * A {@link Selection} has a lifecycle with three states:
 * <ul>
 * <li>{@link State#PENDING} &mdash; just created, not yet submitted</li>
 * <li>{@link State#SUBMITTED} &mdash; submitted to a {@link Runway} for
 * processing</li>
 * <li>{@link State#FINISHED} &mdash; execution complete, results available</li>
 * </ul>
 * Configuration methods may only be called while pending. Results may only be
 * retrieved when finished.
 * </p>
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
public abstract class Selection<T extends Record> {

    /**
     * Create a {@link FindSelection} that finds {@link Record Records} of the
     * given {@code clazz} matching the {@code criteria}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria) {
        return new FindSelection<>(clazz, criteria, false);
    }

    /**
     * Create a {@link FindSelection} with all optional parameters.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order, or {@code null}
     * @param page the pagination, or {@code null}
     * @param realms the {@link Realms} filter, or {@code null}
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, @Nullable Order order, @Nullable Page page,
            @Nullable Realms realms) {
        FindSelection<T> selection = new FindSelection<>(clazz, criteria,
                false);
        selection.order = order;
        selection.page = page;
        if(realms != null) {
            selection.realms = realms;
        }
        return selection;
    }

    /**
     * Create a {@link FindSelection} sorted by {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Order order) {
        return find(clazz, criteria, order, null, null);
    }

    /**
     * Create a {@link FindSelection} sorted by {@code order} and paginated by
     * {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order
     * @param page the pagination
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Order order, Page page) {
        return find(clazz, criteria, order, page, null);
    }

    /**
     * Create a {@link FindSelection} sorted by {@code order} within the
     * specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Order order, Realms realms) {
        return find(clazz, criteria, order, null, realms);
    }

    /**
     * Create a {@link FindSelection} paginated by {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Page page) {
        return find(clazz, criteria, null, page, null);
    }

    /**
     * Create a {@link FindSelection} paginated by {@code page} and sorted by
     * {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @param order the sort order
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Page page, Order order) {
        return find(clazz, criteria, order, page, null);
    }

    /**
     * Create a {@link FindSelection} paginated by {@code page}, sorted by
     * {@code order}, within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Page page, Order order, Realms realms) {
        return find(clazz, criteria, order, page, realms);
    }

    /**
     * Create a {@link FindSelection} paginated by {@code page} within the
     * specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Page page, Realms realms) {
        return find(clazz, criteria, null, page, realms);
    }

    /**
     * Create a {@link FindSelection} within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> find(Class<T> clazz,
            Criteria criteria, Realms realms) {
        return find(clazz, criteria, (Order) null, null, realms);
    }

    /**
     * Create a {@link FindSelection} that finds {@link Record Records} of the
     * given {@code clazz} and its descendants matching the {@code criteria}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria) {
        return new FindSelection<>(clazz, criteria, true);
    }

    /**
     * Create a {@link FindSelection} for the given {@code clazz} and its
     * descendants with all optional parameters.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order, or {@code null}
     * @param page the pagination, or {@code null}
     * @param realms the {@link Realms} filter, or {@code null}
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, @Nullable Order order, @Nullable Page page,
            @Nullable Realms realms) {
        FindSelection<T> selection = new FindSelection<>(clazz, criteria, true);
        selection.order = order;
        selection.page = page;
        if(realms != null) {
            selection.realms = realms;
        }
        return selection;
    }

    /**
     * Create a {@link FindSelection} for descendants, sorted by {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Order order) {
        return findAny(clazz, criteria, order, null, null);
    }

    /**
     * Create a {@link FindSelection} for descendants, sorted by {@code order}
     * and paginated by {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order
     * @param page the pagination
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Page page) {
        return findAny(clazz, criteria, order, page, null);
    }

    /**
     * Create a {@link FindSelection} for descendants, sorted by {@code order}
     * within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Realms realms) {
        return findAny(clazz, criteria, order, null, realms);
    }

    /**
     * Create a {@link FindSelection} for descendants, paginated by
     * {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Page page) {
        return findAny(clazz, criteria, null, page, null);
    }

    /**
     * Create a {@link FindSelection} for descendants, paginated by {@code page}
     * and sorted by {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @param order the sort order
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Order order) {
        return findAny(clazz, criteria, order, page, null);
    }

    /**
     * Create a {@link FindSelection} for descendants, paginated by
     * {@code page}, sorted by {@code order}, within the specified
     * {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Order order, Realms realms) {
        return findAny(clazz, criteria, order, page, realms);
    }

    /**
     * Create a {@link FindSelection} for descendants, paginated by {@code page}
     * within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param page the pagination
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Realms realms) {
        return findAny(clazz, criteria, null, page, realms);
    }

    /**
     * Create a {@link FindSelection} for descendants within the specified
     * {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param realms the {@link Realms} filter
     * @return a new {@link FindSelection}
     */
    public static <T extends Record> FindSelection<T> findAny(Class<T> clazz,
            Criteria criteria, Realms realms) {
        return findAny(clazz, criteria, (Order) null, null, realms);
    }

    /**
     * Create a {@link LoadRecordSelection} that loads a single {@link Record}
     * by its {@code id}.
     *
     * @param clazz the {@link Record} class
     * @param id the record ID
     * @return a new {@link LoadRecordSelection}
     */
    public static <T extends Record> LoadRecordSelection<T> load(Class<T> clazz,
            long id) {
        return new LoadRecordSelection<>(clazz, id, false);
    }

    /**
     * Create a {@link LoadRecordSelection} that loads a single {@link Record}
     * by its {@code id} within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param id the record ID
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadRecordSelection}
     */
    public static <T extends Record> LoadRecordSelection<T> load(Class<T> clazz,
            long id, Realms realms) {
        LoadRecordSelection<T> selection = new LoadRecordSelection<>(clazz, id,
                false);
        selection.realms = realms;
        return selection;
    }

    /**
     * Create a {@link LoadRecordSelection} that loads a single {@link Record}
     * of the given {@code clazz} or its descendants by {@code id}.
     *
     * @param clazz the {@link Record} class
     * @param id the record ID
     * @return a new {@link LoadRecordSelection}
     */
    public static <T extends Record> LoadRecordSelection<T> loadAny(
            Class<T> clazz, long id) {
        return new LoadRecordSelection<>(clazz, id, true);
    }

    /**
     * Create a {@link LoadClassSelection} that loads all {@link Record Records}
     * of the given {@code clazz}.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(
            Class<T> clazz) {
        return new LoadClassSelection<>(clazz, false);
    }

    /**
     * Create a {@link LoadClassSelection} with all optional parameters.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order, or {@code null}
     * @param page the pagination, or {@code null}
     * @param realms the {@link Realms} filter, or {@code null}
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            @Nullable Order order, @Nullable Page page,
            @Nullable Realms realms) {
        LoadClassSelection<T> selection = new LoadClassSelection<>(clazz,
                false);
        selection.order = order;
        selection.page = page;
        if(realms != null) {
            selection.realms = realms;
        }
        return selection;
    }

    /**
     * Create a {@link LoadClassSelection} sorted by {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Order order) {
        return load(clazz, order, null, null);
    }

    /**
     * Create a {@link LoadClassSelection} sorted by {@code order} and paginated
     * by {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order
     * @param page the pagination
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Order order, Page page) {
        return load(clazz, order, page, null);
    }

    /**
     * Create a {@link LoadClassSelection} sorted by {@code order} within the
     * specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Order order, Realms realms) {
        return load(clazz, order, null, realms);
    }

    /**
     * Create a {@link LoadClassSelection} paginated by {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Page page) {
        return load(clazz, null, page, null);
    }

    /**
     * Create a {@link LoadClassSelection} paginated by {@code page} and sorted
     * by {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @param order the sort order
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Page page, Order order) {
        return load(clazz, order, page, null);
    }

    /**
     * Create a {@link LoadClassSelection} paginated by {@code page}, sorted by
     * {@code order}, within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Page page, Order order, Realms realms) {
        return load(clazz, order, page, realms);
    }

    /**
     * Create a {@link LoadClassSelection} paginated by {@code page} within the
     * specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Page page, Realms realms) {
        return load(clazz, null, page, realms);
    }

    /**
     * Create a {@link LoadClassSelection} within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> load(Class<T> clazz,
            Realms realms) {
        return load(clazz, (Order) null, null, realms);
    }

    /**
     * Create a {@link LoadClassSelection} that loads all {@link Record Records}
     * of the given {@code clazz} and its descendants.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz) {
        return new LoadClassSelection<>(clazz, true);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants with all optional
     * parameters.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order, or {@code null}
     * @param page the pagination, or {@code null}
     * @param realms the {@link Realms} filter, or {@code null}
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, @Nullable Order order, @Nullable Page page,
            @Nullable Realms realms) {
        LoadClassSelection<T> selection = new LoadClassSelection<>(clazz, true);
        selection.order = order;
        selection.page = page;
        if(realms != null) {
            selection.realms = realms;
        }
        return selection;
    }

    /**
     * Create a {@link LoadClassSelection} for descendants, sorted by
     * {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Order order) {
        return loadAny(clazz, order, null, null);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants, sorted by
     * {@code order} and paginated by {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order
     * @param page the pagination
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Order order, Page page) {
        return loadAny(clazz, order, page, null);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants, sorted by
     * {@code order} within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Order order, Realms realms) {
        return loadAny(clazz, order, null, realms);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants, paginated by
     * {@code page}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Page page) {
        return loadAny(clazz, null, page, null);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants, paginated by
     * {@code page} and sorted by {@code order}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @param order the sort order
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Page page, Order order) {
        return loadAny(clazz, order, page, null);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants, paginated by
     * {@code page}, sorted by {@code order}, within the specified
     * {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @param order the sort order
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Page page, Order order, Realms realms) {
        return loadAny(clazz, order, page, realms);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants, paginated by
     * {@code page} within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param page the pagination
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Page page, Realms realms) {
        return loadAny(clazz, null, page, realms);
    }

    /**
     * Create a {@link LoadClassSelection} for descendants within the specified
     * {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param realms the {@link Realms} filter
     * @return a new {@link LoadClassSelection}
     */
    public static <T extends Record> LoadClassSelection<T> loadAny(
            Class<T> clazz, Realms realms) {
        return loadAny(clazz, (Order) null, null, realms);
    }

    /**
     * Create a {@link CountSelection} that counts all {@link Record Records} of
     * the given {@code clazz}.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> count(Class<T> clazz) {
        return new CountSelection<>(clazz, null, false);
    }

    /**
     * Create a {@link CountSelection} that counts {@link Record Records} of the
     * given {@code clazz} matching the {@code criteria}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> count(Class<T> clazz,
            Criteria criteria) {
        return new CountSelection<>(clazz, criteria, false);
    }

    /**
     * Create a {@link CountSelection} that counts {@link Record Records} of the
     * given {@code clazz} matching the {@code criteria} within the specified
     * {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param realms the {@link Realms} filter
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> count(Class<T> clazz,
            Criteria criteria, Realms realms) {
        CountSelection<T> selection = new CountSelection<>(clazz, criteria,
                false);
        selection.realms = realms;
        return selection;
    }

    /**
     * Create a {@link CountSelection} that counts all {@link Record Records} of
     * the given {@code clazz} within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param realms the {@link Realms} filter
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> count(Class<T> clazz,
            Realms realms) {
        CountSelection<T> selection = new CountSelection<>(clazz, null, false);
        selection.realms = realms;
        return selection;
    }

    /**
     * Create a {@link CountSelection} that counts all {@link Record Records} of
     * the given {@code clazz} and its descendants.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> countAny(
            Class<T> clazz) {
        return new CountSelection<>(clazz, null, true);
    }

    /**
     * Create a {@link CountSelection} that counts {@link Record Records} of the
     * given {@code clazz} and its descendants matching the {@code criteria}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> countAny(Class<T> clazz,
            Criteria criteria) {
        return new CountSelection<>(clazz, criteria, true);
    }

    /**
     * Create a {@link CountSelection} that counts {@link Record Records} of the
     * given {@code clazz} and its descendants matching the {@code criteria}
     * within the specified {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @param realms the {@link Realms} filter
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> countAny(Class<T> clazz,
            Criteria criteria, Realms realms) {
        CountSelection<T> selection = new CountSelection<>(clazz, criteria,
                true);
        selection.realms = realms;
        return selection;
    }

    /**
     * Create a {@link CountSelection} that counts all {@link Record Records} of
     * the given {@code clazz} and its descendants within the specified
     * {@code realms}.
     *
     * @param clazz the {@link Record} class
     * @param realms the {@link Realms} filter
     * @return a new {@link CountSelection}
     */
    public static <T extends Record> CountSelection<T> countAny(Class<T> clazz,
            Realms realms) {
        CountSelection<T> selection = new CountSelection<>(clazz, null, true);
        selection.realms = realms;
        return selection;
    }

    /**
     * The target {@link Record} class.
     */
    final Class<T> clazz;

    /**
     * Whether to include descendants of {@link #clazz} in the results.
     */
    final boolean any;

    /**
     * The {@link Realms} filter.
     */
    Realms realms = Realms.any();

    /**
     * The current lifecycle state.
     */
    volatile State state = State.PENDING;

    /**
     * The result of the selection.
     */
    Object result;

    /**
     * Construct a new {@link Selection}.
     *
     * @param clazz the target class
     * @param any whether to include descendants
     */
    Selection(Class<T> clazz, boolean any) {
        this.clazz = clazz;
        this.any = any;
    }

    /**
     * Return the result of this {@link Selection}.
     * <p>
     * The return type is unchecked &mdash; the caller is responsible for
     * casting to the appropriate type.
     * </p>
     *
     * @param <R> the expected result type
     * @return the result
     * @throws IllegalStateException if this {@link Selection} has not finished
     *             execution
     */
    @SuppressWarnings("unchecked")
    public <R> R get() {
        checkState(state == State.FINISHED, "Selection has not been executed");
        return (R) result;
    }

    /**
     * Return {@code true} if this {@link Selection} can be combined with other
     * {@link Selection Selections} in a single database call.
     *
     * @return {@code true} if combinable
     */
    abstract boolean isCombinable();

    /**
     * Return {@code true} if this is a counting {@link Selection}.
     *
     * @return {@code true} if counting
     */
    boolean isCounting() {
        return false;
    }

    /**
     * Ensure this {@link Selection} is still in the {@link State#PENDING}
     * state.
     *
     * @throws IllegalStateException if already submitted
     */
    final void ensurePending() {
        checkState(state == State.PENDING,
                "Selection has already been submitted");
    }

    /**
     * The lifecycle state of a {@link Selection}.
     */
    enum State {

        /**
         * The {@link Selection} has been created but not yet submitted for
         * execution.
         */
        PENDING,

        /**
         * The {@link Selection} has been submitted to a {@link Runway} and is
         * being processed.
         */
        SUBMITTED,

        /**
         * The {@link Selection} has finished execution and results are
         * available.
         */
        FINISHED
    }

}
