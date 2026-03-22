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
 * Each {@link Selection} targets a specific {@link Record} class and can
 * optionally specify an ID for a single-record lookup, a {@link Criteria} for
 * filtered queries, and sorting or pagination constraints.
 * </p>
 * <p>
 * A {@link Selection} has a lifecycle with three states:
 * <ul>
 * <li>{@link State#PENDING} &mdash; just created, not yet submitted</li>
 * <li>{@link State#SUBMITTED} &mdash; submitted to a {@link Runway} for
 * processing</li>
 * <li>{@link State#FINISHED} &mdash; execution complete, results available</li>
 * </ul>
 * Configuration methods may only be called while {@link State#PENDING}. Results
 * may only be retrieved when {@link State#FINISHED}. A {@link Selection} can
 * only be submitted once.
 * </p>
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
public class Selection<T extends Record> {

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

    /**
     * Create a {@link Selection} that loads all {@link Record Records} of the
     * given {@code clazz}.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> of(Class<T> clazz) {
        return new Selection<>(clazz, null, null, false);
    }

    /**
     * Create a {@link Selection} that loads a single {@link Record} of the
     * given {@code clazz} by its {@code id}.
     *
     * @param clazz the {@link Record} class
     * @param id the record ID
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> of(Class<T> clazz, long id) {
        return new Selection<>(clazz, id, null, false);
    }

    /**
     * Create a {@link Selection} that finds {@link Record Records} of the given
     * {@code clazz} matching the {@code criteria}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> of(Class<T> clazz,
            Criteria criteria) {
        return new Selection<>(clazz, null, criteria, false);
    }

    /**
     * Create a {@link Selection} for the given {@code clazz} with all optional
     * parameters. The {@link Selection} is automatically configured based on
     * which arguments are {@code null}:
     * <ul>
     * <li>If {@code criteria} is non-null, this is a criteria-based query
     * (find). Otherwise, it is a load-all.</li>
     * <li>{@code order}, {@code page}, and {@code realms} are applied when
     * non-null.</li>
     * </ul>
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria, or {@code null} for load-all
     * @param order the sort order, or {@code null}
     * @param page the pagination, or {@code null}
     * @param realms the {@link Realms} filter, or {@code null} for all realms
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> of(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Order order,
            @Nullable Page page, @Nullable Realms realms) {
        Selection<T> selection = new Selection<>(clazz, null, criteria, false);
        selection.order = order;
        selection.page = page;
        if(realms != null) {
            selection.realms = realms;
        }
        return selection;
    }

    /**
     * Create a {@link Selection} that loads all {@link Record Records} of the
     * given {@code clazz} and its descendants.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> ofAny(Class<T> clazz) {
        return new Selection<>(clazz, null, null, true);
    }

    /**
     * Create a {@link Selection} that loads a single {@link Record} of the
     * given {@code clazz} or its descendants by {@code id}.
     *
     * @param clazz the {@link Record} class
     * @param id the record ID
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> ofAny(Class<T> clazz,
            long id) {
        return new Selection<>(clazz, id, null, true);
    }

    /**
     * Create a {@link Selection} that finds {@link Record Records} of the given
     * {@code clazz} and its descendants matching the {@code criteria}.
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> ofAny(Class<T> clazz,
            Criteria criteria) {
        return new Selection<>(clazz, null, criteria, true);
    }

    /**
     * Create a {@link Selection} for the given {@code clazz} and its
     * descendants with all optional parameters. The {@link Selection} is
     * automatically configured based on which arguments are {@code null}:
     * <ul>
     * <li>If {@code criteria} is non-null, this is a criteria-based query
     * (find). Otherwise, it is a load-all.</li>
     * <li>{@code order}, {@code page}, and {@code realms} are applied when
     * non-null.</li>
     * </ul>
     *
     * @param clazz the {@link Record} class
     * @param criteria the query criteria, or {@code null} for load-all
     * @param order the sort order, or {@code null}
     * @param page the pagination, or {@code null}
     * @param realms the {@link Realms} filter, or {@code null} for all realms
     * @return a new {@link Selection}
     */
    public static <T extends Record> Selection<T> ofAny(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Order order,
            @Nullable Page page, @Nullable Realms realms) {
        Selection<T> selection = new Selection<>(clazz, null, criteria, true);
        selection.order = order;
        selection.page = page;
        if(realms != null) {
            selection.realms = realms;
        }
        return selection;
    }

    /**
     * The target {@link Record} class.
     */
    final Class<T> clazz;

    /**
     * The record ID for single-record lookups, or {@code null} if this is not
     * an ID-based selection.
     */
    @Nullable
    final Long id;

    /**
     * The query criteria, or {@code null} if this is a load-all or ID-based
     * selection.
     */
    final Criteria criteria;

    /**
     * Whether to include descendants of {@link #clazz} in the results.
     */
    final boolean any;

    /**
     * The sort order, or {@code null} for no sorting.
     */
    @Nullable
    Order order;

    /**
     * The pagination, or {@code null} for no pagination.
     */
    @Nullable
    Page page;

    /**
     * The {@link Realms} filter.
     */
    Realms realms = Realms.any();

    /**
     * The current lifecycle state.
     */
    volatile State state = State.PENDING;

    /**
     * The result of the selection. For ID-based selections, this is a single
     * {@link Record} (or {@code null}). For criteria-based or load-all
     * selections, this is a {@link java.util.Set Set&lt;T&gt;}.
     */
    Object result;

    /**
     * Construct a new {@link Selection}.
     *
     * @param clazz the target class
     * @param id the record ID, or {@code null}
     * @param criteria the query criteria, or {@code null}
     * @param any whether to include descendants
     */
    private Selection(Class<T> clazz, @Nullable Long id,
            @Nullable Criteria criteria, boolean any) {
        this.clazz = clazz;
        this.id = id;
        this.criteria = criteria;
        this.any = any;
    }

    /**
     * Return the result of this {@link Selection}.
     * <p>
     * For ID-based selections, the result is a single {@link Record} (or
     * {@code null} if not found). For criteria-based or load-all selections,
     * the result is a {@link java.util.Set Set} of {@link Record Records}.
     * </p>
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
     * Sort the results of this {@link Selection} by the given {@code order}.
     *
     * @param order the sort order
     * @return this {@link Selection} for chaining
     * @throws IllegalStateException if this {@link Selection} is not
     *             {@link State#PENDING}
     */
    public Selection<T> order(Order order) {
        checkState(state == State.PENDING,
                "Selection has already been submitted");
        this.order = order;
        return this;
    }

    /**
     * Paginate the results of this {@link Selection} by the given {@code page}.
     *
     * @param page the pagination
     * @return this {@link Selection} for chaining
     * @throws IllegalStateException if this {@link Selection} is not
     *             {@link State#PENDING}
     */
    public Selection<T> page(Page page) {
        checkState(state == State.PENDING,
                "Selection has already been submitted");
        this.page = page;
        return this;
    }

    /**
     * Constrain this {@link Selection} to the given {@code realms}.
     *
     * @param realms the {@link Realms} filter
     * @return this {@link Selection} for chaining
     * @throws IllegalStateException if this {@link Selection} is not
     *             {@link State#PENDING}
     */
    public Selection<T> realms(Realms realms) {
        checkState(state == State.PENDING,
                "Selection has already been submitted");
        this.realms = realms;
        return this;
    }

    /**
     * Return {@code true} if this {@link Selection} can be combined with other
     * {@link Selection Selections} in a single database call. A
     * {@link Selection} is combinable if it has no server-side ordering or
     * pagination, or if it is an ID-based lookup.
     *
     * @return {@code true} if this {@link Selection} is combinable
     */
    boolean isCombinable() {
        return (order == null && page == null) || isById();
    }

    /**
     * Return {@code true} if this is an ID-based selection.
     *
     * @return {@code true} if selecting by ID
     */
    boolean isById() {
        return id != null;
    }

}
