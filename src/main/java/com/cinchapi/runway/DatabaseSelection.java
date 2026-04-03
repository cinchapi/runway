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

import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.runway.Selection.State;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

/**
 * Base implementation of {@link Selection} that holds the resolved, immutable
 * state of a database operation.
 * <p>
 * Concrete subclasses ({@link FindSelection}, {@link LoadClassSelection},
 * {@link LoadRecordSelection}, {@link CountSelection}) are constructed from a
 * {@link BuilderState} and are immutable with respect to their configuration
 * fields. The {@link #state} and {@link #result} fields are mutable for use by
 * the dispatch machinery.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
abstract class DatabaseSelection<T extends Record> implements Selection<T> {

    /**
     * Return {@code true} if the given {@code filter} is the default no-op
     * filter.
     *
     * @param filter the filter to check
     * @return {@code true} if {@code filter} is the default
     */
    static boolean isNoFilter(Predicate<?> filter) {
        return filter == NO_FILTER;
    }

    /**
     * Resolve a {@link Selection} to an {@link DatabaseSelection}, building
     * from a builder if necessary.
     *
     * @param selection the {@link Selection} to resolve
     * @return the resolved {@link DatabaseSelection}
     * @throws IllegalArgumentException if {@code selection} is not a recognized
     *             type
     */
    static DatabaseSelection<?> resolve(Selection<?> selection) {
        if(selection instanceof DatabaseSelection) {
            return (DatabaseSelection<?>) selection;
        }
        else if(selection instanceof Selection.Builder) {
            Selection<?> built = ((Selection.Builder<?, ?>) selection).build();
            return (DatabaseSelection<?>) built;
        }
        else {
            throw new IllegalArgumentException("Unsupported Selection type: "
                    + selection.getClass().getName());
        }
    }

    /**
     * A default {@link Predicate} that accepts all items, used when no
     * client-side filter is specified.
     */
    static final Predicate<?> NO_FILTER = t -> true;

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
    final Realms realms;

    /**
     * The client-side filter applied to results before they are returned.
     * Defaults to {@link #NO_FILTER}.
     */
    @Nullable
    Predicate<T> filter;

    /**
     * The current lifecycle state.
     */
    volatile Selection.State state = Selection.State.PENDING;

    /**
     * The result of the selection.
     */
    Object result;

    /**
     * Construct a new {@link DatabaseSelection}.
     *
     * @param clazz the target class
     * @param any whether to include descendants
     * @param realms the realms filter
     */
    DatabaseSelection(Class<T> clazz, boolean any, Realms realms) {
        this(clazz, any, realms, NO_FILTER);
    }

    /**
     * Construct a new {@link DatabaseSelection}.
     *
     * @param clazz the target class
     * @param any whether to include descendants
     * @param realms the realms filter
     * @param filter
     */
    @SuppressWarnings("unchecked")
    DatabaseSelection(Class<T> clazz, boolean any, Realms realms,
            Predicate<?> filter) {
        this.clazz = clazz;
        this.any = any;
        this.realms = realms;
        this.filter = (Predicate<T>) filter;
    }

    @Override
    public Class<T> clazz() {
        return clazz;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R get() {
        checkState(state == Selection.State.FINISHED,
                "Selection has not been executed");
        return (R) result;
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public final String toString() {
        ToStringHelper helper = MoreObjects.toStringHelper(this).add("clazz",
                clazz.getSimpleName());
        describeSpec(helper);
        helper.add("realms", realms);
        if(any) {
            helper.add("any", true);
        }
        if(filter != null && !isNoFilter(filter)) {
            helper.add("hasFilter", true);
        }
        return helper.toString();
    }

    /**
     * Add type-specific fields to the {@link ToStringHelper} used by
     * {@link #toString()}.
     * <p>
     * Subclasses append their distinguishing properties (e.g., criteria, id,
     * order, page) to {@code helper}. Common fields ({@code clazz},
     * {@code realms}, {@code any}) are added by the caller and must not be
     * duplicated here.
     *
     * @param helper the {@link ToStringHelper} to populate
     */
    protected abstract void describeSpec(ToStringHelper helper);

    /**
     * Return a new {@link DatabaseSelection} with the same configuration as
     * this one but in the {@link State#PENDING} state and with no result. The
     * duplicate is independent of this instance.
     *
     * @return a fresh copy of this {@link DatabaseSelection}
     */
    abstract DatabaseSelection<T> duplicate();

    /**
     * Ensure this {@link DatabaseSelection} is still in the
     * {@link State#PENDING} state.
     *
     * @throws IllegalStateException if already submitted
     */
    final void ensurePending() {
        checkState(state == Selection.State.PENDING,
                "Selection has already been submitted");
    }

    /**
     * Return {@code true} if this {@link DatabaseSelection} can be combined
     * with other {@link DatabaseSelection Selections} in a single database
     * call.
     *
     * @return {@code true} if combinable
     */
    abstract boolean isCombinable();

    /**
     * Return {@code true} if this is a counting {@link DatabaseSelection}.
     *
     * @return {@code true} if counting
     */
    boolean isCounting() {
        return false;
    }

    /**
     * Return {@code true} if this is a unique-result {@link DatabaseSelection}.
     *
     * @return {@code true} if unique
     */
    boolean isUnique() {
        return false;
    }

    /**
     * Return the {@link Reservation} that represents the canonical cache key
     * for this {@link DatabaseSelection}. Two {@link DatabaseSelection
     * DatabaseSelections} with equivalent configuration produce equal
     * {@link Reservation Reservations}.
     *
     * @return the {@link Reservation} for this {@link DatabaseSelection}
     */
    abstract Reservation reservation();

    /**
     * Mutable state accumulated during the build process.
     *
     * @param <T> the {@link Record} type
     */
    static final class BuilderState<T extends Record> {

        /**
         * The target {@link Record} class.
         */
        final Class<T> clazz;

        /**
         * Whether to include descendants.
         */
        boolean any;

        /**
         * The query criteria.
         */
        @Nullable
        Criteria criteria;

        /**
         * The sort order.
         */
        @Nullable
        Order order;

        /**
         * The pagination.
         */
        @Nullable
        Page page;

        /**
         * The client-side filter.
         */
        @SuppressWarnings("unchecked")
        Predicate<T> filter = (Predicate<T>) NO_FILTER;

        /**
         * The record ID for single-record loads.
         */
        @Nullable
        Long id;

        /**
         * Whether this is a counting operation.
         */
        boolean counting;

        /**
         * Whether this is a unique-result operation.
         */
        boolean unique;

        /**
         * The {@link Realms} filter.
         */
        Realms realms = Realms.any();

        /**
         * Construct a new {@link BuilderState}.
         *
         * @param clazz the target class
         * @param any whether to include descendants
         */
        BuilderState(Class<T> clazz, boolean any) {
            this.clazz = clazz;
            this.any = any;
        }
    }

}
