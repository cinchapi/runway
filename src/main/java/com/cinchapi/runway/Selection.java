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

import java.util.function.Predicate;

import com.cinchapi.ccl.syntax.ConditionTree;
import com.cinchapi.concourse.lang.ConcourseCompiler;
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
public interface Selection<T extends Record> {

    /**
     * Create a builder targeting the exact {@code clazz} provided, excluding
     * descendants.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link InitialBuilder}
     */
    public static <T extends Record> InitialBuilder<T> of(Class<T> clazz) {
        return new InitialBuilder<>(
                new DatabaseSelection.BuilderState<>(clazz, false));
    }

    /**
     * Create a builder targeting {@code clazz} and all of its descendants.
     *
     * @param clazz the {@link Record} class
     * @return a new {@link InitialBuilder}
     */
    public static <T extends Record> InitialBuilder<T> ofAny(Class<T> clazz) {
        return new InitialBuilder<>(
                new DatabaseSelection.BuilderState<>(clazz, true));
    }

    /**
     * Return a copy of {@code selection} with the {@code injected}
     * {@link Criteria} applied as a database-level constraint.
     *
     * @param selection the {@link Selection} to augment; must be
     *            {@link State#PENDING}
     * @param injected the injected {@link Criteria}
     * @param <T> the {@link Record} type
     * @return a new {@link Selection} with the visibility constraint applied
     */
    @SuppressWarnings("unchecked")
    public static <T extends Record> Selection<T> withInjectedCriteria(
            Selection<T> selection, Criteria injected) {
        DatabaseSelection<T> resolved = (DatabaseSelection<T>) DatabaseSelection
                .resolve(selection);
        if(resolved instanceof LoadRecordSelection) {
            return withInjectedFilter(selection, record -> {
                ConcourseCompiler compiler = ConcourseCompiler.get();
                ConditionTree tree = (ConditionTree) compiler.parse(injected);
                return compiler.evaluate(tree, record.mmap());
            });
        }
        else {
            resolved = resolved.duplicate();
            if(resolved instanceof FindSelection) {
                FindSelection<T> find = (FindSelection<T>) resolved;
                DatabaseSelection.BuilderState<T> state = new DatabaseSelection.BuilderState<>(
                        resolved.clazz, resolved.any);
                state.criteria = Criteria.where().group(find.criteria).and()
                        .group(injected).build();
                state.order = find.order;
                state.page = find.page;
                state.filter = find.filter;
                state.realms = find.realms;
                return new FindSelection<>(state);
            }
            else if(resolved instanceof CountSelection) {
                CountSelection<T> count = (CountSelection<T>) resolved;
                DatabaseSelection.BuilderState<T> state = new DatabaseSelection.BuilderState<>(
                        resolved.clazz, resolved.any);
                state.counting = true;
                state.criteria = count.criteria != null
                        ? Criteria.where().group(count.criteria).and()
                                .group(injected).build()
                        : injected;
                state.filter = count.filter;
                state.realms = count.realms;
                return new CountSelection<>(state);
            }
            else {
                // LoadClassSelection — visibility criteria becomes the sole
                // criteria, promoting to FindSelection
                SetBasedSelection<T> set = (SetBasedSelection<T>) resolved;
                DatabaseSelection.BuilderState<T> state = new DatabaseSelection.BuilderState<>(
                        resolved.clazz, resolved.any);
                state.criteria = injected;
                state.order = set.order;
                state.page = set.page;
                state.filter = resolved.filter;
                state.realms = resolved.realms;
                return new FindSelection<>(state);
            }
        }
    }

    /**
     * Return a copy of {@code selection} with the given {@code filter}.
     *
     * @param selection the {@link Selection} to augment
     * @param filter the injected filter
     * @param <T> the {@link Record} type
     * @return a new {@link Selection} with the visibility constraint applied
     */
    @SuppressWarnings("unchecked")
    public static <T extends Record> Selection<T> withInjectedFilter(
            Selection<T> selection, Predicate<? super T> filter) {
        DatabaseSelection<T> resolved = (DatabaseSelection<T>) DatabaseSelection
                .resolve(selection);
        resolved = resolved.duplicate();
        resolved.filter = filter == null || DatabaseSelection.isNoFilter(filter)
                ? (Predicate<T>) filter
                : resolved.filter.and(filter);
        return resolved;
    }

    /**
     * Return the target {@link Record} class for this {@link Selection}.
     *
     * @return the target class
     */
    public Class<T> clazz();

    /**
     * Return the result of this {@link Selection}.
     * <p>
     * The return type is unchecked &mdash; the caller is responsible for
     * casting to the appropriate type.
     *
     * @param <R> the expected result type
     * @return the result
     * @throws IllegalStateException if this {@link Selection} has not finished
     *             execution
     */
    public <R> R get();

    /**
     * Return the {@link State} of this {@link Selection}.
     *
     * @return the {@link State} of this {@link Selection}
     */
    public State state();

    /**
     * Abstract base for all {@link Selection} builders. Provides methods that
     * are always valid: {@link #realms(Realms)}, {@link #any()},
     * {@link #any(boolean)}, and {@link #build()}.
     * <p>
     * Each builder implements {@link Selection} so it can be passed directly to
     * {@link DatabaseInterface#select(Selection, Selection...)} without an
     * explicit {@link #build()} call.
     *
     * @param <T> the {@link Record} type
     * @param <B> the concrete builder type (self-referencing)
     */
    abstract class Builder<T extends Record, B extends Builder<T, B>> implements
            Selection<T> {

        /**
         * The accumulated builder state.
         */
        final DatabaseSelection.BuilderState<T> state;

        /**
         * Construct a new {@link Builder}.
         *
         * @param state the shared builder state
         */
        Builder(DatabaseSelection.BuilderState<T> state) {
            this.state = state;
        }

        /**
         * Include descendants of the target class in the results.
         *
         * @return this builder for chaining
         */
        public B any() {
            state.any = true;
            return self();
        }

        /**
         * Set whether to include descendants of the target class in the
         * results.
         *
         * @param any {@code true} to include descendants
         * @return this builder for chaining
         */
        public B any(boolean any) {
            state.any = any;
            return self();
        }

        /**
         * Build an immutable {@link Selection} from the accumulated state.
         *
         * @return a concrete {@link Selection}
         */
        public Selection<T> build() {
            if(state.id != null) {
                return new LoadRecordSelection<>(state);
            }
            else if(state.counting) {
                return new CountSelection<>(state);
            }
            else if(state.criteria != null) {
                return new FindSelection<>(state);
            }
            else {
                return new LoadClassSelection<>(state);
            }
        }

        @Override
        public Class<T> clazz() {
            return state.clazz;
        }

        @Override
        public <R> R get() {
            throw new IllegalStateException("Selection has not been executed");
        }

        /**
         * Constrain the {@link Selection} to the given {@code realms}.
         *
         * @param realms the {@link Realms} filter
         * @return this builder for chaining
         */
        public B realms(Realms realms) {
            state.realms = realms;
            return self();
        }

        @Override
        public State state() {
            return State.PENDING;
        }

        /**
         * Return {@code this} cast to the concrete builder type.
         */
        @SuppressWarnings("unchecked")
        private B self() {
            return (B) this;
        }
    }

    /**
     * A builder for counting {@link Selection Selections}. Only criteria,
     * filter, realms, and any are available.
     *
     * @param <T> the {@link Record} type
     */
    final class CountBuilder<T extends Record>
            extends Builder<T, CountBuilder<T>> {

        /**
         * Construct a new {@link CountBuilder}.
         *
         * @param state the shared builder state
         */
        CountBuilder(DatabaseSelection.BuilderState<T> state) {
            super(state);
        }

        /**
         * Alias for {@link #where(Criteria)}.
         *
         * @param criteria the query criteria
         * @return this builder for chaining
         */
        public CountBuilder<T> criteria(Criteria criteria) {
            return where(criteria);
        }

        /**
         * Apply a client-side {@code filter}.
         *
         * @param filter the filter predicate
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public CountBuilder<T> filter(Predicate<T> filter) {
            state.filter = filter != null ? filter
                    : (Predicate<T>) DatabaseSelection.NO_FILTER;
            return this;
        }

        /**
         * Set or replace the query {@code criteria}.
         *
         * @param criteria the query criteria
         * @return this builder for chaining
         */
        public CountBuilder<T> where(Criteria criteria) {
            state.criteria = criteria;
            return this;
        }
    }

    /**
     * A builder for ID-based {@link Selection Selections}. Only realms and any
     * are available.
     *
     * @param <T> the {@link Record} type
     */
    final class IdBuilder<T extends Record> extends Builder<T, IdBuilder<T>> {

        /**
         * Construct a new {@link IdBuilder}.
         *
         * @param state the shared builder state
         */
        IdBuilder(DatabaseSelection.BuilderState<T> state) {
            super(state);
        }
    }

    /**
     * The initial builder state where all configuration methods are available.
     *
     * @param <T> the {@link Record} type
     */
    final class InitialBuilder<T extends Record>
            extends Builder<T, InitialBuilder<T>> {

        /**
         * Construct a new {@link InitialBuilder}.
         *
         * @param state the shared builder state
         */
        InitialBuilder(DatabaseSelection.BuilderState<T> state) {
            super(state);
        }

        /**
         * Mark this as a counting operation.
         *
         * @return a {@link CountBuilder} for chaining
         */
        public CountBuilder<T> count() {
            state.counting = true;
            return new CountBuilder<>(state);
        }

        /**
         * Alias for {@link #where(Criteria)}.
         *
         * @param criteria the query criteria
         * @return a {@link QueryBuilder} for chaining
         */
        public QueryBuilder<T> criteria(Criteria criteria) {
            return where(criteria);
        }

        /**
         * Apply a client-side {@code filter}.
         *
         * @param filter the filter predicate
         * @return a {@link QueryBuilder} for chaining
         */
        @SuppressWarnings("unchecked")
        public QueryBuilder<T> filter(Predicate<T> filter) {
            state.filter = filter != null ? filter
                    : (Predicate<T>) DatabaseSelection.NO_FILTER;
            return new QueryBuilder<>(state);
        }

        /**
         * Set the record {@code id}, making this a single-record load
         * operation.
         *
         * @param id the record ID
         * @return an {@link IdBuilder} for chaining
         */
        public IdBuilder<T> id(long id) {
            state.id = id;
            return new IdBuilder<>(state);
        }

        /**
         * Set the sort {@code order}.
         *
         * @param order the sort order
         * @return a {@link SortableBuilder} for chaining
         */
        public SortableBuilder<T> order(Order order) {
            state.order = order;
            return new SortableBuilder<>(state);
        }

        /**
         * Set the {@code page} for pagination.
         *
         * @param page the pagination
         * @return a {@link SortableBuilder} for chaining
         */
        public SortableBuilder<T> page(Page page) {
            state.page = page;
            return new SortableBuilder<>(state);
        }

        /**
         * Set the query {@code criteria}.
         *
         * @param criteria the query criteria
         * @return a {@link QueryBuilder} for chaining
         */
        public QueryBuilder<T> where(Criteria criteria) {
            state.criteria = criteria;
            return new QueryBuilder<>(state);
        }
    }

    /**
     * A builder for {@link Selection Selections} that have criteria or a filter
     * set but no sort order or pagination yet. Counting is still available.
     *
     * @param <T> the {@link Record} type
     */
    final class QueryBuilder<T extends Record>
            extends Builder<T, QueryBuilder<T>> {

        /**
         * Construct a new {@link QueryBuilder}.
         *
         * @param state the shared builder state
         */
        QueryBuilder(DatabaseSelection.BuilderState<T> state) {
            super(state);
        }

        /**
         * Mark this as a counting operation.
         *
         * @return a {@link CountBuilder} for chaining
         */
        public CountBuilder<T> count() {
            state.counting = true;
            return new CountBuilder<>(state);
        }

        /**
         * Alias for {@link #where(Criteria)}.
         *
         * @param criteria the query criteria
         * @return this builder for chaining
         */
        public QueryBuilder<T> criteria(Criteria criteria) {
            return where(criteria);
        }

        /**
         * Apply a client-side {@code filter}.
         *
         * @param filter the filter predicate
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public QueryBuilder<T> filter(Predicate<T> filter) {
            state.filter = filter != null ? filter
                    : (Predicate<T>) DatabaseSelection.NO_FILTER;
            return this;
        }

        /**
         * Set the sort {@code order}.
         *
         * @param order the sort order
         * @return a {@link SortableBuilder} for chaining
         */
        public SortableBuilder<T> order(Order order) {
            state.order = order;
            return new SortableBuilder<>(state);
        }

        /**
         * Set the {@code page} for pagination.
         *
         * @param page the pagination
         * @return a {@link SortableBuilder} for chaining
         */
        public SortableBuilder<T> page(Page page) {
            state.page = page;
            return new SortableBuilder<>(state);
        }

        /**
         * Set or replace the query {@code criteria}.
         *
         * @param criteria the query criteria
         * @return this builder for chaining
         */
        public QueryBuilder<T> where(Criteria criteria) {
            state.criteria = criteria;
            return this;
        }
    }

    /**
     * A builder for {@link Selection Selections} that have a sort order or
     * pagination set. Counting and ID-based lookups are no longer available.
     *
     * @param <T> the {@link Record} type
     */
    final class SortableBuilder<T extends Record>
            extends Builder<T, SortableBuilder<T>> {

        /**
         * Construct a new {@link SortableBuilder}.
         *
         * @param state the shared builder state
         */
        SortableBuilder(DatabaseSelection.BuilderState<T> state) {
            super(state);
        }

        /**
         * Alias for {@link #where(Criteria)}.
         *
         * @param criteria the query criteria
         * @return this builder for chaining
         */
        public SortableBuilder<T> criteria(Criteria criteria) {
            return where(criteria);
        }

        /**
         * Apply a client-side {@code filter}.
         *
         * @param filter the filter predicate
         * @return this builder for chaining
         */
        @SuppressWarnings("unchecked")
        public SortableBuilder<T> filter(Predicate<T> filter) {
            state.filter = filter != null ? filter
                    : (Predicate<T>) DatabaseSelection.NO_FILTER;
            return this;
        }

        /**
         * Set or replace the sort {@code order}.
         *
         * @param order the sort order
         * @return this builder for chaining
         */
        public SortableBuilder<T> order(Order order) {
            state.order = order;
            return this;
        }

        /**
         * Set or replace the {@code page} for pagination.
         *
         * @param page the pagination
         * @return this builder for chaining
         */
        public SortableBuilder<T> page(Page page) {
            state.page = page;
            return this;
        }

        /**
         * Set or replace the query {@code criteria}.
         *
         * @param criteria the query criteria
         * @return this builder for chaining
         */
        public SortableBuilder<T> where(Criteria criteria) {
            state.criteria = criteria;
            return this;
        }
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
        FINISHED,

        /**
         * The {@link Selection} is in a transient state where it has been
         * resolved, but not yet marked as finished.
         */
        RESOLVED
    }

}
