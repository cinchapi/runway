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
package com.cinchapi.runway.access;

import java.util.function.Predicate;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Selection;
import com.google.common.collect.ImmutableSet;

/**
 * Describes the visibility that an {@link Audience} has for a given
 * {@link Record} class.
 * <p>
 * Providers are registered via
 * {@link AccessControl#registerVisibilityScope(Class, java.util.function.Function)}.
 * </p>
 * <p>
 * There are five variants:
 * </p>
 * <ul>
 * <li>{@link #unrestricted()} &mdash; the audience sees all records.</li>
 * <li>{@link #none()} &mdash; the audience sees no records.</li>
 * <li>{@link #of(Criteria)} &mdash; visibility is expressed as a
 * {@link Criteria}.</li>
 * <li>{@link #hybrid(Criteria, Audience)} /
 * {@link #hybrid(Criteria, Predicate)} &mdash; visibility is expressed as a
 * {@link Criteria} together with an independent per-record
 * {@link Predicate}.</li>
 * <li>{@link #unsupported()} &mdash; visibility cannot be expressed for this
 * combination.</li>
 * </ul>
 *
 * @author Jeff Nelson
 */
@Immutable
public abstract class Scope {

    /**
     * Return a {@link Scope} indicating that the {@link Audience} can see no
     * records of the class.
     *
     * @return the shared none {@link Scope} singleton
     */
    public static Scope none() {
        return None.INSTANCE;
    }

    /**
     * Return a {@link Scope} whose visibility is expressed by the given
     * {@link Criteria}.
     * <p>
     * {@code criteria} must be locally evaluable on a {@link Record}; use
     * {@link #hybrid(Criteria, Audience)} or
     * {@link #hybrid(Criteria, Predicate)} when {@code criteria} contains a
     * scoped navigation clause such as
     * {@code Criteria.where().scope(prefix, inner)}.
     * </p>
     *
     * @param criteria the {@link Criteria} that limits which records are
     *            visible to the {@link Audience}
     * @return a new criteria-based {@link Scope}
     */
    public static Scope of(Criteria criteria) {
        return new CriteriaBased(criteria);
    }

    /**
     * Return a {@link Scope} whose visibility is expressed by {@code criteria}
     * and whose per-record test is the {@link Audience Audience's}
     * discoverability ({@link AccessControl#$isDiscoverableBy(Audience)}, or
     * {@link AccessControl#$isDiscoverableByAnonymous()} for an anonymous
     * {@link Audience}).
     * <p>
     * Suitable when {@code criteria} is not locally evaluable on a
     * {@link Record} &mdash; for example, when it contains a scoped navigation
     * clause such as {@code Criteria.where().scope(prefix, inner)}.
     * </p>
     * <p>
     * <strong>Caller's responsibility:</strong> {@code criteria} and the
     * default discoverability check must produce logically equivalent results
     * for {@code audience}, or visibility becomes inconsistent. Use
     * {@link #hybrid(Criteria, Predicate)} to supply an explicit
     * {@link Predicate} when discoverability is not an accurate mirror of
     * {@code criteria}.
     * </p>
     *
     * @param criteria the {@link Criteria} that limits which records are
     *            visible to {@code audience}
     * @param audience the {@link Audience} whose discoverability check backs
     *            the per-record predicate
     * @return a new hybrid {@link Scope}
     */
    public static Scope hybrid(Criteria criteria, Audience audience) {
        Predicate<? super Record> defaultPredicate = record -> {
            if(record instanceof AccessControl) {
                AccessControl subject = (AccessControl) record;
                return audience instanceof Anonymous
                        ? subject.$isDiscoverableByAnonymous()
                        : subject.$isDiscoverableBy(audience);
            }
            return true;
        };
        return new Hybrid(criteria, defaultPredicate);
    }

    /**
     * Return a {@link Scope} whose visibility is expressed by {@code criteria}
     * and whose per-record test is {@code predicate}.
     * <p>
     * Suitable when {@code criteria} is not locally evaluable on a
     * {@link Record} &mdash; for example, when it contains a scoped navigation
     * clause such as {@code Criteria.where().scope(prefix, inner)}.
     * </p>
     * <p>
     * <strong>Caller's responsibility:</strong> {@code criteria} and
     * {@code predicate} must produce logically equivalent results for the
     * {@link Audience} this {@link Scope} is being constructed for, or
     * visibility becomes inconsistent.
     * </p>
     *
     * @param criteria the {@link Criteria} that limits which records are
     *            visible to the {@link Audience}
     * @param predicate the per-record visibility {@link Predicate}
     * @return a new hybrid {@link Scope}
     */
    public static Scope hybrid(Criteria criteria,
            Predicate<? super Record> predicate) {
        return new Hybrid(criteria, predicate);
    }

    /**
     * Return a {@link Scope} indicating that the {@link Audience} has
     * unrestricted visibility: all records of the class are visible.
     *
     * @return the shared unrestricted {@link Scope} singleton
     */
    public static Scope unrestricted() {
        return Unrestricted.INSTANCE;
    }

    /**
     * Return a {@link Scope} indicating that visibility cannot be expressed as
     * a database constraint for this {@link Audience} and {@link Record}
     * combination.
     *
     * @return the shared unsupported {@link Scope} singleton
     */
    public static Scope unsupported() {
        return Unsupported.INSTANCE;
    }

    /**
     * Private constructor prevents external subclassing.
     */
    private Scope() {}

    /**
     * Apply this {@link Scope} to {@code selection} and return the modified
     * {@link Selection}.
     *
     * @param selection the {@link Selection} to apply this scope to
     * @return the modified {@link Selection}
     * @throws UnsupportedOperationException if {@link #isApplicable()} returns
     *             {@code false}
     */
    public abstract Selection<?> apply(Selection<?> selection);

    /**
     * Return {@code true} if this {@link Scope} can be applied to a
     * {@link Selection}.
     *
     * @return {@code true} if supported
     */
    public abstract boolean isApplicable();

    /**
     * Test whether the given {@code record} falls within this {@link Scope}.
     *
     * @param record the {@link Record} to test
     * @return {@code true} if the {@code record} is within this {@link Scope}
     */
    public abstract boolean test(Record record);

    /**
     * A {@link Scope} whose visibility is expressed as a {@link Criteria}.
     */
    @Immutable
    private static final class CriteriaBased extends Scope {

        /**
         * The visibility criteria.
         */
        private final Criteria criteria;

        /**
         * Construct a new {@link CriteriaBased}.
         *
         * @param criteria the visibility criteria
         */
        private CriteriaBased(Criteria criteria) {
            this.criteria = criteria;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Selection<?> apply(Selection<?> selection) {
            return Selection.withInjectedCriteria((Selection<Record>) selection,
                    criteria);
        }

        @Override
        public boolean isApplicable() {
            return true;
        }

        @Override
        public boolean test(Record record) {
            return record.matches(criteria);
        }
    }

    /**
     * A {@link Scope} whose visibility is expressed by a {@link Criteria}
     * together with an independent per-record {@link Predicate}.
     */
    @Immutable
    private static final class Hybrid extends Scope {

        /**
         * The visibility {@link Criteria}.
         */
        private final Criteria criteria;

        /**
         * The per-record visibility {@link Predicate}.
         */
        private final Predicate<? super Record> localTest;

        /**
         * Construct a new {@link Hybrid}.
         *
         * @param criteria the visibility {@link Criteria}
         * @param localTest the per-record {@link Predicate}
         */
        private Hybrid(Criteria criteria, Predicate<? super Record> localTest) {
            this.criteria = criteria;
            this.localTest = localTest;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Selection<?> apply(Selection<?> selection) {
            return Selection.withInjectedCriteria((Selection<Record>) selection,
                    criteria);
        }

        @Override
        public boolean isApplicable() {
            return true;
        }

        @Override
        public boolean test(Record record) {
            return localTest.test(record);
        }
    }

    /**
     * A {@link Scope} that grants no visibility.
     */
    @Immutable
    private static final class None extends Scope {

        /**
         * The singleton instance.
         */
        private static final None INSTANCE = new None();

        /**
         * Private constructor.
         */
        private None() {}

        @Override
        @SuppressWarnings("unchecked")
        public Selection<?> apply(Selection<?> selection) {
            selection = Selection.withInjectedFilter(
                    (Selection<Record>) selection, record -> false);
            String className = selection.getClass().getSimpleName();
            Object result;
            if(className.equals("CountSelection")) {
                result = 0;
            }
            else if(className.equals("LoadRecordSelection")) {
                result = null;
            }
            else {
                result = ImmutableSet.of();
            }
            Reflection.call(selection, "setResult", result); /* (authorized) */
            Reflection.call(selection, "setState",
                    Selection.State.RESOLVED); /* (authorized) */
            return selection;
        }

        @Override
        public boolean isApplicable() {
            return true;
        }

        @Override
        public boolean test(Record record) {
            return false;
        }
    }

    /**
     * A {@link Scope} that grants unrestricted visibility.
     */
    @Immutable
    private static final class Unrestricted extends Scope {

        /**
         * The singleton instance.
         */
        private static final Unrestricted INSTANCE = new Unrestricted();

        /**
         * Private constructor.
         */
        private Unrestricted() {}

        @Override
        public Selection<?> apply(Selection<?> selection) {
            return selection;
        }

        @Override
        public boolean isApplicable() {
            return true;
        }

        @Override
        public boolean test(Record record) {
            return true;
        }

    }

    /**
     * A {@link Scope} indicating that the visibility rule cannot be applied at
     * the database level.
     */
    @Immutable
    private static final class Unsupported extends Scope {

        /**
         * The singleton instance.
         */
        private static final Unsupported INSTANCE = new Unsupported();

        /**
         * Private constructor.
         */
        private Unsupported() {}

        @Override
        public Selection<?> apply(Selection<?> selection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isApplicable() {
            return false;
        }

        @Override
        public boolean test(Record record) {
            throw new UnsupportedOperationException();
        }

    }

}
