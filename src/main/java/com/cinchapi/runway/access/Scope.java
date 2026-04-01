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

import javax.annotation.concurrent.Immutable;

import com.cinchapi.ccl.syntax.ConditionTree;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Selection;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

/**
 * Describes the visibility that an {@link Audience} has for a given
 * {@link Record} class, and knows how to apply that visibility to a
 * {@link Selection}.
 * <p>
 * A {@link Scope} is obtained by registering a provider with
 * {@link AccessControl#registerVisibilityScope(Class, java.util.function.Function)}
 * and is resolved at query time by {@link Audience#select(Selection[])}.
 * </p>
 * <p>
 * There are four variants:
 * </p>
 * <ul>
 * <li>{@link #unrestricted()} &mdash; the audience sees all records; no filter
 * or criteria is applied.</li>
 * <li>{@link #none()} &mdash; the audience sees no records; results are always
 * empty.</li>
 * <li>{@link #of(Criteria)} &mdash; visibility is expressed as a
 * {@link Criteria} that is pushed into the query.</li>
 * <li>{@link #unsupported()} &mdash; visibility cannot be expressed as a
 * database constraint for this combination.</li>
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
     *
     * @param criteria the {@link Criteria} that limits which records are
     *            visible to the {@link Audience}
     * @return a new criteria-based {@link Scope}
     */
    public static Scope of(Criteria criteria) {
        return new CriteriaBased(criteria);
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
            ConcourseCompiler compiler = ConcourseCompiler.get();
            ConditionTree tree = (ConditionTree) compiler.parse(criteria);
            Multimap<String, Object> mmap = Reflection.call(record, "mmap");
            return compiler.evaluate(tree, mmap); /* (authorized) */
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
            Reflection.set("result", result, selection); /* (authorized) */
            Reflection.set("state", Selection.State.RESOLVED,
                    selection); /* (authorized) */
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
