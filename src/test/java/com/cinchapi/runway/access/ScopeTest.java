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

import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Selection;

/**
 * Unit tests for the {@link Scope} factory methods and singleton guarantees.
 *
 * @author Jeff Nelson
 */
public class ScopeTest {

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unrestricted()} returns
     * the input {@link Selection} unchanged.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Selection}.</li>
     * <li>Call {@link Scope#unrestricted()}.{@code apply(selection)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Selection} is the same
     * reference as the input.
     */
    @Test
    public void testUnrestrictedApplyReturnsSameSelection() {
        Selection<?> selection = Selection.of(TestRecord.class);
        Assert.assertSame(selection, Scope.unrestricted().apply(selection));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#none()} returns a
     * different {@link Selection} with a filter injected.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Selection}.</li>
     * <li>Call {@link Scope#none()}.{@code apply(selection)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A non-null {@link Selection} is returned that
     * is not the same reference as the input.
     */
    @Test
    public void testNoneApplyReturnsModifiedSelection() {
        Selection<?> selection = Selection.of(TestRecord.class);
        Selection<?> result = Scope.none().apply(selection);
        Assert.assertNotNull(result);
        Assert.assertNotSame(selection, result);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#of(Criteria)} returns a
     * different {@link Selection} with criteria injected.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Criteria} and a {@link Selection}.</li>
     * <li>Call {@link Scope#of(Criteria)}.{@code apply(selection)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A non-null {@link Selection} is returned that
     * is not the same reference as the input.
     */
    @Test
    public void testOfApplyReturnsModifiedSelection() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Selection<?> selection = Selection.of(TestRecord.class);
        Selection<?> result = Scope.of(criteria).apply(selection);
        Assert.assertNotNull(result);
        Assert.assertNotSame(selection, result);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unsupported()} returns
     * {@code false} from {@link Scope#isApplicable()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#isApplicable()} on
     * {@link Scope#unsupported()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code false} is returned.
     */
    @Test
    public void testUnsupportedIsNotApplicable() {
        Assert.assertFalse(Scope.unsupported().isApplicable());
    }

    /**
     * <strong>Goal:</strong> Verify that all concrete {@link Scope} variants
     * that carry a meaningful visibility rule return {@code true} from
     * {@link Scope#isApplicable()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#isApplicable()} on {@link Scope#unrestricted()},
     * {@link Scope#none()}, and a {@link Scope#of(Criteria)} instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> All three return {@code true}.
     */
    @Test
    public void testApplicableVariantsReturnTrue() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Assert.assertTrue(Scope.unrestricted().isApplicable());
        Assert.assertTrue(Scope.none().isApplicable());
        Assert.assertTrue(Scope.of(criteria).isApplicable());
    }

    /**
     * <strong>Goal:</strong> Verify that calling {@link Scope#apply(Selection)}
     * on {@link Scope#unsupported()} throws
     * {@link UnsupportedOperationException}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#apply(Selection)} on
     * {@link Scope#unsupported()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedApplyThrows() {
        Scope.unsupported().apply(Selection.of(TestRecord.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unrestricted()} returns
     * the same singleton instance on every call.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#unrestricted()} twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same object reference.
     */
    @Test
    public void testUnrestrictedIsSingleton() {
        Assert.assertSame(Scope.unrestricted(), Scope.unrestricted());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#none()} returns the same
     * singleton instance on every call.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#none()} twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same object reference.
     */
    @Test
    public void testNoneIsSingleton() {
        Assert.assertSame(Scope.none(), Scope.none());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unsupported()} returns
     * the same singleton instance on every call.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#unsupported()} twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same object reference.
     */
    @Test
    public void testUnsupportedIsSingleton() {
        Assert.assertSame(Scope.unsupported(), Scope.unsupported());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Scope#hybrid(Criteria, Predicate)} returns a {@link Selection}
     * that differs from the input, routing the criteria through the
     * database-injection path just like {@link Scope#of(Criteria)}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a non-scoped {@link Criteria} and a {@link Selection}.</li>
     * <li>Call
     * {@link Scope#hybrid(Criteria, Predicate)}.{@code apply(selection)} with
     * an always-true predicate.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A non-null {@link Selection} is returned that
     * is not the same reference as the input.
     */
    @Test
    public void testHybridApplyReturnsModifiedSelection() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Selection<?> selection = Selection.of(TestRecord.class);
        Selection<?> result = Scope.hybrid(criteria, r -> true)
                .apply(selection);
        Assert.assertNotNull(result);
        Assert.assertNotSame(selection, result);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Scope#hybrid(Criteria, Predicate)} reports {@code true} from
     * {@link Scope#isApplicable()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a hybrid {@link Scope} with any criteria and
     * predicate.</li>
     * <li>Call {@link Scope#isApplicable()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code true} is returned.
     */
    @Test
    public void testHybridIsApplicable() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Assert.assertTrue(Scope.hybrid(criteria, r -> true).isApplicable());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Scope#hybrid(Criteria, Predicate)}.{@code test(record)} dispatches
     * to the supplied {@link Predicate} rather than evaluating the
     * {@link Criteria} locally. This is the whole point of the hybrid form: the
     * criteria is reserved for the database path and the predicate owns
     * per-record visibility.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a hybrid {@link Scope} with an always-true predicate and
     * call {@code test} on a record.</li>
     * <li>Construct a hybrid {@link Scope} with an always-false predicate and
     * call {@code test} on the same record.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first call returns {@code true}; the
     * second returns {@code false}.
     */
    @Test
    public void testHybridTestUsesExplicitPredicate() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        TestRecord record = new TestRecord();
        Assert.assertTrue(Scope.hybrid(criteria, r -> true).test(record));
        Assert.assertFalse(Scope.hybrid(criteria, r -> false).test(record));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Scope#hybrid(Audience, Criteria)} (the convenience form) defaults
     * the per-record predicate to {@link AccessControl#$isDiscoverableBy} for a
     * non-anonymous {@link Audience}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link TestAudience}.</li>
     * <li>Construct two {@link TestAccessControlRecord
     * TestAccessControlRecords}: one that is discoverable by the audience, one
     * that is not.</li>
     * <li>Call {@code Scope.hybrid(audience, criteria).test(record)} on
     * each.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The discoverable record passes; the
     * undiscoverable record does not.
     */
    @Test
    public void testHybridDefaultPredicateUsesIsDiscoverableBy() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        TestAudience audience = new TestAudience();
        Scope scope = Scope.hybrid(audience, criteria);
        Assert.assertTrue(scope.test(new TestAccessControlRecord(true, false)));
        Assert.assertFalse(
                scope.test(new TestAccessControlRecord(false, true)));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Scope#hybrid(Audience, Criteria)} defaults the per-record
     * predicate to {@link AccessControl#$isDiscoverableByAnonymous} when the
     * supplied {@link Audience} is {@link Audience#anonymous()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct two {@link TestAccessControlRecord
     * TestAccessControlRecords}: one anonymous-discoverable, one not.</li>
     * <li>Call
     * {@code Scope.hybrid(Audience.anonymous(), criteria).test(record)} on
     * each.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The anonymous-discoverable record passes; the
     * other does not.
     */
    @Test
    public void testHybridDefaultPredicateUsesAnonymousDiscoverability() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Scope scope = Scope.hybrid(Audience.anonymous(), criteria);
        Assert.assertTrue(scope.test(new TestAccessControlRecord(false, true)));
        Assert.assertFalse(
                scope.test(new TestAccessControlRecord(true, false)));
    }

    /**
     * <strong>Goal:</strong> Verify that the default predicate built by
     * {@link Scope#hybrid(Audience, Criteria)} returns {@code true} for records
     * that do not implement {@link AccessControl}, matching the permissive
     * treatment such records receive elsewhere in the framework.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a hybrid {@link Scope} with a {@link TestAudience}.</li>
     * <li>Call {@code test} on a {@link TestRecord} (not
     * {@link AccessControl}).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code true} is returned.
     */
    @Test
    public void testHybridDefaultPredicateAllowsNonAccessControlRecord() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Scope scope = Scope.hybrid(new TestAudience(), criteria);
        Assert.assertTrue(scope.test(new TestRecord()));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Scope#hybrid(Criteria, Predicate)}.{@code apply(selection)}
     * succeeds when the {@link Criteria} contains a scoped navigation clause.
     * This is the escape-hatch behavior that motivates the hybrid form:
     * {@link Scope#of(Criteria)} would also route through
     * {@link Selection#withInjectedCriteria}, but its per-record {@code test()}
     * would later throw because the local CCL compiler cannot evaluate scoped
     * conditions. The hybrid form sidesteps that by holding the predicate
     * separately.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Criteria} containing
     * {@code scope(prefix, inner)}.</li>
     * <li>Call {@link Scope#hybrid(Criteria, Predicate)}.{@code apply} with an
     * always-true predicate and a {@link Selection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A non-null {@link Selection} distinct from the
     * input is returned.
     */
    @Test
    public void testHybridAcceptsScopeBearingCriteria() {
        Criteria scoped = Criteria
                .where().scope("parent.children", Criteria.where()
                        .key("user.userId").operator(Operator.EQUALS).value(7L))
                .build();
        Selection<?> selection = Selection.of(TestRecord.class);
        Selection<?> result = Scope.hybrid(scoped, r -> true).apply(selection);
        Assert.assertNotNull(result);
        Assert.assertNotSame(selection, result);
    }

    /**
     * A minimal {@link Record} used as the type parameter for {@link Selection}
     * instances in these tests.
     */
    static class TestRecord extends Record {}

    /**
     * A minimal {@link Audience} implementation used to verify the
     * non-anonymous branch of {@link Scope#hybrid(Audience, Criteria)}'s
     * default predicate.
     */
    static class TestAudience extends Record implements Audience {}

    /**
     * A minimal {@link AccessControl} {@link Record} whose discoverability for
     * both authenticated and anonymous {@link Audience audiences} is set at
     * construction time. All other access methods deny.
     */
    static class TestAccessControlRecord extends Record implements
            AccessControl {

        /**
         * Whether this record is discoverable by a non-anonymous
         * {@link Audience}.
         */
        private final boolean discoverable;

        /**
         * Whether this record is discoverable by an anonymous {@link Audience}.
         */
        private final boolean discoverableAnonymous;

        /**
         * Construct a new {@link TestAccessControlRecord}.
         *
         * @param discoverable value returned by
         *            {@link #$isDiscoverableBy(Audience)}
         * @param discoverableAnonymous value returned by
         *            {@link #$isDiscoverableByAnonymous()}
         */
        TestAccessControlRecord(boolean discoverable,
                boolean discoverableAnonymous) {
            this.discoverable = discoverable;
            this.discoverableAnonymous = discoverableAnonymous;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return discoverable;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return discoverableAnonymous;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }
    }

}
