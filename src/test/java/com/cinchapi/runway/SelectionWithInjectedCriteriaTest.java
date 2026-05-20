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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link Selection#withInjectedCriteria(Selection, Criteria)}.
 *
 * @author Jeff Nelson
 */
public class SelectionWithInjectedCriteriaTest {

    /**
     * <strong>Goal:</strong> Verify that injecting visibility criteria into a
     * {@link FindSelection} produces a new {@link FindSelection} whose criteria
     * is the AND of the original and the visibility criteria.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link FindSelection} with a base criteria.</li>
     * <li>Call {@link Selection#withInjectedCriteria} with a visibility
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link FindSelection} whose
     * criteria is non-null and differs from the original (the two are
     * combined).
     */
    @Test
    public void testFindSelectionCriteriaIsAndedWithVisibility() {
        Criteria base = Criteria.where().key("active").operator(Operator.EQUALS)
                .value(true).build();
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).where(base)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertTrue(result instanceof FindSelection);
        FindSelection<TestRecord> find = (FindSelection<TestRecord>) result;
        Assert.assertNotSame(base, find.criteria);
        Assert.assertNotNull(find.criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting visibility criteria into a
     * {@link LoadClassSelection} (which has no criteria) converts it into a
     * {@link FindSelection} with the visibility criteria as its criteria.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadClassSelection} with no criteria.</li>
     * <li>Call {@link Selection#withInjectedCriteria} with a visibility
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link FindSelection} (not a
     * {@link LoadClassSelection}) whose criteria equals the visibility
     * criteria.
     */
    @Test
    public void testLoadClassSelectionBecomesFindSelection() {
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertFalse(result instanceof LoadClassSelection);
        Assert.assertTrue(result instanceof FindSelection);
        FindSelection<TestRecord> find = (FindSelection<TestRecord>) result;
        Assert.assertSame(visibility, find.criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting visibility criteria into a
     * {@link CountSelection} that already has criteria produces a new
     * {@link CountSelection} with AND-ed criteria.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link CountSelection} with a base criteria.</li>
     * <li>Call {@link Selection#withInjectedCriteria} with a visibility
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link CountSelection} with
     * non-null criteria that is not the original.
     */
    @Test
    public void testCountSelectionCriteriaIsAnded() {
        Criteria base = Criteria.where().key("active").operator(Operator.EQUALS)
                .value(true).build();
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).where(base)
                .count().build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertTrue(result instanceof CountSelection);
        CountSelection<TestRecord> count = (CountSelection<TestRecord>) result;
        Assert.assertNotSame(base, count.criteria);
        Assert.assertNotNull(count.criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting visibility criteria into a
     * {@link CountSelection} that has no criteria uses the visibility criteria
     * as the sole criteria.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link CountSelection} with no criteria.</li>
     * <li>Call {@link Selection#withInjectedCriteria} with a visibility
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link CountSelection} whose
     * criteria is exactly the visibility criteria.
     */
    @Test
    public void testCountSelectionWithNoCriteriaUsesVisibility() {
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).count()
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertTrue(result instanceof CountSelection);
        CountSelection<TestRecord> count = (CountSelection<TestRecord>) result;
        Assert.assertSame(visibility, count.criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting visibility criteria into a
     * {@link UniqueSelection} that already has criteria produces a new
     * {@link UniqueSelection} with AND-ed criteria.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link UniqueSelection} with a base criteria.</li>
     * <li>Call {@link Selection#withInjectedCriteria} with a visibility
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} with
     * non-null criteria that is not the original.
     */
    @Test
    public void testUniqueSelectionCriteriaIsAnded() {
        Criteria base = Criteria.where().key("active").operator(Operator.EQUALS)
                .value(true).build();
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.ofUnique(TestRecord.class)
                .where(base).build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertTrue(result instanceof UniqueSelection);
        UniqueSelection<TestRecord> unique = (UniqueSelection<TestRecord>) result;
        Assert.assertNotSame(base, unique.criteria);
        Assert.assertNotNull(unique.criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting visibility criteria into a
     * {@link UniqueSelection} that has no criteria uses the visibility criteria
     * as the sole criteria.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link UniqueSelection} with no criteria.</li>
     * <li>Call {@link Selection#withInjectedCriteria} with a visibility
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} whose
     * criteria is exactly the visibility criteria.
     */
    @Test
    public void testUniqueSelectionWithNoCriteriaUsesVisibility() {
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.ofUnique(TestRecord.class)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertTrue(result instanceof UniqueSelection);
        UniqueSelection<TestRecord> unique = (UniqueSelection<TestRecord>) result;
        Assert.assertSame(visibility, unique.criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting visibility criteria into a
     * {@link UniqueSelection} preserves the {@code any} flag.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link UniqueSelection} via {@code ofAnyUnique}.</li>
     * <li>Call {@link Selection#withInjectedCriteria} with a visibility
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} with
     * {@code any == true}.
     */
    @Test
    public void testUniqueSelectionPreservesAnyFlag() {
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.ofAnyUnique(TestRecord.class)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertTrue(result instanceof UniqueSelection);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertTrue(db.any);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting a scope-bearing visibility
     * {@link Criteria} into a {@link LoadRecordSelection} produces a
     * {@link UniqueSelection} (not a {@link LoadRecordSelection}) so that the
     * scoped condition is evaluated by the engine. The local CCL compiler
     * cannot honor same-destination semantics across a navigation prefix and
     * throws {@link UnsupportedOperationException} when asked to, so the
     * filter-based path that {@link LoadRecordSelection} otherwise takes is
     * unavailable for this {@link Criteria} shape.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via {@code id(...)}.</li>
     * <li>Build a {@link Criteria} containing a {@code scope(prefix, inner)}
     * clause.</li>
     * <li>Call {@link Selection#withInjectedCriteria}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} (not a
     * {@link LoadRecordSelection}); its criteria is non-null.
     */
    @Test
    public void testLoadRecordSelectionWithScopedCriteriaBecomesUniqueSelection() {
        Criteria scoped = Criteria
                .where().scope("parent.children", Criteria.where()
                        .key("user.userId").operator(Operator.EQUALS).value(7L))
                .build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).id(42L)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                scoped);
        Assert.assertFalse(result instanceof LoadRecordSelection);
        Assert.assertTrue(result instanceof UniqueSelection);
        UniqueSelection<TestRecord> unique = (UniqueSelection<TestRecord>) result;
        Assert.assertNotNull(unique.criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that injecting a non-scope-bearing
     * visibility {@link Criteria} into a {@link LoadRecordSelection} preserves
     * the existing local-filter path. The local CCL compiler can evaluate such
     * conditions correctly, so no conversion is needed and the single-record
     * fetch by id is retained.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via {@code id(...)}.</li>
     * <li>Build a simple key/value {@link Criteria} with no scoped
     * condition.</li>
     * <li>Call {@link Selection#withInjectedCriteria}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is still a
     * {@link LoadRecordSelection}; its filter has been composed with the
     * visibility predicate and is no longer the {@code NO_FILTER} sentinel.
     */
    @Test
    public void testLoadRecordSelectionWithSimpleCriteriaKeepsLocalFilter() {
        Criteria visibility = Criteria.where().key("owner")
                .operator(Operator.EQUALS).value(42L).build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).id(42L)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                visibility);
        Assert.assertTrue(result instanceof LoadRecordSelection);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertFalse(DatabaseSelection.isNoFilter(db.filter));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code scope(prefix, inner)} is
     * detected even when it is nested inside a {@code group(...)} clause that
     * is conjoined with another condition &mdash; the engine still needs to
     * evaluate the scoped sub-tree, so the {@link LoadRecordSelection} must be
     * promoted to a {@link UniqueSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via {@code id(...)}.</li>
     * <li>Build a {@link Criteria} of the form
     * {@code (scope(prefix, inner)) AND key = value}.</li>
     * <li>Call {@link Selection#withInjectedCriteria}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection},
     * because the scoped sub-tree was discovered during the recursive
     * inspection of the injected {@link Criteria}.
     */
    @Test
    public void testLoadRecordSelectionDetectsNestedScopedCriteria() {
        Criteria nested = Criteria.where()
                .group(Criteria.where().scope("parent.children",
                        Criteria.where().key("user.userId")
                                .operator(Operator.EQUALS).value(7L)))
                .and().key("active").operator(Operator.EQUALS).value(true)
                .build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).id(42L)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                nested);
        Assert.assertFalse(result instanceof LoadRecordSelection);
        Assert.assertTrue(result instanceof UniqueSelection);
    }

    /**
     * A simple {@link Record} subclass for testing.
     */
    static class TestRecord extends Record {} // empty — used only for type
                                              // checking

}
