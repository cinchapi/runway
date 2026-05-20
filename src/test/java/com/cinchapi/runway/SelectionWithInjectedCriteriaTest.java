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
     * {@link UniqueSelection} (not a {@link LoadRecordSelection}) whose
     * criteria is a freshly constructed {@code $id$ = id AND injected} that
     * still carries the scoped sub-tree, so the engine evaluates the scoped
     * condition. The local CCL compiler cannot honor same-destination semantics
     * across a navigation prefix and throws
     * {@link UnsupportedOperationException} when asked to, so the filter-based
     * path that {@link LoadRecordSelection} otherwise takes is unavailable for
     * this {@link Criteria} shape.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via {@code id(42L)}.</li>
     * <li>Build a {@link Criteria} containing a {@code scope(prefix, inner)}
     * clause.</li>
     * <li>Call {@link Selection#withInjectedCriteria}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} whose
     * criteria is a new instance distinct from the injected one, whose CCL
     * anchors on {@link Record#IDENTIFIER_KEY} bound to {@code 42}, and which
     * still reports as {@link DatabaseSelection#isScopeBearing} so the engine
     * sees the scoped sub-tree.
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
        Assert.assertNotSame(scoped, unique.criteria);
        String ccl = unique.criteria.ccl();
        Assert.assertTrue(ccl.contains(Record.IDENTIFIER_KEY));
        Assert.assertTrue(ccl.contains("42"));
        Assert.assertTrue(DatabaseSelection.isScopeBearing(unique.criteria));
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
     * promoted to a {@link UniqueSelection} whose criteria is a fresh
     * {@code $id$ = id AND injected} that still carries the nested scope.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via {@code id(42L)}.</li>
     * <li>Build a {@link Criteria} of the form
     * {@code (scope(prefix, inner)) AND key = value}.</li>
     * <li>Call {@link Selection#withInjectedCriteria}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection}, its
     * criteria is a new instance distinct from the injected one, the CCL
     * anchors on {@link Record#IDENTIFIER_KEY} bound to {@code 42}, and
     * {@link DatabaseSelection#isScopeBearing} still reports {@code true} so
     * the recursive inspection caught the nested scope.
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
        UniqueSelection<TestRecord> unique = (UniqueSelection<TestRecord>) result;
        Assert.assertNotSame(nested, unique.criteria);
        String ccl = unique.criteria.ccl();
        Assert.assertTrue(ccl.contains(Record.IDENTIFIER_KEY));
        Assert.assertTrue(ccl.contains("42"));
        Assert.assertTrue(DatabaseSelection.isScopeBearing(unique.criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that promoting a
     * {@link LoadRecordSelection} to a {@link UniqueSelection} on the
     * scope-bearing path preserves the {@code any} flag, the {@link Realms}
     * filter, and any client-side {@link Predicate} that was already attached
     * to the load. The new {@link UniqueSelection} carries the engine-side
     * criteria, but the orthogonal post-selection state (realms scoping,
     * hierarchy inclusion, local predicate) must survive intact.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via
     * {@code ofAny(...).id(42L).realms(...)}.</li>
     * <li>Wrap with {@link Selection#withInjectedFilter} to attach a
     * client-side predicate (the {@code id(...)} builder does not expose
     * {@code filter(...)} directly).</li>
     * <li>Apply a scope-bearing visibility {@link Criteria} via
     * {@link Selection#withInjectedCriteria}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} with
     * {@code any == true}, the same {@link Realms} instance, and a filter that
     * is no longer the {@code NO_FILTER} sentinel (proving the original
     * predicate survived the conversion).
     */
    @Test
    public void testLoadRecordSelectionWithScopedCriteriaPreservesFilterRealmsAndAny() {
        Criteria scoped = Criteria
                .where().scope("parent.children", Criteria.where()
                        .key("user.userId").operator(Operator.EQUALS).value(7L))
                .build();
        Realms realms = Realms.only("test-realm");
        Predicate<TestRecord> filter = r -> true;
        Selection<TestRecord> sel = Selection.ofAny(TestRecord.class).id(42L)
                .realms(realms).build();
        sel = Selection.withInjectedFilter(sel, filter);
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                scoped);
        Assert.assertTrue(result instanceof UniqueSelection);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertTrue(db.any);
        Assert.assertSame(realms, db.realms);
        Assert.assertFalse(DatabaseSelection.isNoFilter(db.filter));
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link UniqueSelection} produced
     * by promoting a {@link LoadRecordSelection} on the scope-bearing path
     * carries a {@link Criteria} that
     * {@link Record#isDatabaseResolvableCondition} accepts. Without this
     * guarantee, {@code $selectCriteria} falls back to
     * {@code filter()}/{@code filterAny()}, which calls
     * {@link Record#matches(Criteria)} on the scope-bearing criteria and throws
     * {@link UnsupportedOperationException} at runtime.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via {@code id(42L)}.</li>
     * <li>Build a scope-bearing {@link Criteria}.</li>
     * <li>Call {@link Selection#withInjectedCriteria}.</li>
     * <li>Test the promoted {@link UniqueSelection}'s criteria against
     * {@link Record#isDatabaseResolvableCondition}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@link Record#isDatabaseResolvableCondition}
     * returns {@code true}.
     */
    @Test
    public void testLoadRecordSelectionWithScopedCriteriaPromotedCriteriaIsDatabaseResolvable() {
        Criteria scoped = Criteria
                .where().scope("parent.children", Criteria.where()
                        .key("user.userId").operator(Operator.EQUALS).value(7L))
                .build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).id(42L)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                scoped);
        UniqueSelection<TestRecord> unique = (UniqueSelection<TestRecord>) result;
        Assert.assertTrue(Record.isDatabaseResolvableCondition(TestRecord.class,
                unique.criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that promoting a
     * {@link LoadRecordSelection} built from {@code Selection.of(...)} (i.e.,
     * {@code any=false}) on the scope-bearing path produces a
     * {@link UniqueSelection} with {@code any=true}. The promoted selection
     * runs through {@code $selectCriteria}, which wraps with {@code forClass}
     * when {@code any=false} and excludes records stored under a subclass
     * section; setting {@code any=true} keeps subclass-by-id resolution
     * working.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection} via
     * {@code Selection.of(...).id(42L)} so the input {@code any} is
     * {@code false}.</li>
     * <li>Apply a scope-bearing visibility {@link Criteria} via
     * {@link Selection#withInjectedCriteria}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} with
     * {@code any=true}.
     */
    @Test
    public void testLoadRecordSelectionWithScopedCriteriaPromotionForcesAnyTrue() {
        Criteria scoped = Criteria
                .where().scope("parent.children", Criteria.where()
                        .key("user.userId").operator(Operator.EQUALS).value(7L))
                .build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class).id(42L)
                .build();
        Selection<TestRecord> result = Selection.withInjectedCriteria(sel,
                scoped);
        Assert.assertTrue(result instanceof UniqueSelection);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertTrue(db.any);
    }

    /**
     * A simple {@link Record} subclass for testing.
     */
    static class TestRecord extends Record {}

}
