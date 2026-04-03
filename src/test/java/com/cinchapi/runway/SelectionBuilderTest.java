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
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for the {@link Selection} fluent builder API, verifying correct
 * routing (subclass type returned from {@code build()}) and compile-time state
 * transitions.
 *
 * @author Jeff Nelson
 */
public class SelectionBuilderTest {

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.of(clazz).build()}
     * produces a {@link LoadClassSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with no criteria and no ID.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link LoadClassSelection}.
     */
    @Test
    public void testBuildWithNoCriteriaNoIdReturnsLoadClassSelection() {
        Selection<TestRecord> sel = Selection.of(TestRecord.class).build();
        Assert.assertTrue(sel instanceof LoadClassSelection);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(criteria).build()} produces a
     * {@link FindSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with criteria set.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link FindSelection}.
     */
    @Test
    public void testBuildWithCriteriaReturnsFindSelection() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("test").build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .where(criteria).build();
        Assert.assertTrue(sel instanceof FindSelection);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).criteria(criteria).build()} produces a
     * {@link FindSelection} (alias for {@code where}).
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} using {@code criteria()} instead of
     * {@code where()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link FindSelection}.
     */
    @Test
    public void testCriteriaAliasReturnsFindSelection() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("test").build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .criteria(criteria).build();
        Assert.assertTrue(sel instanceof FindSelection);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).id(n).build()} produces a
     * {@link LoadRecordSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with an ID set.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link LoadRecordSelection}.
     */
    @Test
    public void testBuildWithIdReturnsLoadRecordSelection() {
        Selection<TestRecord> sel = Selection.of(TestRecord.class).id(42)
                .build();
        Assert.assertTrue(sel instanceof LoadRecordSelection);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).count().build()} produces a
     * {@link CountSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} marked as counting.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link CountSelection}.
     */
    @Test
    public void testBuildWithCountReturnsCountSelection() {
        Selection<TestRecord> sel = Selection.of(TestRecord.class).count()
                .build();
        Assert.assertTrue(sel instanceof CountSelection);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(c).count().build()} produces a
     * {@link CountSelection} with criteria set.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with criteria and counting.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link CountSelection} and its
     * criteria field is non-null.
     */
    @Test
    public void testBuildWithCriteriaAndCountReturnsCountSelection() {
        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(21).build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .where(criteria).count().build();
        Assert.assertTrue(sel instanceof CountSelection);
        Assert.assertNotNull(((CountSelection<?>) sel).criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.ofAny(clazz).build()}
     * sets the {@code any} flag.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} via {@code ofAny()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resulting {@link Selection} has
     * {@code any == true}.
     */
    @Test
    public void testOfAnySetsFlagTrue() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.ofAny(TestRecord.class).build());
        Assert.assertTrue(sel.any);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).any().build()} sets the {@code any} flag.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} using the {@code any()} fluent method.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resulting {@link Selection} has
     * {@code any == true}.
     */
    @Test
    public void testAnyFluentMethodSetsFlagTrue() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).any().build());
        Assert.assertTrue(sel.any);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).any(false).build()} keeps the {@code any} flag
     * as {@code false}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} using {@code any(false)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resulting {@link Selection} has
     * {@code any == false}.
     */
    @Test
    public void testAnyBooleanFalseKeepsFlagFalse() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).any(false).build());
        Assert.assertFalse(sel.any);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.ofAny(clazz).where(c).build()} produces a
     * {@link FindSelection} with {@code any} set.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with criteria and descendant inclusion via
     * {@code ofAny()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link FindSelection} with
     * {@code any == true}.
     */
    @Test
    public void testOfAnyWithCriteriaReturnsFindSelectionWithAny() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("x").build();
        DatabaseSelection<?> sel = DatabaseSelection.resolve(
                Selection.ofAny(TestRecord.class).where(criteria).build());
        Assert.assertTrue(sel instanceof FindSelection);
        Assert.assertTrue(sel.any);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code order} is passed through to the
     * built {@link FindSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link FindSelection} with criteria and order.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link FindSelection} has the specified
     * order.
     */
    @Test
    public void testOrderPassedThroughToFindSelection() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("x").build();
        Order order = Order.by("name");
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .where(criteria).order(order).build();
        Assert.assertTrue(sel instanceof FindSelection);
        Assert.assertNotNull(((FindSelection<?>) sel).order);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code order} and {@code page} are
     * passed through to a {@link LoadClassSelection} when no criteria is set.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with order and page but no criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link LoadClassSelection}
     * with order and page set.
     */
    @Test
    public void testOrderAndPagePassedToLoadClassSelection() {
        Order order = Order.by("name");
        Page page = Page.sized(10);
        Selection<TestRecord> sel = Selection.of(TestRecord.class).order(order)
                .page(page).build();
        Assert.assertTrue(sel instanceof LoadClassSelection);
        LoadClassSelection<?> lc = (LoadClassSelection<?>) sel;
        Assert.assertNotNull(lc.order);
        Assert.assertNotNull(lc.page);
    }

    /**
     * <strong>Goal:</strong> Verify that a filter makes the {@link Selection}
     * non-combinable.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with a filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isCombinable()} returns {@code false}.
     */
    @Test
    public void testFilterMakesSelectionNonCombinable() {
        DatabaseSelection<?> sel = DatabaseSelection.resolve(
                Selection.of(TestRecord.class).filter(r -> true).build());
        Assert.assertFalse(sel.isCombinable());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link LoadClassSelection} with no
     * order, page, or filter is combinable.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a plain {@link LoadClassSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isCombinable()} returns {@code true}.
     */
    @Test
    public void testPlainLoadClassSelectionIsCombinable() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).build());
        Assert.assertTrue(sel.isCombinable());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link CountSelection} is never
     * combinable.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link CountSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isCombinable()} returns {@code false}.
     */
    @Test
    public void testCountSelectionIsNotCombinable() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).count().build());
        Assert.assertFalse(sel.isCombinable());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link LoadRecordSelection} is
     * always combinable.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadRecordSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isCombinable()} returns {@code true}.
     */
    @Test
    public void testLoadRecordSelectionIsCombinable() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).id(1).build());
        Assert.assertTrue(sel.isCombinable());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code isCounting()} returns
     * {@code true} for a {@link CountSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link CountSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isCounting()} returns {@code true}.
     */
    @Test
    public void testCountSelectionIsCounting() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).count().build());
        Assert.assertTrue(sel.isCounting());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code isCounting()} returns
     * {@code false} for a non-counting {@link Selection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadClassSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isCounting()} returns {@code false}.
     */
    @Test
    public void testNonCountSelectionIsNotCounting() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).build());
        Assert.assertFalse(sel.isCounting());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection} with no criteria
     * produces a {@link LoadClassSelection}, not a {@link FindSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with only order set (no criteria).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link LoadClassSelection},
     * confirming that absent criteria routes to load, not find.
     */
    @Test
    public void testNoCriteriaWithOrderReturnsLoadClassNotFind() {
        Order order = Order.by("name");
        Selection<TestRecord> sel = Selection.of(TestRecord.class).order(order)
                .build();
        Assert.assertTrue(sel instanceof LoadClassSelection);
        Assert.assertFalse(sel instanceof FindSelection);
    }

    /**
     * <strong>Goal:</strong> Verify that a filter can be combined with count.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with filter and count.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link CountSelection} with
     * the filter set.
     */
    @Test
    public void testFilterWithCountReturnsCountSelection() {
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .filter(r -> true).count().build();
        Assert.assertTrue(sel instanceof CountSelection);
        Assert.assertNotNull(((CountSelection<?>) sel).filter);
    }

    /**
     * <strong>Goal:</strong> Verify that the built {@link Selection} preserves
     * the realms set during building.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with a specific {@link Realms}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Selection} has the specified
     * realms.
     */
    @Test
    public void testRealmsPreservedInBuiltSelection() {
        Realms realms = Realms.only("test-realm");
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).realms(realms).build());
        Assert.assertEquals(realms, sel.realms);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).unique().build()} produces a
     * {@link UniqueSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} via
     * {@code Selection.of(clazz).unique()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection}.
     */
    @Test
    public void testBuildWithUniqueReturnsUniqueSelection() {
        Selection<TestRecord> sel = Selection.of(TestRecord.class).unique()
                .build();
        Assert.assertTrue(sel instanceof UniqueSelection);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(c).unique().build()} produces a
     * {@link UniqueSelection} with criteria set.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with criteria and {@code unique()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} and
     * its criteria field is non-null.
     */
    @Test
    public void testBuildWithCriteriaAndUniqueReturnsUniqueSelection() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("test").build();
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .where(criteria).unique().build();
        Assert.assertTrue(sel instanceof UniqueSelection);
        Assert.assertNotNull(((UniqueSelection<?>) sel).criteria);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.ofUnique(clazz).build()} produces a
     * {@link UniqueSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} via {@code Selection.ofUnique(clazz)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} with
     * {@code any == false}.
     */
    @Test
    public void testOfUniqueReturnsUniqueSelection() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.ofUnique(TestRecord.class).build());
        Assert.assertTrue(sel instanceof UniqueSelection);
        Assert.assertFalse(sel.any);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.ofAnyUnique(clazz).build()} produces a
     * {@link UniqueSelection} with {@code any == true}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} via
     * {@code Selection.ofAnyUnique(clazz)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is a {@link UniqueSelection} with
     * {@code any == true}.
     */
    @Test
    public void testOfAnyUniqueReturnsUniqueSelectionWithAny() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.ofAnyUnique(TestRecord.class).build());
        Assert.assertTrue(sel instanceof UniqueSelection);
        Assert.assertTrue(sel.any);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} is never
     * combinable.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link UniqueSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isCombinable()} returns {@code false}.
     */
    @Test
    public void testUniqueSelectionIsNotCombinable() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).unique().build());
        Assert.assertFalse(sel.isCombinable());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code isUnique()} returns
     * {@code true} for a {@link UniqueSelection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link UniqueSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isUnique()} returns {@code true}.
     */
    @Test
    public void testUniqueSelectionIsUnique() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).unique().build());
        Assert.assertTrue(sel.isUnique());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code isUnique()} returns
     * {@code false} for a non-unique {@link Selection}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link LoadClassSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isUnique()} returns {@code false}.
     */
    @Test
    public void testNonUniqueSelectionIsNotUnique() {
        DatabaseSelection<?> sel = DatabaseSelection
                .resolve(Selection.of(TestRecord.class).build());
        Assert.assertFalse(sel.isUnique());
    }

    /**
     * A simple {@link Record} subclass for testing.
     */
    static class TestRecord extends Record {
        // empty — used only for type checking
    }

}
