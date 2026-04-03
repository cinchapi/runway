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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.runway.access.AccessControl;
import com.cinchapi.runway.access.Audience;

/**
 * Unit tests for {@link Selection}, {@link Selections}, and
 * {@link Runway#select(Selection...)}.
 *
 * @author Jeff Nelson
 */
public class RunwaySelectAndReserveTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a single by-ID {@link Selection} loads
     * the correct {@link Record}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget} in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Widget}.</li>
     * <li>Create a {@link Selection} for that {@link Widget Widget's} ID.</li>
     * <li>Execute via {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains the saved {@link Widget}
     * with the correct name.
     */
    @Test
    public void testSelectByIdReturnsSingleRecord() {
        Widget widget = new Widget("alpha");
        widget.save();
        Selection<Widget> sel = Selection.of(Widget.class).id(widget.id())
                .build();
        runway.select(sel);
        Widget loaded = sel.get();
        Assert.assertNotNull(loaded);
        Assert.assertEquals("alpha", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that a load-all {@link Selection} returns
     * all {@link Record Records} of the target class.
     * <p>
     * <strong>Start state:</strong> Multiple saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save three {@link Widget Widgets}.</li>
     * <li>Create a load-all {@link Selection} for {@link Widget}.</li>
     * <li>Execute via {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains all three {@link Widget
     * Widgets}.
     */
    @Test
    public void testSelectLoadAllReturnsAllRecords() {
        new Widget("a").save();
        new Widget("b").save();
        new Widget("c").save();
        Selection<Widget> sel = Selection.of(Widget.class).build();
        runway.select(sel);
        Set<Widget> widgets = sel.get();
        Assert.assertEquals(3, widgets.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a criteria-based {@link Selection}
     * returns only matching {@link Record Records}.
     * <p>
     * <strong>Start state:</strong> Multiple saved {@link Widget Widgets} with
     * different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} with varying scores.</li>
     * <li>Create a criteria-based {@link Selection} filtering by score &gt;
     * 50.</li>
     * <li>Execute via {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only {@link Widget Widgets} with score &gt; 50
     * are returned.
     */
    @Test
    public void testSelectByCriteriaReturnsFilteredRecords() {
        new Widget("low", 10).save();
        new Widget("mid", 50).save();
        new Widget("high", 80).save();
        new Widget("top", 100).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .build();
        runway.select(sel);
        Set<Widget> results = sel.get();
        Assert.assertEquals(2, results.size());
        for (Widget w : results) {
            Assert.assertTrue(w.score > 50);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that multiple heterogeneous
     * {@link Selection Selections} can be executed in one batch.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} and
     * {@link Gadget Gadgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} and {@link Gadget
     * Gadgets}.</li>
     * <li>Create selections for both types.</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}
     * call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each {@link Selection} contains only
     * {@link Record Records} of its target class.
     */
    @Test
    public void testMultiSelectMixedTypes() {
        new Widget("w1").save();
        new Widget("w2").save();
        new Gadget("g1", "red").save();
        new Gadget("g2", "blue").save();
        new Gadget("g3", "green").save();
        Selection<Widget> widgetSel = Selection.of(Widget.class).build();
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class).build();
        Selections results = runway.select(widgetSel, gadgetSel);
        Set<Widget> widgets = widgetSel.get();
        Set<Gadget> gadgets = gadgetSel.get();
        Assert.assertEquals(2, widgets.size());
        Assert.assertEquals(3, gadgets.size());
        Assert.assertEquals(2, results.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a multi-select batch can combine by-ID
     * and criteria-based {@link Selection Selections}.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} and a specific
     * {@link Gadget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} and a {@link Gadget}.</li>
     * <li>Create a criteria-based {@link Selection} for {@link Widget} and an
     * ID-based {@link Selection} for the {@link Gadget}.</li>
     * <li>Execute both in one call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The criteria selection returns matching
     * {@link Widget Widgets} and the ID selection returns the specific
     * {@link Gadget}.
     */
    @Test
    public void testMultiSelectMixedIdAndCriteria() {
        new Widget("low", 10).save();
        new Widget("high", 90).save();
        Gadget g = new Gadget("target", "red");
        g.save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> widgetSel = Selection.of(Widget.class)
                .criteria(criteria).build();
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class).id(g.id())
                .build();
        runway.select(widgetSel, gadgetSel);
        Set<Widget> widgets = widgetSel.get();
        Gadget loaded = gadgetSel.get();
        Assert.assertEquals(1, widgets.size());
        Assert.assertEquals("high", widgets.iterator().next().name);
        Assert.assertNotNull(loaded);
        Assert.assertEquals("target", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that positional access via
     * {@link Selections#get(int)} works correctly.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} and
     * {@link Gadget Gadgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save records of both types.</li>
     * <li>Execute a multi-select.</li>
     * <li>Access results by index.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Positional access returns the same results as
     * {@link Selection#get()}.
     */
    @Test
    public void testPositionalAccess() {
        new Widget("w1").save();
        Gadget g = new Gadget("g1", "red");
        g.save();
        Selection<Widget> widgetSel = Selection.of(Widget.class).build();
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class).id(g.id())
                .build();
        Selections results = runway.select(widgetSel, gadgetSel);
        Set<Widget> widgets = results.get(0);
        Gadget gadget = results.get(1);
        Assert.assertEquals(1, widgets.size());
        Assert.assertNotNull(gadget);
        Assert.assertEquals("g1", gadget.name);
    }

    /**
     * <strong>Goal:</strong> Verify that calling {@link Selection#get()} before
     * execution throws {@link IllegalStateException}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Selection} without executing it.</li>
     * <li>Call {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetBeforeExecutionThrows() {
        Selection<Widget> sel = Selection.of(Widget.class).build();
        sel.get();
    }

    /**
     * <strong>Goal:</strong> Verify that submitting a {@link Selection} twice
     * throws {@link IllegalStateException}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Widget}.</li>
     * <li>Create a {@link Selection} and submit it.</li>
     * <li>Submit it again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second submission throws
     * {@link IllegalStateException}.
     */
    @Test(expected = IllegalStateException.class)
    public void testDoubleSubmitThrows() {
        Widget w = new Widget("dupe");
        w.save();
        Selection<Widget> sel = Selection.of(Widget.class).id(w.id()).build();
        runway.select(sel);
        runway.select(sel);
    }

    /**
     * <strong>Goal:</strong> Verify that an ID-based {@link Selection} for a
     * nonexistent record returns {@code null}.
     * <p>
     * <strong>Start state:</strong> No records in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create an ID-based {@link Selection} with a random ID.</li>
     * <li>Execute it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code get()} returns {@code null}.
     */
    @Test
    public void testSelectByIdMissingReturnsNull() {
        Selection<Widget> sel = Selection.of(Widget.class).id(Random.getLong())
                .build();
        runway.select(sel);
        Widget loaded = sel.get();
        Assert.assertNull(loaded);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Selection#ofAny(Class)}
     * includes descendants.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player} and {@link PointGuard}
     * instances.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Player} and a {@link PointGuard}.</li>
     * <li>Create an {@code ofAny} {@link Selection} for {@link Player}.</li>
     * <li>Execute it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result includes both the {@link Player}
     * and the {@link PointGuard}.
     */
    @Test
    public void testOfAnyIncludesDescendants() {
        new Player("guard", 30).save();
        new PointGuard("pg", 25, 10).save();
        Selection<Player> sel = Selection.ofAny(Player.class).build();
        runway.select(sel);
        Set<Player> players = sel.get();
        Assert.assertEquals(2, players.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a criteria-based {@link Selection}
     * with no matches returns an empty set.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} that do not
     * match the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} with low scores.</li>
     * <li>Create a criteria-based {@link Selection} for scores &gt; 1000.</li>
     * <li>Execute it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is an empty set.
     */
    @Test
    public void testSelectByCriteriaNoMatchReturnsEmpty() {
        new Widget("low", 5).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(1000).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .build();
        runway.select(sel);
        Set<Widget> results = sel.get();
        Assert.assertTrue(results.isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that after
     * {@link Runway#select(Selection...)}, a subsequent {@link Runway#find}
     * with the same parameters returns the reserved result without hitting the
     * database again.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} with varying
     * scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets}.</li>
     * <li>Create a criteria-based {@link Selection} and execute it via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Call {@link Runway#find} with the same parameters.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result from {@code find()} is the exact
     * same object reference as the {@link Selection Selection's} result,
     * proving it came from the reserve.
     */
    @Test
    public void testReserveHitOnFindAfterSelect() {
        new Widget("low", 10).save();
        new Widget("high", 80).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .build();
        runway.reserve();
        runway.select(sel);
        Set<Widget> fromSelect = sel.get();
        Set<Widget> fromFind = runway.find(Widget.class, criteria);
        Assert.assertSame(fromSelect, fromFind);
    }

    /**
     * <strong>Goal:</strong> Verify that after
     * {@link Runway#select(Selection...)}, a subsequent
     * {@link Runway#load(Class, long)} with the same ID returns the reserved
     * result.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Widget}.</li>
     * <li>Create an ID-based {@link Selection} and execute it via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Call {@link Runway#load(Class, long)} with the same ID.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result from {@code load()} is the exact
     * same object reference as the {@link Selection Selection's} result.
     */
    @Test
    public void testReserveHitOnLoadAfterSelect() {
        Widget w = new Widget("reserved");
        w.save();
        Selection<Widget> sel = Selection.of(Widget.class).id(w.id()).build();
        runway.reserve();
        runway.select(sel);
        Widget fromSelect = sel.get();
        Widget fromLoad = runway.load(Widget.class, w.id());
        Assert.assertSame(fromSelect, fromLoad);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#unreserve()} causes
     * subsequent {@code find()} calls to go to the database instead of
     * returning the reserved result.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} with a
     * pre-populated reserve.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} to populate the reserve.</li>
     * <li>Call {@link Runway#unreserve()}.</li>
     * <li>Call {@link Runway#find} with the same parameters.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> After clearing the reserve, {@code find()}
     * returns a different object reference (fresh from the database), though
     * with equivalent content.
     */
    @Test
    public void testClearReserveCausesDbHit() {
        new Widget("w1", 10).save();
        new Widget("w2", 80).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .build();
        runway.reserve();
        runway.select(sel);
        Set<Widget> fromSelect = sel.get();
        runway.unreserve();
        Set<Widget> fromFind = runway.find(Widget.class, criteria);
        Assert.assertNotSame(fromSelect, fromFind);
        Assert.assertEquals(fromSelect.size(), fromFind.size());
    }

    /**
     * <strong>Goal:</strong> Verify that after
     * {@link Runway#select(Selection...)}, a subsequent
     * {@link Runway#load(Class)} with the same parameters returns the reserved
     * result.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets}.</li>
     * <li>Create a load-all {@link Selection} and execute it via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Call {@link Runway#load(Class)} with the same parameters.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result from {@code load()} is the exact
     * same object reference as the {@link Selection Selection's} result.
     */
    @Test
    public void testReserveHitOnLoadAllAfterSelect() {
        new Widget("a").save();
        new Widget("b").save();
        Selection<Widget> sel = Selection.of(Widget.class).build();
        runway.reserve();
        runway.select(sel);
        Set<Widget> fromSelect = sel.get();
        Set<Widget> fromLoad = runway.load(Widget.class);
        Assert.assertSame(fromSelect, fromLoad);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Selections#next()} returns
     * results in submission order.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} and
     * {@link Gadget Gadgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save records of both types.</li>
     * <li>Execute a multi-select.</li>
     * <li>Call {@link Selections#next()} twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> First call returns {@link Widget Widgets},
     * second returns {@link Gadget Gadgets}.
     */
    @Test
    public void testNextReturnsResultsInOrder() {
        new Widget("w1").save();
        new Gadget("g1", "red").save();
        Selection<Widget> widgetSel = Selection.of(Widget.class).build();
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class).build();
        Selections results = runway.select(widgetSel, gadgetSel);
        Set<Widget> widgets = results.next();
        Set<Gadget> gadgets = results.next();
        Assert.assertEquals(1, widgets.size());
        Assert.assertEquals(1, gadgets.size());
    }

    /**
     * <strong>Goal:</strong> Verify that two combinable {@link Selection
     * Selections} targeting the same class with different criteria each receive
     * only their matching {@link Record Records}, not the union.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} with varying
     * scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} with scores 10, 50, 80, and
     * 100.</li>
     * <li>Create two criteria-based {@link Selection Selections} for
     * {@link Widget}: one for scores &gt; 50, another for scores &lt; 20.</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}
     * call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first {@link Selection} contains only
     * {@link Widget Widgets} with scores &gt; 50 and the second contains only
     * {@link Widget Widgets} with scores &lt; 20.
     */
    @Test
    public void testSameClassDifferentCriteriaNotMerged() {
        new Widget("low", 10).save();
        new Widget("mid", 50).save();
        new Widget("high", 80).save();
        new Widget("top", 100).save();
        Criteria highScores = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Criteria lowScores = Criteria.where().key("score")
                .operator(Operator.LESS_THAN).value(20).build();
        Selection<Widget> highSel = Selection.of(Widget.class)
                .criteria(highScores).build();
        Selection<Widget> lowSel = Selection.of(Widget.class)
                .criteria(lowScores).build();
        runway.select(highSel, lowSel);
        Set<Widget> highResults = highSel.get();
        Set<Widget> lowResults = lowSel.get();
        Assert.assertEquals(2, highResults.size());
        for (Widget w : highResults) {
            Assert.assertTrue(w.score > 50);
        }
        Assert.assertEquals(1, lowResults.size());
        for (Widget w : lowResults) {
            Assert.assertTrue(w.score < 20);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that calling {@link Selections#next()}
     * after all results have been consumed throws
     * {@link IllegalStateException}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Widget}.</li>
     * <li>Execute two {@link Selection Selections}.</li>
     * <li>Call {@link Selections#next()} twice to consume both results.</li>
     * <li>Call {@link Selections#next()} a third time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The third call throws
     * {@link IllegalStateException}.
     */
    @Test(expected = IllegalStateException.class)
    public void testNextThrowsWhenExhausted() {
        new Widget("w1").save();
        Selection<Widget> sel1 = Selection.of(Widget.class).build();
        Selection<Widget> sel2 = Selection.of(Widget.class).build();
        Selections results = runway.select(sel1, sel2);
        results.next();
        results.next();
        results.next();
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection} with an
     * {@link Order} is correctly treated as non-combinable and executed in
     * isolation.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} and
     * {@link Gadget Gadgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} and {@link Gadget
     * Gadgets}.</li>
     * <li>Create a {@link Widget} {@link Selection} with an {@link Order}.</li>
     * <li>Create a combinable {@link Gadget} {@link Selection}.</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}
     * call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both {@link Selection Selections} return
     * correct results; the ordered one is isolated without affecting the
     * combinable one.
     */
    @Test
    public void testNonCombinableSelectionExecutedInIsolation() {
        new Widget("beta", 20).save();
        new Widget("alpha", 10).save();
        new Gadget("g1", "red").save();
        Selection<Widget> widgetSel = Selection.of(Widget.class)
                .order(Order.by("name")).build();
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class).build();
        Selections results = runway.select(widgetSel, gadgetSel);
        Set<Widget> widgets = widgetSel.get();
        Set<Gadget> gadgets = gadgetSel.get();
        Assert.assertEquals(2, widgets.size());
        Assert.assertEquals(1, gadgets.size());
        Assert.assertEquals(2, results.size());
        java.util.Iterator<Widget> it = widgets.iterator();
        Assert.assertEquals("alpha", it.next().name);
        Assert.assertEquals("beta", it.next().name);
    }

    /**
     * <strong>Goal:</strong> Verify that two by-ID {@link Selection Selections}
     * targeting the same class are correctly combined and each returns only its
     * specific {@link Record}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save two {@link Widget Widgets}.</li>
     * <li>Create two by-ID {@link Selection Selections}, one for each
     * {@link Widget Widget's} ID.</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}
     * call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each {@link Selection} returns the correct
     * {@link Widget} by name, proving the by-ID {@code demux} path
     * distinguishes them even though they share a class.
     */
    @Test
    public void testSameClassByIdSelectionsReturnCorrectRecords() {
        Widget w1 = new Widget("first");
        w1.save();
        Widget w2 = new Widget("second");
        w2.save();
        Selection<Widget> sel1 = Selection.of(Widget.class).id(w1.id()).build();
        Selection<Widget> sel2 = Selection.of(Widget.class).id(w2.id()).build();
        runway.select(sel1, sel2);
        Widget loaded1 = sel1.get();
        Widget loaded2 = sel2.get();
        Assert.assertNotNull(loaded1);
        Assert.assertNotNull(loaded2);
        Assert.assertEquals("first", loaded1.name);
        Assert.assertEquals("second", loaded2.name);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection} returns the
     * correct count of all {@link Record Records} of the target class.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save three {@link Widget Widgets}.</li>
     * <li>Create a {@link Selection} for {@link Widget}.</li>
     * <li>Execute via {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code get()} returns {@code 3}.
     */
    @Test
    public void testCountSelectionReturnsCorrectCount() {
        new Widget("a").save();
        new Widget("b").save();
        new Widget("c").save();
        Selection<Widget> sel = Selection.of(Widget.class).count().build();
        runway.select(sel);
        int count = sel.get();
        Assert.assertEquals(3, count);
    }

    /**
     * <strong>Goal:</strong> Verify that a criteria-based {@link Selection}
     * returns only the count of matching {@link Record Records}.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} with varying
     * scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} with scores 10, 50, 80, and
     * 100.</li>
     * <li>Create a {@link Selection} with criteria filtering score &gt;
     * 50.</li>
     * <li>Execute via {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code get()} returns {@code 2}.
     */
    @Test
    public void testCountSelectionWithCriteriaReturnsCorrectCount() {
        new Widget("low", 10).save();
        new Widget("mid", 50).save();
        new Widget("high", 80).save();
        new Widget("top", 100).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .count().build();
        runway.select(sel);
        int count = sel.get();
        Assert.assertEquals(2, count);
    }

    /**
     * <strong>Goal:</strong> Verify that executing a {@link Selection} reserves
     * the count so that a subsequent {@link Runway#count} call with the same
     * parameters returns the cached value.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} with varying
     * scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} with varying scores.</li>
     * <li>Create a {@link Selection} with criteria filtering score &gt; 50 and
     * execute it.</li>
     * <li>Call {@link Runway#count(Class, Criteria)} with the same
     * parameters.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result from {@code count()} matches the
     * {@link Selection Selection's} result.
     */
    @Test
    public void testCountSelectionReservedForSubsequentCountMethod() {
        new Widget("low", 10).save();
        new Widget("high", 80).save();
        new Widget("top", 100).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .count().build();
        runway.reserve();
        runway.select(sel);
        int fromSelect = sel.get();
        int fromCount = runway.count(Widget.class, criteria);
        Assert.assertEquals(fromSelect, fromCount);
    }

    /**
     * <strong>Goal:</strong> Verify that executing a {@link Selection} reserves
     * the result so that a subsequent {@link Runway#count} call with the same
     * parameters returns the size of the cached find result.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} with varying
     * scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} with varying scores.</li>
     * <li>Create a {@link Selection} with criteria filtering score &gt; 50 and
     * execute it.</li>
     * <li>Call {@link Runway#count(Class, Criteria)} with the same
     * parameters.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count equals the size of the
     * {@link Selection Selection's} result.
     */
    @Test
    public void testFindSelectionReservedForSubsequentCountMethod() {
        new Widget("low", 10).save();
        new Widget("high", 80).save();
        new Widget("top", 100).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .build();
        runway.reserve();
        runway.select(sel);
        Set<Widget> fromSelect = sel.get();
        int fromCount = runway.count(Widget.class, criteria);
        Assert.assertEquals(fromSelect.size(), fromCount);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Runway#count} call backed by
     * a reserved {@link Selection} does not consume the reservation, so a
     * subsequent {@link Runway#find} still returns the same cached object.
     * <p>
     * <strong>Start state:</strong> Saved {@link Widget Widgets} with varying
     * scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Widget Widgets} with varying scores.</li>
     * <li>Create a {@link Selection} with criteria filtering score &gt; 50 and
     * execute it.</li>
     * <li>Call {@link Runway#count(Class, Criteria)} with the same
     * parameters.</li>
     * <li>Call {@link Runway#find(Class, Criteria)} with the same
     * parameters.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result from {@code find()} is the exact
     * same object reference as the {@link Selection Selection's} result,
     * proving the count did not consume the reservation.
     */
    @Test
    public void testCountSelectionDoesNotConsumeReservation() {
        new Widget("low", 10).save();
        new Widget("high", 80).save();
        new Widget("top", 100).save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<Widget> sel = Selection.of(Widget.class).criteria(criteria)
                .build();
        runway.reserve();
        runway.select(sel);
        Set<Widget> fromSelect = sel.get();
        runway.count(Widget.class, criteria);
        Set<Widget> fromFind = runway.find(Widget.class, criteria);
        Assert.assertSame(fromSelect, fromFind);
    }

    /**
     * <strong>Goal:</strong> Verify that a second
     * {@link Runway#select(Selection...)} with a {@link CountSelection} hits
     * the reservation cache populated by the first {@code select()} call,
     * rather than re-querying the database.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save three {@link Widget Widgets}.</li>
     * <li>Execute a count {@link Selection} via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Save a fourth {@link Widget} to mutate the database.</li>
     * <li>Execute a second count {@link Selection} with the same
     * parameters.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second {@link Selection} returns the
     * cached count ({@code 3}) from the first {@code select()} call, not the
     * fresh database count ({@code 4}).
     */
    @Test
    public void testCountSelectionCacheHitOnSecondSelect() {
        new Widget("a").save();
        new Widget("b").save();
        new Widget("c").save();
        Selection<Widget> sel1 = Selection.of(Widget.class).count().build();
        runway.reserve();
        runway.select(sel1);
        int fromFirst = sel1.get();
        Assert.assertEquals(3, fromFirst);
        new Widget("d").save();
        Selection<Widget> sel2 = Selection.of(Widget.class).count().build();
        runway.select(sel2);
        int fromSecond = sel2.get();
        Assert.assertEquals(fromFirst, fromSecond);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience Audience's}
     * {@code load()} call uses the cached result from a
     * {@link Runway#select(Selection...)} pre-fetch, and that the
     * {@link AccessControl} visibility filter is correctly applied on top of
     * the cached data. This simulates the middleware/handler pattern where the
     * middleware pre-fetches via the {@link Runway} instance and the route
     * handler reads through an {@link Audience} {@link Record}.
     * <p>
     * <strong>Start state:</strong> Three saved {@link SecureWidget
     * SecureWidgets} (two public, one classified) and a saved {@link Actor}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save three {@link SecureWidget SecureWidgets}: two with
     * {@code classified = false}, one with {@code classified = true}.</li>
     * <li>Create and save an {@link Actor}.</li>
     * <li>Middleware: execute a load-all {@link Selection} via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Handler: call {@code actor.load(SecureWidget.class)} through the
     * {@link Audience} interface.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Audience} {@code load()} returns
     * only the 2 non-classified {@link SecureWidget SecureWidgets}. Each
     * returned {@link Record} is the same object reference as the corresponding
     * {@link Record} in the pre-fetched result, proving the cached data was
     * reused.
     */
    @Test
    public void testAudienceLoadAppliesAccessControlOverCachedResult() {
        new SecureWidget("alpha", false).save();
        new SecureWidget("beta", false).save();
        new SecureWidget("secret", true).save();
        Actor actor = new Actor("user");
        actor.save();
        Selection<SecureWidget> sel = Selection.of(SecureWidget.class).build();
        runway.reserve();
        runway.select(sel);
        Set<SecureWidget> fromSelect = sel.get();
        Assert.assertEquals(3, fromSelect.size());
        Set<SecureWidget> fromAudience = actor.load(SecureWidget.class);
        Assert.assertEquals(2, fromAudience.size());
        for (SecureWidget w : fromAudience) {
            Assert.assertFalse(w.classified);
            Assert.assertTrue(fromSelect.stream().anyMatch(s -> s == w));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience Audience's}
     * {@code find()} call uses the cached result from a
     * {@link Runway#select(Selection...)} pre-fetch, and that the
     * {@link AccessControl} visibility filter is correctly applied on top of
     * the cached data.
     * <p>
     * <strong>Start state:</strong> {@link SecureWidget SecureWidgets} with
     * varying scores and visibility, and a saved {@link Actor}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link SecureWidget SecureWidgets}: some matching the
     * criteria with different visibility.</li>
     * <li>Create and save an {@link Actor}.</li>
     * <li>Middleware: execute a criteria-based {@link Selection} via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Handler: call {@code actor.find(SecureWidget.class, criteria)}
     * through the {@link Audience} interface.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Audience} {@code find()} returns
     * only the visible matching {@link SecureWidget SecureWidgets}. Each
     * returned {@link Record} is the same object reference as the corresponding
     * {@link Record} in the pre-fetched result.
     */
    @Test
    public void testAudienceFindAppliesAccessControlOverCachedResult() {
        new SecureWidget("low", 10, false).save();
        new SecureWidget("high", 80, false).save();
        new SecureWidget("secret-high", 90, true).save();
        Actor actor = new Actor("user");
        actor.save();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();
        Selection<SecureWidget> sel = Selection.of(SecureWidget.class)
                .criteria(criteria).build();
        runway.reserve();
        runway.select(sel);
        Set<SecureWidget> fromSelect = sel.get();
        Assert.assertEquals(2, fromSelect.size());
        Set<SecureWidget> fromAudience = actor.find(SecureWidget.class,
                criteria);
        Assert.assertEquals(1, fromAudience.size());
        for (SecureWidget w : fromAudience) {
            Assert.assertFalse(w.classified);
            Assert.assertTrue(fromSelect.stream().anyMatch(s -> s == w));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that passing duplicate {@link Selection
     * Selections} (identical class and criteria) to
     * {@link Runway#select(Selection...)} returns correct results for both
     * without redundant database queries.
     * <p>
     * <strong>Start state:</strong> Multiple saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save three {@link Widget Widgets}.</li>
     * <li>Build two identical load-all {@link Selection Selections} for
     * {@link Widget}.</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}
     * call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both {@link Selection Selections} return the
     * same three {@link Widget Widgets}.
     */
    @Test
    public void testDuplicateSelectionsReturnSameResults() {
        new Widget("a").save();
        new Widget("b").save();
        new Widget("c").save();
        Selection<Widget> sel1 = Selection.of(Widget.class).build();
        Selection<Widget> sel2 = Selection.of(Widget.class).build();
        runway.select(sel1, sel2);
        Set<Widget> result1 = sel1.get();
        Set<Widget> result2 = sel2.get();
        Assert.assertEquals(3, result1.size());
        Assert.assertEquals(result1, result2);
        Assert.assertSame(result1, result2);
    }

    /**
     * A test {@link Record} that implements {@link Audience} for testing the
     * middleware/handler cache pattern.
     */
    class Actor extends Record implements Audience {

        /**
         * The actor name.
         */
        String name;

        /**
         * Construct a new {@link Actor}.
         *
         * @param name the name
         */
        Actor(String name) {
            this.name = name;
        }
    }

    /**
     * A test {@link Record} with {@link AccessControl} that hides classified
     * instances from non-admin {@link Audience Audiences}.
     */
    class SecureWidget extends Record implements AccessControl {

        /**
         * The widget name.
         */
        String name;

        /**
         * The widget score.
         */
        int score;

        /**
         * Whether this widget is classified.
         */
        boolean classified;

        /**
         * Construct a new {@link SecureWidget}.
         *
         * @param name the name
         * @param classified whether classified
         */
        SecureWidget(String name, boolean classified) {
            this(name, 0, classified);
        }

        /**
         * Construct a new {@link SecureWidget}.
         *
         * @param name the name
         * @param score the score
         * @param classified whether classified
         */
        SecureWidget(String name, int score, boolean classified) {
            this.name = name;
            this.score = score;
            this.classified = classified;
        }

        @Override
        public boolean $isCreatableBy(Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(Audience audience) {
            return !classified;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return !classified;
        }

        @Override
        public Set<String> $readableBy(Audience audience) {
            return classified ? NO_KEYS : ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return classified ? NO_KEYS : ALL_KEYS;
        }

        @Override
        public Set<String> $writableBy(Audience audience) {
            return classified ? NO_KEYS : ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return ALL_KEYS;
        }
    }

    /**
     * A simple test {@link Record} with a name and score.
     */
    class Widget extends Record {

        /**
         * The widget name.
         */
        String name;

        /**
         * The widget score.
         */
        int score;

        /**
         * Construct a new {@link Widget}.
         *
         * @param name the name
         */
        Widget(String name) {
            this(name, 0);
        }

        /**
         * Construct a new {@link Widget}.
         *
         * @param name the name
         * @param score the score
         */
        Widget(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }

    /**
     * A second test {@link Record} type, distinct from {@link Widget}.
     */
    class Gadget extends Record {

        /**
         * The gadget name.
         */
        String name;

        /**
         * The gadget color.
         */
        String color;

        /**
         * Construct a new {@link Gadget}.
         *
         * @param name the name
         * @param color the color
         */
        Gadget(String name, String color) {
            this.name = name;
            this.color = color;
        }
    }

}
