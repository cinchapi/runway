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
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;

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
        Selection<Widget> sel = Selection.of(Widget.class, widget.id());
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
        Selection<Widget> sel = Selection.of(Widget.class);
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
        Selection<Widget> sel = Selection.of(Widget.class, criteria);
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
        Selection<Widget> widgetSel = Selection.of(Widget.class);
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class);
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
        Selection<Widget> widgetSel = Selection.of(Widget.class, criteria);
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class, g.id());
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
        Selection<Widget> widgetSel = Selection.of(Widget.class);
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class, g.id());
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
        Selection<Widget> sel = Selection.of(Widget.class);
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
        Selection<Widget> sel = Selection.of(Widget.class, w.id());
        runway.select(sel);
        runway.select(sel);
    }

    /**
     * <strong>Goal:</strong> Verify that configuring a {@link Selection} after
     * submission throws {@link IllegalStateException}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Selection} and submit it.</li>
     * <li>Attempt to call {@code order()} on it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@link IllegalStateException} is thrown.
     */
    @Test(expected = IllegalStateException.class)
    public void testConfigAfterSubmitThrows() {
        new Widget("x").save();
        Selection<Widget> sel = Selection.of(Widget.class);
        runway.select(sel);
        sel.order(null);
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
        Selection<Widget> sel = Selection.of(Widget.class, Random.getLong());
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
        Selection<Player> sel = Selection.ofAny(Player.class);
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
        Selection<Widget> sel = Selection.of(Widget.class, criteria);
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
        Selection<Widget> sel = Selection.of(Widget.class, criteria);
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
        Selection<Widget> sel = Selection.of(Widget.class, w.id());
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
        Selection<Widget> sel = Selection.of(Widget.class, criteria);
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
        Selection<Widget> sel = Selection.of(Widget.class);
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
        Selection<Widget> widgetSel = Selection.of(Widget.class);
        Selection<Gadget> gadgetSel = Selection.of(Gadget.class);
        Selections results = runway.select(widgetSel, gadgetSel);
        Set<Widget> widgets = results.next();
        Set<Gadget> gadgets = results.next();
        Assert.assertEquals(1, widgets.size());
        Assert.assertEquals(1, gadgets.size());
    }

    // ---- Inner Record types for testing ----

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
