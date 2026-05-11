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

/**
 * Integration tests for {@link Runway#select(Selection...)} that exercise the
 * bulk-command dispatch path enabled when the connected server supports the
 * Concourse Command API (1.0.0+).
 * <p>
 * The bulk path collapses an N-selection call into a single
 * {@code prepare()}/{@code submit()} round trip via the supplier-pipeline
 * {@link com.cinchapi.runway.db.BatchReader BatchReader}. These tests verify
 * that the dispatch produces the same results as the legacy combinable/isolated
 * path for every {@link DatabaseSelection} subtype the dispatch can hand to
 * {@code $select} &mdash; including mixed-subtype batches, same-class
 * selections with divergent criteria (which the legacy path would isolate), and
 * cache short-circuits inside the batch.
 *
 * @author Jeff Nelson
 */
public class BulkMultiSelectIntegrationTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a multi-selection call that mixes
     * every {@link DatabaseSelection} subtype the dispatch can handle &mdash;
     * load-by-id, find-by-criteria, load-class, count, and unique &mdash;
     * resolves each {@link Selection} to the same result that an equivalent
     * individual call would produce.
     * <p>
     * <strong>Start state:</strong> Three {@link Widget Widgets} saved with
     * scores 10, 50, and 90; one {@link Gadget} saved with name "target".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a load-by-id {@link Selection} for the "target"
     * {@link Gadget}.</li>
     * <li>Build a find {@link Selection} for {@link Widget Widgets} with score
     * &gt; 25.</li>
     * <li>Build a load-class {@link Selection} for all {@link Widget
     * Widgets}.</li>
     * <li>Build a count {@link Selection} for {@link Widget Widgets} with score
     * &gt; 25.</li>
     * <li>Build a unique {@link Selection} for the {@link Widget} with score =
     * 90.</li>
     * <li>Execute all five in a single
     * {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each {@link Selection} yields the
     * corresponding direct-call result: the "target" {@link Gadget}, two
     * {@link Widget Widgets} for the find, three {@link Widget Widgets} for the
     * load-class, {@code 2} for the count, and the score-90 {@link Widget} for
     * the unique.
     */
    @Test
    public void testBulkMultiSelectMixedSubtypesReturnCorrectResults() {
        Widget low = new Widget("low", 10);
        low.save();
        Widget mid = new Widget("mid", 50);
        mid.save();
        Widget high = new Widget("high", 90);
        high.save();
        Gadget target = new Gadget("target", "red");
        target.save();

        Criteria scoreGt25 = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(25).build();
        Criteria scoreEq90 = Criteria.where().key("score")
                .operator(Operator.EQUALS).value(90).build();

        Selection<Gadget> byId = Selection.of(Gadget.class).id(target.id())
                .build();
        Selection<Widget> byCriteria = Selection.of(Widget.class)
                .criteria(scoreGt25).build();
        Selection<Widget> byClass = Selection.of(Widget.class).build();
        Selection<Widget> byCount = Selection.of(Widget.class)
                .criteria(scoreGt25).count().build();
        Selection<Widget> byUnique = Selection.of(Widget.class)
                .criteria(scoreEq90).unique().build();

        runway.select(byId, byCriteria, byClass, byCount, byUnique);

        Gadget loadedGadget = byId.get();
        Set<Widget> filtered = byCriteria.get();
        Set<Widget> all = byClass.get();
        int count = byCount.get();
        Widget unique = byUnique.get();

        Assert.assertNotNull(loadedGadget);
        Assert.assertEquals("target", loadedGadget.name);
        Assert.assertEquals(2, filtered.size());
        Assert.assertEquals(3, all.size());
        Assert.assertEquals(2, count);
        Assert.assertNotNull(unique);
        Assert.assertEquals("high", unique.name);
    }

    /**
     * <strong>Goal:</strong> Verify that same-class {@link Selection
     * Selections} with divergent criteria &mdash; the case the legacy dispatch
     * would isolate to avoid {@code demux} cross-contamination &mdash; resolve
     * to their own criteria's matches and do not share results.
     * <p>
     * <strong>Start state:</strong> Four {@link Widget Widgets} saved with
     * scores 10, 30, 60, and 90.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a find {@link Selection} for score &lt; 25.</li>
     * <li>Build a find {@link Selection} for score &gt; 75.</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first yields the score = 10
     * {@link Widget}; the second yields the score = 90 {@link Widget}; neither
     * leaks into the other's result.
     */
    @Test
    public void testBulkMultiSelectSameClassDivergentCriteria() {
        new Widget("ten", 10).save();
        new Widget("thirty", 30).save();
        new Widget("sixty", 60).save();
        new Widget("ninety", 90).save();

        Criteria lt25 = Criteria.where().key("score")
                .operator(Operator.LESS_THAN).value(25).build();
        Criteria gt75 = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(75).build();

        Selection<Widget> low = Selection.of(Widget.class).criteria(lt25)
                .build();
        Selection<Widget> high = Selection.of(Widget.class).criteria(gt75)
                .build();

        runway.select(low, high);

        Set<Widget> lowResult = low.get();
        Set<Widget> highResult = high.get();

        Assert.assertEquals(1, lowResult.size());
        Assert.assertEquals("ten", lowResult.iterator().next().name);
        Assert.assertEquals(1, highResult.size());
        Assert.assertEquals("ninety", highResult.iterator().next().name);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection} whose result is
     * already in the thread-local reservation cache short-circuits inside the
     * bulk dispatch without poisoning the rest of the batch.
     * <p>
     * <strong>Start state:</strong> Three {@link Widget Widgets} saved with
     * names "a", "b", "c". A reservation is active.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Pre-warm the reservation cache by executing a load-class
     * {@link Selection} for {@link Widget}.</li>
     * <li>Build two new {@link Selection Selections}: a load-class for
     * {@link Widget} (which should hit the reservation) and a find for the "a"
     * {@link Widget} (which should not).</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load-class yields all three {@link Widget
     * Widgets}; the find yields exactly the "a" {@link Widget}.
     */
    @Test
    public void testBulkMultiSelectCachedSelectionShortCircuits() {
        new Widget("a").save();
        new Widget("b").save();
        new Widget("c").save();

        runway.reserve();
        try {
            runway.select(Selection.of(Widget.class).build());

            Selection<Widget> all = Selection.of(Widget.class).build();
            Criteria nameA = Criteria.where().key("name")
                    .operator(Operator.EQUALS).value("a").build();
            Selection<Widget> findA = Selection.of(Widget.class).criteria(nameA)
                    .build();

            runway.select(all, findA);

            Set<Widget> allResult = all.get();
            Set<Widget> findAResult = findA.get();

            Assert.assertEquals(3, allResult.size());
            Assert.assertEquals(1, findAResult.size());
            Assert.assertEquals("a", findAResult.iterator().next().name);
        }
        finally {
            runway.unreserve();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that duplicate {@link Selection Selections}
     * (objects with the same query parameters) passed in a single
     * bulk-dispatched call all receive the same result without triggering
     * redundant database work.
     * <p>
     * <strong>Start state:</strong> Two {@link Widget Widgets} saved with
     * scores 10 and 90.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two distinct {@link Selection} objects with identical
     * criteria.</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both {@link Selection Selections} yield the
     * same single-{@link Widget} result &mdash; the score = 90 {@link Widget}.
     */
    @Test
    public void testBulkMultiSelectDuplicateSelectionsReturnSameResult() {
        new Widget("low", 10).save();
        new Widget("high", 90).save();

        Criteria gt50 = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();

        Selection<Widget> a = Selection.of(Widget.class).criteria(gt50).build();
        Selection<Widget> b = Selection.of(Widget.class).criteria(gt50).build();

        runway.select(a, b);

        Set<Widget> resultA = a.get();
        Set<Widget> resultB = b.get();

        Assert.assertEquals(1, resultA.size());
        Assert.assertEquals(1, resultB.size());
        Assert.assertEquals("high", resultA.iterator().next().name);
        Assert.assertEquals("high", resultB.iterator().next().name);
    }

    /**
     * A simple test {@link Record} with a name and score.
     */
    class Widget extends Record {

        String name;

        int score;

        Widget(String name) {
            this(name, 0);
        }

        Widget(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }

    /**
     * A second test {@link Record} type used to verify multi-class bulk
     * selections.
     */
    class Gadget extends Record {

        String name;

        String color;

        Gadget(String name, String color) {
            this.name = name;
            this.color = color;
        }
    }

}
