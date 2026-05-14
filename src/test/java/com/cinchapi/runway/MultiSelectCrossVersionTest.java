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

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.CrossVersionTest;
import com.cinchapi.concourse.test.runners.CrossVersionTestRunner.Versions;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Cross-version regression tests for {@link Runway#select(Selection...)}
 * multi-selection dispatch.
 * <p>
 * The {@code 0.12.8} server lacks the Concourse Command API, so
 * {@code select(Selection...)} dispatches through the legacy
 * combinable/isolated path. The latest server supports
 * {@code prepare()}/{@code submit()}, so {@code select(Selection...)}
 * dispatches every {@link Selection} through a single batched
 * {@link com.cinchapi.runway.db.EventualReader EventualReader}. Running the
 * same {@link Test} body against both versions guards against either path
 * regressing relative to the other.
 *
 * @author Jeff Nelson
 */
@Versions({ "0.12.8", Testing.CONCOURSE_VERSION })
public class MultiSelectCrossVersionTest extends CrossVersionTest {

    private Runway runway;

    @Override
    public void afterStartedTest() {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    @Override
    public void beforeEachTest() {
        runway = Runway.builder().port(server.getClientPort()).build();
    }

    /**
     * <strong>Goal:</strong> Verify that a multi-selection call mixing every
     * {@link DatabaseSelection} subtype the dispatch can handle &mdash;
     * load-by-id, find-by-criteria, load-class, count, and unique &mdash;
     * resolves each {@link Selection} to the same result on both the legacy and
     * bulk paths.
     * <p>
     * <strong>Start state:</strong> Three {@link Widget Widgets} saved with
     * scores 10, 50, and 90; one {@link Gadget} saved with the name
     * {@code "target"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a load-by-id {@link Selection} for the {@code "target"}
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
     * corresponding direct-call result: the {@code "target"} {@link Gadget},
     * two {@link Widget Widgets} for the find, three {@link Widget Widgets} for
     * the load-class, {@code 2} for the count, and the score-90 {@link Widget}
     * for the unique.
     */
    @Test
    public void testMixedSubtypesReturnCorrectResults() {
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
     * to their own criteria's matches and do not share results on either path.
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
    public void testSameClassDivergentCriteria() {
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
     * dispatch without poisoning the rest of the batch on either path.
     * <p>
     * <strong>Start state:</strong> Three {@link Widget Widgets} saved with
     * names {@code "a"}, {@code "b"}, {@code "c"}. A reservation is active.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Pre-warm the reservation cache by executing a load-class
     * {@link Selection} for {@link Widget}.</li>
     * <li>Build two new {@link Selection Selections}: a load-class for
     * {@link Widget} (which should hit the reservation) and a find for the
     * {@code "a"} {@link Widget} (which should not).</li>
     * <li>Execute both in a single {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load-class yields all three {@link Widget
     * Widgets}; the find yields exactly the {@code "a"} {@link Widget}.
     */
    @Test
    public void testCachedSelectionShortCircuits() {
        new Widget("a", 1).save();
        new Widget("b", 2).save();
        new Widget("c", 3).save();

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
     * (objects with the same query parameters) passed in a single dispatched
     * call all receive the same result on either path.
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
    public void testDuplicateSelectionsReturnSameResult() {
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
     * <strong>Goal:</strong> Verify that load-by-id {@link Selection
     * Selections} targeting a {@link Record} class with descendants &mdash;
     * forcing the section-lookup branch &mdash; and scoped by {@link Realms}
     * resolve to the correct records on either path when dispatched alongside
     * an unrelated {@link Selection}.
     * <p>
     * <strong>Start state:</strong> Two {@link SuperWidget} records saved under
     * realm {@code "alpha"} and one {@link SubWidget} saved under realm
     * {@code "beta"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the three hierarchy {@link Record Records}.</li>
     * <li>Build two load-by-id {@link Selection Selections} typed as
     * {@link SuperWidget} with {@link Realms#only(String) realms("alpha")},
     * targeting one in-realm record and one out-of-realm
     * {@link SubWidget}.</li>
     * <li>Build a third unrelated load-class {@link Selection} to confirm
     * shared-batch behavior.</li>
     * <li>Execute all three in a single
     * {@link Runway#select(Selection...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The in-realm load returns the
     * {@link SuperWidget}; the out-of-realm load returns {@code null}; the
     * load-class returns all three hierarchy records.
     */
    @Test
    public void testRecordWithHierarchyAndRealms() {
        SuperWidget alphaOne = new SuperWidget("alphaOne");
        alphaOne.addRealm("alpha");
        alphaOne.save();
        SuperWidget alphaTwo = new SuperWidget("alphaTwo");
        alphaTwo.addRealm("alpha");
        alphaTwo.save();
        SubWidget betaSub = new SubWidget("betaSub", "extra");
        betaSub.addRealm("beta");
        betaSub.save();

        Selection<SuperWidget> inRealm = Selection.of(SuperWidget.class)
                .id(alphaOne.id()).realms(Realms.only("alpha")).build();
        Selection<SuperWidget> outOfRealm = Selection.of(SuperWidget.class)
                .id(betaSub.id()).realms(Realms.only("alpha")).build();
        Selection<SuperWidget> all = Selection.of(SuperWidget.class).any()
                .build();

        runway.select(inRealm, outOfRealm, all);

        SuperWidget inRealmResult = inRealm.get();
        SuperWidget outOfRealmResult = outOfRealm.get();
        Set<SuperWidget> allResult = all.get();

        Assert.assertNotNull(inRealmResult);
        Assert.assertEquals("alphaOne", inRealmResult.name);
        Assert.assertNull(outOfRealmResult);
        Assert.assertEquals(3, allResult.size());
    }

    /**
     * A {@link Record} class with at least one descendant so that loading by id
     * triggers the section-lookup branch in {@code $selectRecord}.
     */
    class SuperWidget extends Record {

        String name;

        SuperWidget(String name) {
            this.name = name;
        }
    }

    /**
     * A subclass of {@link SuperWidget} that gives the hierarchy more than one
     * section.
     */
    class SubWidget extends SuperWidget {

        String extra;

        SubWidget(String name, String extra) {
            super(name);
            this.extra = extra;
        }
    }

    /**
     * A simple test {@link Record} with a name and score.
     */
    class Widget extends Record {

        String name;

        int score;

        Widget(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }

    /**
     * A second test {@link Record} type used to verify multi-class dispatched
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
