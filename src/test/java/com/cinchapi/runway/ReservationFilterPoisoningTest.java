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
import com.cinchapi.runway.access.Scope;

/**
 * Tests that verify the reservation cache does not return stale filtered
 * results to subsequent {@link Selection Selections} that use a different
 * filter.
 * <p>
 * The reservation cache keys on database query parameters (class, criteria,
 * order, page) but not on the client-side filter. If a {@link Selection} with
 * filter F1 executes and its <strong>filtered</strong> result is stored in the
 * reservation, a later {@link Selection} with the same query parameters but a
 * different filter F2 will receive the F1-filtered result from the cache. The
 * framework then applies F2 on top &mdash; but the data that F1 already
 * excluded is irrecoverably lost. This "double filtering" produces incorrect
 * results.
 * </p>
 *
 * @author Jeff Nelson
 */
public class ReservationFilterPoisoningTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a restrictive filter on the first
     * {@link Selection} does not poison the reservation cache for a subsequent
     * permissive {@link Selection} with the same query parameters.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} with
     * different categories.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Item Items}: two with {@code category = "A"} and
     * one with {@code category = "B"}.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Execute a {@link Selection} that finds all {@link Item Items} with
     * {@code category = "A"} and a filter that rejects everything (simulating a
     * restrictive audience).</li>
     * <li>Execute a second {@link Selection} with the same criteria but a
     * filter that accepts everything (simulating a permissive audience).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second {@link Selection} returns both
     * {@link Item Items} matching the criteria, not the empty set left behind
     * by the first filter.
     */
    @Test
    public void testFilteredReservationDoesNotPoisonSubsequentSelect() {
        new Item("item1", "A").save();
        new Item("item2", "A").save();
        new Item("item3", "B").save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("A").build();

        runway.reserve();

        // First selection: same criteria, but a filter that rejects all records
        // (e.g. a bridge audience that cannot pass any visibility check)
        Selection<Item> restrictive = Selection.of(Item.class).where(criteria)
                .filter(item -> false);
        runway.select(restrictive);
        Set<Item> restrictedResult = restrictive.get();
        Assert.assertEquals("Restrictive filter should produce 0 results", 0,
                restrictedResult.size());

        // Second selection: same criteria, but a filter that accepts all
        // records (e.g. the real user audience)
        Selection<Item> permissive = Selection.of(Item.class).where(criteria)
                .filter(item -> true);
        runway.select(permissive);
        Set<Item> permissiveResult = permissive.get();
        Assert.assertEquals(
                "Permissive filter should return all 2 "
                        + "matching items, not the poisoned "
                        + "empty set from the prior filter",
                2, permissiveResult.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a partially restrictive filter does
     * not reduce the result set for a subsequent broader filter.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} in the same
     * category with different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Item Items} in category "X" with scores 10, 50, and
     * 90.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Execute a {@link Selection} that finds all {@link Item Items} with
     * {@code category = "X"} and a filter that only accepts scores above
     * 80.</li>
     * <li>Execute a second {@link Selection} with the same criteria but a
     * filter that accepts scores above 20.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second {@link Selection} returns 2
     * {@link Item Items} (scores 50 and 90), not 1 (only score 90, the survivor
     * of the first filter).
     */
    @Test
    public void testPartialFilterDoesNotReduceSubsequentResults() {
        new Item("low", "X", 10).save();
        new Item("mid", "X", 50).save();
        new Item("high", "X", 90).save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("X").build();

        runway.reserve();

        // First: narrow filter (score > 80) → only "high"
        Selection<Item> narrow = Selection.of(Item.class).where(criteria)
                .filter(item -> item.score > 80);
        runway.select(narrow);
        Set<Item> narrowResult = narrow.get();
        Assert.assertEquals(1, narrowResult.size());

        // Second: broader filter (score > 20) → should get "mid" and "high",
        // but if the reservation is poisoned, "mid" was already excluded
        Selection<Item> broad = Selection.of(Item.class).where(criteria)
                .filter(item -> item.score > 20);
        runway.select(broad);
        Set<Item> broadResult = broad.get();
        Assert.assertEquals(
                "Broader filter should return 2 items, "
                        + "not the 1 item left by the " + "narrow filter",
                2, broadResult.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a count {@link Selection} with a
     * filter does not poison the reservation for a subsequent count with a
     * different filter.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} in the same
     * category.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Item Items} in category "C".</li>
     * <li>Open a reservation.</li>
     * <li>Execute a count {@link Selection} with a reject-all filter.</li>
     * <li>Execute a count {@link Selection} with an accept-all filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second count returns 3, not 0.
     */
    @Test
    public void testFilteredCountDoesNotPoisonSubsequentCount() {
        new Item("c1", "C").save();
        new Item("c2", "C").save();
        new Item("c3", "C").save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("C").build();

        runway.reserve();

        // First: count with reject-all filter
        Selection<Item> restrictiveCount = Selection.of(Item.class)
                .where(criteria).filter(item -> false).count();
        runway.select(restrictiveCount);
        int restricted = restrictiveCount.get();
        Assert.assertEquals(0, restricted);

        // Second: count with accept-all filter
        Selection<Item> permissiveCount = Selection.of(Item.class)
                .where(criteria).filter(item -> true).count();
        runway.select(permissiveCount);
        int permissive = permissiveCount.get();
        Assert.assertEquals(
                "Permissive count should return 3, not "
                        + "the poisoned 0 from the prior " + "filter",
                3, permissive);
    }

    /**
     * <strong>Goal:</strong> Verify that a filtered count does not poison a
     * subsequent <em>unfiltered</em> count with the same query parameters.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} in the same
     * category.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Item Items} in category "E".</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Execute a count {@link Selection} with a reject-all filter.</li>
     * <li>Execute an unfiltered count {@link Selection} with the same
     * criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The filtered count returns 0. The unfiltered
     * count returns 3.
     */
    @Test
    public void testFilteredCountDoesNotPoisonUnfilteredCount() {
        new Item("e1", "E").save();
        new Item("e2", "E").save();
        new Item("e3", "E").save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("E").build();

        runway.reserve();

        // First: count with reject-all filter
        Selection<Item> filteredCount = Selection.of(Item.class).where(criteria)
                .filter(item -> false).count();
        runway.select(filteredCount);
        int filtered = filteredCount.get();
        Assert.assertEquals(0, filtered);

        // Second: unfiltered count — must not return 0
        Selection<Item> unfilteredCount = Selection.of(Item.class)
                .where(criteria).count();
        runway.select(unfilteredCount);
        int unfiltered = unfilteredCount.get();
        Assert.assertEquals(
                "Unfiltered count should return 3, not "
                        + "the poisoned 0 from the prior " + "filter",
                3, unfiltered);
    }

    /**
     * <strong>Goal:</strong> Verify that an unfiltered {@link Selection}
     * correctly caches its result and a subsequent filtered {@link Selection}
     * with the same query parameters applies the filter to the cached
     * unfiltered data.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} with
     * different categories.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Item Items}: two with {@code category = "A"} and
     * one with {@code category = "B"}.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Execute an unfiltered {@link Selection} that finds all {@link Item
     * Items} with {@code category = "A"}.</li>
     * <li>Execute a second {@link Selection} with the same criteria but a
     * filter that only accepts items with {@code score > 50}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first {@link Selection} returns 2
     * {@link Item Items}. The second {@link Selection} returns 1 {@link Item}
     * (the one with {@code score > 50}), demonstrating that the filter was
     * correctly applied to the cached unfiltered data.
     */
    @Test
    public void testUnfilteredCacheHitIsCorrectlyFiltered() {
        new Item("low", "A", 10).save();
        new Item("high", "A", 90).save();
        new Item("other", "B", 50).save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("A").build();

        runway.reserve();

        // First: unfiltered — caches all matching items
        Selection<Item> unfiltered = Selection.of(Item.class).where(criteria);
        runway.select(unfiltered);
        Set<Item> unfilteredResult = unfiltered.get();
        Assert.assertEquals(2, unfilteredResult.size());

        // Second: filtered — should apply filter to the cached unfiltered data
        Selection<Item> filtered = Selection.of(Item.class).where(criteria)
                .filter(item -> item.score > 50);
        runway.select(filtered);
        Set<Item> filteredResult = filtered.get();
        Assert.assertEquals(
                "Filtered selection should return 1 item "
                        + "from the cached unfiltered data",
                1, filteredResult.size());
    }

    /**
     * <strong>Goal:</strong> Verify that an unfiltered count does not poison a
     * subsequent filtered count with the same query parameters.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} in the same
     * category with different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Item Items} in category "D" with scores 10, 50, and
     * 90.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Execute an unfiltered count {@link Selection}.</li>
     * <li>Execute a filtered count {@link Selection} that only accepts scores
     * above 40.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The unfiltered count returns 3. The filtered
     * count returns 2 (scores 50 and 90), not 3.
     */
    @Test
    public void testUnfilteredCountDoesNotPoisonFilteredCount() {
        new Item("low", "D", 10).save();
        new Item("mid", "D", 50).save();
        new Item("high", "D", 90).save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("D").build();

        runway.reserve();

        // First: unfiltered count
        Selection<Item> unfilteredCount = Selection.of(Item.class)
                .where(criteria).count();
        runway.select(unfilteredCount);
        int unfiltered = unfilteredCount.get();
        Assert.assertEquals(3, unfiltered);

        // Second: filtered count — must not return the cached unfiltered count
        // of 3
        Selection<Item> filteredCount = Selection.of(Item.class).where(criteria)
                .filter(item -> item.score > 40).count();
        runway.select(filteredCount);
        int filtered = filteredCount.get();
        Assert.assertEquals("Filtered count should return 2, not "
                + "the cached unfiltered count of 3", 2, filtered);
    }

    /**
     * <strong>Goal:</strong> Verify that two {@link Selection Selections} with
     * the same query parameters but different filters produce independent
     * results when passed to a single {@link Runway#select(Selection...)} call.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} in category
     * "F" with different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Item Items} in category "F" with scores 10, 50, and
     * 90.</li>
     * <li>Construct two {@link Selection Selections} with the same criteria but
     * different filters: one that rejects everything and one that accepts
     * scores above 20.</li>
     * <li>Pass both to a single {@link Runway#select(Selection...)} call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The restrictive {@link Selection} returns 0
     * {@link Item Items}. The permissive {@link Selection} returns 2
     * {@link Item Items} (scores 50 and 90). The permissive result must not be
     * contaminated by the restrictive filter.
     */
    @Test
    public void testBatchSelectWithDifferentFiltersProducesIndependentResults() {
        new Item("f1", "F", 10).save();
        new Item("f2", "F", 50).save();
        new Item("f3", "F", 90).save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("F").build();

        // Two selections with the same criteria but different filters in a
        // single select() call
        Selection<Item> restrictive = Selection.of(Item.class).where(criteria)
                .filter(item -> false);
        Selection<Item> permissive = Selection.of(Item.class).where(criteria)
                .filter(item -> item.score > 20);

        runway.select(restrictive, permissive);

        Set<Item> restrictedResult = restrictive.get();
        Set<Item> permissiveResult = permissive.get();

        Assert.assertEquals("Restrictive filter should return 0 items", 0,
                restrictedResult.size());
        Assert.assertEquals("Permissive filter should return 2 items "
                + "independently, not the 0 from " + "the restrictive filter",
                2, permissiveResult.size());
    }

    /**
     * <strong>Goal:</strong> Verify that duplicate unfiltered {@link Selection
     * Selections} with identical query parameters in a single
     * {@link Runway#select(Selection...)} call are deduped and both receive the
     * correct result.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Item Items} in category
     * "G".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Item Items} in category "G".</li>
     * <li>Construct two unfiltered {@link Selection Selections} with the same
     * criteria.</li>
     * <li>Pass both to a single {@link Runway#select(Selection...)} call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both {@link Selection Selections} return the
     * same 2 {@link Item Items}. The duplicate receives its result via
     * propagation from the canonical.
     */
    @Test
    public void testDuplicateUnfilteredSelectionsAreDedupedAndBothReturnResults() {
        new Item("g1", "G").save();
        new Item("g2", "G").save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("G").build();

        Selection<Item> first = Selection.of(Item.class).where(criteria);
        Selection<Item> second = Selection.of(Item.class).where(criteria);

        runway.select(first, second);

        Set<Item> firstResult = first.get();
        Set<Item> secondResult = second.get();

        Assert.assertEquals(2, firstResult.size());
        Assert.assertEquals(
                "Duplicate selection should receive the "
                        + "same result via propagation",
                2, secondResult.size());
        Assert.assertEquals(firstResult, secondResult);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection} pre-resolved via
     * {@link Scope#none()} &mdash; state {@code RESOLVED}, injected reject-all
     * filter, empty result installed in place &mdash; does not seed the
     * reservation cache when passed to a single-selection
     * {@link Runway#select(Selection...)} call. A subsequent unscoped
     * {@link Runway#find(Class, Criteria) find} with the same criteria must
     * return the real data.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Item Items} in category
     * "S".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Item Items} with {@code category = "S"}.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Build a {@link Selection} for {@code category = "S"} and apply
     * {@link Scope#none()} to install an empty {@code RESOLVED} result.</li>
     * <li>Pass the {@code RESOLVED} {@link Selection} through
     * {@link Runway#select(Selection...)}.</li>
     * <li>Invoke {@link Runway#find(Class, Criteria) find} for the same
     * criteria with no scope.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first call returns 0 {@link Item Items}.
     * The subsequent {@link Runway#find(Class, Criteria) find} returns both
     * {@link Item Items} &mdash; the {@link Scope#none()} result does not
     * poison the reservation.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testScopeNoneResolvedDoesNotPoisonSingleSelectionReservation() {
        new Item("a", "S").save();
        new Item("b", "S").save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("S").build();

        runway.reserve();

        Selection<Item> scoped = (Selection<Item>) Scope.none()
                .apply(Selection.of(Item.class).where(criteria));
        runway.select(scoped);
        Assert.assertEquals(0, scoped.get().size());

        Set<Item> unscoped = runway.find(Item.class, criteria);
        Assert.assertEquals(
                "Scope.none() RESOLVED selection must not poison "
                        + "the reservation for a same-key unscoped read",
                2, unscoped.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection} pre-resolved via
     * {@link Scope#none()} passed alongside other {@link Selection Selections}
     * to a single multi-selection {@link Runway#select(Selection...)} call does
     * not seed the reservation cache, so a subsequent unscoped
     * {@link Runway#find(Class, Criteria) find} with the same criteria still
     * returns the real data.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Item Items} in category
     * "T".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Item Items} with {@code category = "T"}.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Build a {@link Scope#none()}-resolved {@link Selection} for
     * {@code category = "T"} and a sibling {@link Selection} for an unrelated
     * criteria.</li>
     * <li>Pass both to a single {@link Runway#select(Selection...)} call.</li>
     * <li>Invoke {@link Runway#find(Class, Criteria) find} for the
     * {@code category = "T"} criteria with no scope.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Scope#none()} {@link Selection}
     * returns 0 {@link Item Items}. The subsequent
     * {@link Runway#find(Class, Criteria) find} returns both {@link Item
     * Items}.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testScopeNoneResolvedDoesNotPoisonMultiSelectionReservation() {
        new Item("a", "T").save();
        new Item("b", "T").save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("T").build();
        Criteria siblingCriteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("nonexistent").build();

        runway.reserve();

        Selection<Item> scoped = (Selection<Item>) Scope.none()
                .apply(Selection.of(Item.class).where(criteria));
        Selection<Item> sibling = Selection.of(Item.class)
                .where(siblingCriteria);
        runway.select(scoped, sibling);
        Assert.assertEquals(0, scoped.get().size());

        Set<Item> unscoped = runway.find(Item.class, criteria);
        Assert.assertEquals("Scope.none() RESOLVED selection in a multi-select "
                + "must not poison the reservation for a "
                + "same-key unscoped read", 2, unscoped.size());
    }

    /**
     * <strong>Goal:</strong> Verify that an unscoped {@link Selection} passed
     * alongside a {@link Scope#none()}-resolved {@link Selection} in the same
     * multi-selection {@link Runway#select(Selection...)} call <em>does</em>
     * seed the reservation cache for its own query parameters.
     * <p>
     * Skipping the reservation for {@code RESOLVED} {@link Selection
     * Selections} must not regress reservation seeding for the unscoped
     * siblings in the batch.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Item Items} in category
     * "U".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Item Items} with {@code category = "U"}.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Build a {@link Scope#none()}-resolved {@link Selection} and a normal
     * {@link Selection} for {@code category = "U"}.</li>
     * <li>Pass both to a single {@link Runway#select(Selection...)} call.</li>
     * <li>Save a third {@link Item} with {@code category = "U"} &mdash; this
     * mutation must not be visible through the reservation.</li>
     * <li>Invoke {@link Runway#find(Class, Criteria) find} for
     * {@code category = "U"}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The normal {@link Selection} returns 2
     * {@link Item Items}. The subsequent {@link Runway#find(Class, Criteria)
     * find} also returns 2 (the seeded reservation), not 3 (a fresh database
     * read).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testUnscopedSiblingOfScopeNoneStillSeedsReservation() {
        new Item("a", "U").save();
        new Item("b", "U").save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("U").build();

        runway.reserve();

        Selection<Item> scoped = (Selection<Item>) Scope.none()
                .apply(Selection.of(Item.class).where(criteria));
        Selection<Item> normal = Selection.of(Item.class).where(criteria);
        runway.select(scoped, normal);

        Assert.assertEquals(0, scoped.get().size());
        Assert.assertEquals(2, normal.get().size());

        new Item("c", "U").save();

        Set<Item> reserved = runway.find(Item.class, criteria);
        Assert.assertEquals(
                "Unscoped sibling of a RESOLVED selection must seed "
                        + "the reservation; a same-key follow-up should "
                        + "see the cached two items, not the three now "
                        + "stored",
                2, reserved.size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#none()} applied to every
     * {@link Selection} subtype the dispatch handles (find, load-class,
     * load-by-id, count) inside a single multi-selection call preserves each
     * subtype's pre-installed scoped-empty result and does not poison the
     * reservation cache for any of them.
     * <p>
     * The reservation cache key is shared across subtypes for a given query
     * signature, but {@link Scope#none()} produces different result shapes
     * (empty set / 0 / {@code null}). This test exercises each shape in a
     * single dispatch so any cross-subtype regression in the
     * {@code RESOLVED}-handling branches surfaces here.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Item Items} in category
     * "W".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Item Items} with {@code category = "W"}.</li>
     * <li>Open a reservation via {@link Runway#reserve()}.</li>
     * <li>Build a find, a load-class, a load-by-id, and a count
     * {@link Selection} for the {@link Item Items} and apply
     * {@link Scope#none()} to each.</li>
     * <li>Pass all four to a single {@link Runway#select(Selection...)}
     * call.</li>
     * <li>Invoke {@link Runway#find(Class, Criteria) find},
     * {@link Runway#load(Class) load}, {@link Runway#load(Class, long) load by
     * id}, and {@link Runway#count(Class, Criteria) count} with the same query
     * parameters but no scope.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The find, load-class, and load-by-id scoped
     * {@link Selection Selections} return their empty/{@code null} results and
     * the count scoped {@link Selection} returns {@code 0}. The subsequent
     * unscoped reads return real data: 2 items for find and load-class, the
     * {@link Item} for load-by-id, and {@code 2} for count.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testScopeNoneAcrossSelectionSubtypesPreservesResultsAndDoesNotPoison() {
        Item one = new Item("a", "W");
        one.save();
        Item two = new Item("b", "W");
        two.save();

        Criteria criteria = Criteria.where().key("category")
                .operator(Operator.EQUALS).value("W").build();

        runway.reserve();

        Selection<Item> findScoped = (Selection<Item>) Scope.none()
                .apply(Selection.of(Item.class).where(criteria));
        Selection<Item> loadClassScoped = (Selection<Item>) Scope.none()
                .apply(Selection.of(Item.class));
        Selection<Item> loadRecordScoped = (Selection<Item>) Scope.none()
                .apply(Selection.of(Item.class).id(one.id()));
        Selection<Item> countScoped = (Selection<Item>) Scope.none()
                .apply(Selection.of(Item.class).where(criteria).count());

        runway.select(findScoped, loadClassScoped, loadRecordScoped,
                countScoped);

        Set<Item> findResult = findScoped.get();
        Set<Item> loadClassResult = loadClassScoped.get();
        Item loadRecordResult = loadRecordScoped.get();
        int countResult = countScoped.get();

        Assert.assertEquals(0, findResult.size());
        Assert.assertEquals(0, loadClassResult.size());
        Assert.assertNull(loadRecordResult);
        Assert.assertEquals(0, countResult);

        Assert.assertEquals(2, runway.find(Item.class, criteria).size());
        Assert.assertEquals(2, runway.load(Item.class).size());
        Assert.assertNotNull(runway.load(Item.class, one.id()));
        Assert.assertEquals(2, runway.count(Item.class, criteria));
    }

    /**
     * A simple test {@link Record} with a name, category, and score.
     */
    class Item extends Record {

        /**
         * The item name.
         */
        String name;

        /**
         * The item category.
         */
        String category;

        /**
         * The item score.
         */
        int score;

        /**
         * Construct a new {@link Item}.
         *
         * @param name the name
         * @param category the category
         */
        Item(String name, String category) {
            this(name, category, 0);
        }

        /**
         * Construct a new {@link Item}.
         *
         * @param name the name
         * @param category the category
         * @param score the score
         */
        Item(String name, String category, int score) {
            this.name = name;
            this.category = category;
            this.score = score;
        }
    }

}
