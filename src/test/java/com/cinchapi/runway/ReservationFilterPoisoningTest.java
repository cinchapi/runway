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

        // First selection: same criteria, but a filter that
        // rejects all records (e.g. a bridge audience that
        // cannot pass any visibility check)
        Selection<Item> restrictive = Selection.of(Item.class).where(criteria)
                .filter(item -> false);
        runway.select(restrictive);
        Set<Item> restrictedResult = restrictive.get();
        Assert.assertEquals("Restrictive filter should produce 0 results", 0,
                restrictedResult.size());

        // Second selection: same criteria, but a filter that
        // accepts all records (e.g. the real user audience)
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

        // Second: broader filter (score > 20) → should get
        // "mid" and "high", but if the reservation is
        // poisoned, "mid" was already excluded
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

        // Second: filtered — should apply filter to the
        // cached unfiltered data
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

        // Second: filtered count — must not return
        // the cached unfiltered count of 3
        Selection<Item> filteredCount = Selection.of(Item.class).where(criteria)
                .filter(item -> item.score > 40).count();
        runway.select(filteredCount);
        int filtered = filteredCount.get();
        Assert.assertEquals("Filtered count should return 2, not "
                + "the cached unfiltered count of 3", 2, filtered);
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
