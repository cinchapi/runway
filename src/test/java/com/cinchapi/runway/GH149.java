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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableList;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/149">GH-149</a>: on the
 * legacy read path for servers without native sorting and pagination, reads
 * that combine a client-side filter with an {@link Order} or a {@link Page}
 * must apply the filter, and pagination must operate over the records that pass
 * the filter.
 *
 * @author Jeff Nelson
 */
public class GH149 extends RunwayBaseClientServerTest {

    /**
     * Force {@link #runway} onto the legacy read path used for servers that
     * predate native sorting and pagination support.
     */
    private void forceLegacyReadPath() {
        Reflection.set("hasNativeSortingAndPagination", false, runway);
    }

    /**
     * Save a {@link Player} for each of the {@code scores}, named
     * alphabetically ({@code "a"}, {@code "b"}, ...) in iteration order.
     *
     * @param scores the score for each saved {@link Player}
     */
    private void savePlayers(int... scores) {
        for (int i = 0; i < scores.length; ++i) {
            Player player = new Player(String.valueOf((char) ('a' + i)),
                    scores[i]);
            runway.save(player);
        }
    }

    /**
     * Save {@code count} {@link Player Players} with random names and random
     * scores between {@code 0} and {@code 99}.
     *
     * @param count the number of {@link Player Players} to save
     */
    private void saveRandomPlayers(int count) {
        for (int i = 0; i < count; ++i) {
            Player player = new Player(Random.getSimpleString(),
                    Random.getPositiveNumber().intValue() % 100);
            runway.save(player);
        }
    }

    /**
     * Return the {@link Player} names from {@code players} in iteration order.
     *
     * @param players the {@link Player Players} whose names to list
     * @return the names in iteration order
     */
    private List<String> names(Set<Player> players) {
        return players.stream().map(player -> player.name)
                .collect(Collectors.toList());
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted {@code find} applies the
     * client-side filter on the legacy read path.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with scores
     * {@code 10} through {@code 60} and a {@link Runway} forced onto the legacy
     * read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Find {@link Player Players} with {@code score > 15}, ordered by
     * {@code score}, filtered to {@code score < 45}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only the records that match the criteria and
     * pass the filter are returned, in {@code score} order.
     */
    @Test
    public void testFindWithOrderAppliesFilter() {
        savePlayers(10, 20, 30, 40, 50, 60);
        forceLegacyReadPath();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(15);
        Set<Player> actual = runway.find(Player.class, criteria,
                Order.by("score"), player -> player.score < 45);
        Assert.assertEquals(ImmutableList.of("b", "c", "d"), names(actual));
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted and paginated {@code find}
     * applies the client-side filter before pagination on the legacy read path.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with scores
     * {@code 10} through {@code 80} and a {@link Runway} forced onto the legacy
     * read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Find {@link Player Players} with {@code score > 0}, ordered by
     * {@code score}, filtered to multiples of {@code 20}, on the page that
     * skips {@code 2} and limits to {@code 2}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The page contains the third and fourth records
     * that pass the filter ({@code score} {@code 60} and {@code 80}), not
     * records drawn from the unfiltered stream.
     */
    @Test
    public void testFindWithOrderAndPageAppliesFilterBeforePagination() {
        savePlayers(10, 20, 30, 40, 50, 60, 70, 80);
        forceLegacyReadPath();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Set<Player> actual = runway.find(Player.class, criteria,
                Order.by("score"), Page.skipLimit(2, 2),
                player -> player.score % 20 == 0);
        Assert.assertEquals(ImmutableList.of("f", "h"), names(actual));
    }

    /**
     * <strong>Goal:</strong> Verify that a paginated {@code find} without an
     * {@link Order} applies the client-side filter on the legacy read path.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with scores
     * {@code 10} through {@code 80} and a {@link Runway} forced onto the legacy
     * read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Find {@link Player Players} with {@code score > 0}, filtered to
     * multiples of {@code 20}, on the page that skips {@code 0} and limits to
     * {@code 3}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The page contains {@code 3} records and every
     * one of them passes the filter.
     */
    @Test
    public void testFindWithPageAppliesFilter() {
        savePlayers(10, 20, 30, 40, 50, 60, 70, 80);
        forceLegacyReadPath();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Set<Player> actual = runway.find(Player.class, criteria,
                Page.skipLimit(0, 3), player -> player.score % 20 == 0);
        Assert.assertEquals(3, actual.size());
        actual.forEach(player -> Assert.assertEquals(0, player.score % 20));
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted {@code load} applies the
     * client-side filter on the legacy read path.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with scores
     * {@code 10} through {@code 60} and a {@link Runway} forced onto the legacy
     * read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load {@link Player Players} ordered by {@code score}, filtered to
     * {@code score < 45}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only the records that pass the filter are
     * returned, in {@code score} order.
     */
    @Test
    public void testLoadWithOrderAppliesFilter() {
        savePlayers(10, 20, 30, 40, 50, 60);
        forceLegacyReadPath();
        Set<Player> actual = runway.load(Player.class, Order.by("score"),
                player -> player.score < 45);
        Assert.assertEquals(ImmutableList.of("a", "b", "c", "d"),
                names(actual));
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted and paginated {@code load}
     * applies the client-side filter before pagination on the legacy read path.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with scores
     * {@code 10} through {@code 80} and a {@link Runway} forced onto the legacy
     * read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load {@link Player Players} ordered by {@code score}, filtered to
     * multiples of {@code 20}, on the page that skips {@code 2} and limits to
     * {@code 2}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The page contains the third and fourth records
     * that pass the filter ({@code score} {@code 60} and {@code 80}), not
     * records drawn from the unfiltered stream.
     */
    @Test
    public void testLoadWithOrderAndPageAppliesFilterBeforePagination() {
        savePlayers(10, 20, 30, 40, 50, 60, 70, 80);
        forceLegacyReadPath();
        Set<Player> actual = runway.load(Player.class, Order.by("score"),
                Page.skipLimit(2, 2), player -> player.score % 20 == 0);
        Assert.assertEquals(ImmutableList.of("f", "h"), names(actual));
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted {@code find} applies the
     * client-side filter on the legacy read path across randomized data.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with random
     * names and scores and a {@link Runway} forced onto the legacy read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Compute the expected records from an unfiltered sorted {@code find}
     * by applying the filter client-side.</li>
     * <li>Find {@link Player Players} with {@code score > 0}, ordered by
     * {@code name}, with the same filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The found records equal the expected filtered
     * records.
     */
    @Test
    public void testFindWithOrderAppliesFilterRandomizedData() {
        saveRandomPlayers(100);
        forceLegacyReadPath();
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Predicate<Player> filter = player -> player.score < 45;
        Set<Player> expected = runway
                .find(Player.class, criteria, Order.by("name")).stream()
                .filter(filter)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Player> actual = runway.find(Player.class, criteria,
                Order.by("name"), filter);
        Assert.assertEquals(expected, actual);
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted and paginated {@code find}
     * applies the client-side filter before pagination on the legacy read path
     * across randomized data.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with random
     * names and scores and a {@link Runway} forced onto the legacy read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Compute the expected records from an unfiltered sorted {@code find}
     * by filtering, then applying the page's skip and limit client-side.</li>
     * <li>Find {@link Player Players} with {@code score > 0}, ordered by
     * {@code name}, with the same filter and page.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The found records equal the expected records,
     * which proves the page is drawn only from records that pass the filter.
     */
    @Test
    public void testFindWithOrderAndPageAppliesFilterRandomizedData() {
        saveRandomPlayers(100);
        forceLegacyReadPath();
        Page page = Page.skipLimit(2, 5);
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Predicate<Player> filter = player -> player.score % 20 == 0;
        Set<Player> expected = runway
                .find(Player.class, criteria, Order.by("name")).stream()
                .filter(filter).skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Player> actual = runway.find(Player.class, criteria,
                Order.by("name"), page, filter);
        Assert.assertEquals(expected, actual);
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted {@code load} applies the
     * client-side filter on the legacy read path across randomized data.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with random
     * names and scores and a {@link Runway} forced onto the legacy read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Compute the expected records from an unfiltered sorted {@code load}
     * by applying the filter client-side.</li>
     * <li>Load {@link Player Players} ordered by {@code name} with the same
     * filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded records equal the expected filtered
     * records.
     */
    @Test
    public void testLoadWithOrderAppliesFilterRandomizedData() {
        saveRandomPlayers(100);
        forceLegacyReadPath();
        Predicate<Player> filter = player -> player.score < 45;
        Set<Player> expected = runway.load(Player.class, Order.by("name"))
                .stream().filter(filter)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Player> actual = runway.load(Player.class, Order.by("name"),
                filter);
        Assert.assertEquals(expected, actual);
    }

    /**
     * <strong>Goal:</strong> Verify that a sorted and paginated {@code load}
     * applies the client-side filter before pagination on the legacy read path
     * across randomized data.
     * <p>
     * <strong>Start state:</strong> Saved {@link Player Players} with random
     * names and scores and a {@link Runway} forced onto the legacy read path.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Compute the expected records from an unfiltered sorted {@code load}
     * by filtering, then applying the page's skip and limit client-side.</li>
     * <li>Load {@link Player Players} ordered by {@code name} with the same
     * filter and page.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded records equal the expected records,
     * which proves the page is drawn only from records that pass the filter.
     */
    @Test
    public void testLoadWithOrderAndPageAppliesFilterRandomizedData() {
        saveRandomPlayers(100);
        forceLegacyReadPath();
        Page page = Page.skipLimit(2, 5);
        Predicate<Player> filter = player -> player.score % 20 == 0;
        Set<Player> expected = runway.load(Player.class, Order.by("name"))
                .stream().filter(filter).skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Player> actual = runway.load(Player.class, Order.by("name"), page,
                filter);
        Assert.assertEquals(expected, actual);
    }

}
