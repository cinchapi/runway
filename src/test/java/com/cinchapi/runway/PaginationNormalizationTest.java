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
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests that verify pagination correctly applies both skip and limit across all
 * query methods. This addresses a normalization fix where some legacy code
 * paths were missing the skip operation.
 *
 * @author Jeff Nelson
 */
public class PaginationNormalizationTest extends RunwayBaseClientServerTest {

    @Test
    public void testFindWithOrderAndPageMatchesSkipLimit() {
        for (int i = 0; i < 10; i++) {
            Player player = new Player("Player" + i, i * 10);
            runway.save(player);
        }

        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(0);
        Order order = Order.by("score").ascending();
        Page page = Page.of(2, 3);

        Set<Player> expected = runway.find(Player.class, criteria, order)
                .stream().skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Player> actual = runway.find(Player.class, criteria, order, page);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testLoadWithOrderAndPageMatchesSkipLimit() {
        for (int i = 0; i < 10; i++) {
            Player player = new Player("Player" + i, i * 10);
            runway.save(player);
        }

        Order order = Order.by("score").ascending();
        Page page = Page.of(2, 3);

        Set<Player> expected = runway.load(Player.class, order).stream()
                .skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Player> actual = runway.load(Player.class, order, page);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testLoadAnyWithOrderAndPageMatchesSkipLimit() {
        for (int i = 0; i < 5; i++) {
            Player player = new Player("Player" + i, i * 10);
            runway.save(player);
        }
        for (int i = 0; i < 5; i++) {
            PointGuard pg = new PointGuard("PG" + i, (i + 5) * 10, i);
            runway.save(pg);
        }

        Order order = Order.by("score").ascending();
        Page page = Page.of(2, 3);

        Set<Player> expected = runway.loadAny(Player.class, order).stream()
                .skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Player> actual = runway.loadAny(Player.class, order, page);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testFindAnyWithOrderAndPageMatchesSkipLimit() {
        for (int i = 0; i < 5; i++) {
            Player player = new Player("Player" + i, i * 10);
            runway.save(player);
        }
        for (int i = 0; i < 5; i++) {
            PointGuard pg = new PointGuard("PG" + i, (i + 5) * 10, i);
            runway.save(pg);
        }

        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN_OR_EQUALS).value(0);
        Order order = Order.by("score").ascending();
        Page page = Page.of(2, 3);

        Set<Player> expected = runway.findAny(Player.class, criteria, order)
                .stream().skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Player> actual = runway.findAny(Player.class, criteria, order,
                page);

        Assert.assertEquals(expected, actual);
    }

}
