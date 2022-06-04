/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;

public class RunwayPaginationTest extends RunwayBaseClientServerTest {

    @Test
    public void testLoadPaginateWithFilter() {
        for (int i = 0; i < 100; ++i) {
            String name = Random.getSimpleString();
            int score = Random.getPositiveNumber().intValue() % 100;
            Player player = new Player(name, score);
            player.save();
        }
        Page page = Page.of(2, 5);
        Predicate<Player> filter = player -> player.get("isAllstar");
        Set<Player> expected = runway.load(Player.class, Order.by("name"))
                .stream().filter(filter).skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Player> actual = runway.load(Player.class, Order.by("name"), page,
                filter);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testFindLocalCriteriaResolutionPagination() {
        for (int i = 0; i < 100; ++i) {
            String name = Random.getSimpleString();
            int score = Random.getPositiveNumber().intValue() % 100;
            Player player = new Player(name, score);
            player.save();
        }
        Page page = Page.of(2, 5);
        Criteria condition = Criteria.where().key("isAllstar")
                .operator(Operator.EQUALS).value(true);
        Set<Player> expected = runway
                .find(Player.class, condition, Order.by("name")).stream()
                .skip(page.skip()).limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Player> actual = runway.find(Player.class, condition,
                Order.by("name"), page);
        Assert.assertEquals(expected, actual);
    }

}
