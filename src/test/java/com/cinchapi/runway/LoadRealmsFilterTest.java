/*
 * Copyright (c) 2013-2026 Cinchapi Inc.
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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.sort.Order;

/**
 * Tests that verify realm filtering is correctly applied across all load
 * method variants. This addresses a bug where {@code load(order, realms)}
 * in the legacy path was missing the realms filter.
 *
 * @author Jeff Nelson
 */
public class LoadRealmsFilterTest extends RunwayBaseClientServerTest {

    @Test
    public void testLoadWithRealmsFiltersCorrectly() {
        Player player1 = new Player("Player1", 10);
        player1.addRealm("realm-a");

        Player player2 = new Player("Player2", 20);
        player2.addRealm("realm-b");

        Player player3 = new Player("Player3", 30);
        player3.addRealm("realm-a");

        runway.save(player1, player2, player3);

        Set<Player> realmAPlayers = runway.load(Player.class,
                Realms.only("realm-a"));

        Assert.assertEquals(2, realmAPlayers.size());
        Assert.assertTrue(realmAPlayers.stream()
                .allMatch(p -> p.realms().contains("realm-a")));
    }

    @Test
    public void testLoadWithOrderAndRealmsFiltersCorrectly() {
        Player player1 = new Player("Zach", 10);
        player1.addRealm("realm-a");

        Player player2 = new Player("Aaron", 20);
        player2.addRealm("realm-b");

        Player player3 = new Player("Mike", 30);
        player3.addRealm("realm-a");

        runway.save(player1, player2, player3);

        Order order = Order.by("name").ascending();
        Set<Player> realmAPlayers = runway.load(Player.class, order,
                Realms.only("realm-a"));

        Assert.assertEquals(2, realmAPlayers.size());
        Assert.assertTrue(realmAPlayers.stream()
                .allMatch(p -> p.realms().contains("realm-a")));

        String[] names = realmAPlayers.stream()
                .map(p -> p.name)
                .toArray(String[]::new);
        Assert.assertEquals("Mike", names[0]);
        Assert.assertEquals("Zach", names[1]);
    }

    @Test
    public void testLoadAnyWithRealmsFiltersCorrectly() {
        Player player = new Player("Player", 10);
        player.addRealm("realm-a");

        PointGuard pg1 = new PointGuard("PG1", 20, 5);
        pg1.addRealm("realm-b");

        PointGuard pg2 = new PointGuard("PG2", 30, 8);
        pg2.addRealm("realm-a");

        runway.save(player, pg1, pg2);

        Set<Player> realmAPlayers = runway.loadAny(Player.class,
                Realms.only("realm-a"));

        Assert.assertEquals(2, realmAPlayers.size());
        Assert.assertTrue(realmAPlayers.stream()
                .allMatch(p -> p.realms().contains("realm-a")));
    }

    @Test
    public void testLoadAnyWithOrderAndRealmsFiltersCorrectly() {
        Player player = new Player("Zach", 10);
        player.addRealm("realm-a");

        PointGuard pg1 = new PointGuard("Aaron", 20, 5);
        pg1.addRealm("realm-b");

        PointGuard pg2 = new PointGuard("Mike", 30, 8);
        pg2.addRealm("realm-a");

        runway.save(player, pg1, pg2);

        Order order = Order.by("name").ascending();
        Set<Player> realmAPlayers = runway.loadAny(Player.class, order,
                Realms.only("realm-a"));

        Assert.assertEquals(2, realmAPlayers.size());
        Assert.assertTrue(realmAPlayers.stream()
                .allMatch(p -> p.realms().contains("realm-a")));

        String[] names = realmAPlayers.stream()
                .map(p -> p.name)
                .toArray(String[]::new);
        Assert.assertEquals("Mike", names[0]);
        Assert.assertEquals("Zach", names[1]);
    }

    @Test
    public void testLoadWithMultipleRealms() {
        Player player1 = new Player("Player1", 10);
        player1.addRealm("realm-a");

        Player player2 = new Player("Player2", 20);
        player2.addRealm("realm-b");

        Player player3 = new Player("Player3", 30);
        player3.addRealm("realm-c");

        runway.save(player1, player2, player3);

        Set<Player> multiRealmPlayers = runway.load(Player.class,
                Realms.anyOf("realm-a", "realm-c"));

        Assert.assertEquals(2, multiRealmPlayers.size());
        Assert.assertTrue(multiRealmPlayers.stream()
                .anyMatch(p -> p.name.equals("Player1")));
        Assert.assertTrue(multiRealmPlayers.stream()
                .anyMatch(p -> p.name.equals("Player3")));
    }

    @Test
    public void testLoadWithOrderPageAndRealms() {
        for (int i = 0; i < 10; i++) {
            Player player = new Player("Player" + i, i * 10);
            player.addRealm(i % 2 == 0 ? "realm-even" : "realm-odd");
            runway.save(player);
        }

        Order order = Order.by("score").ascending();
        com.cinchapi.concourse.lang.paginate.Page page =
                com.cinchapi.concourse.lang.paginate.Page.sized(3);

        Set<Player> evenPlayers = runway.load(Player.class, order, page,
                Realms.only("realm-even"));

        Assert.assertEquals(3, evenPlayers.size());
        Assert.assertTrue(evenPlayers.stream()
                .allMatch(p -> p.realms().contains("realm-even")));
    }
}

