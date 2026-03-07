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

import com.cinchapi.concourse.lang.sort.Order;

/**
 * Tests that verify {@link Runway#loadAny} correctly loads records from the
 * entire class hierarchy, not just the exact class specified. This addresses a
 * bug where the legacy fallback path incorrectly called {@code load()} instead
 * of {@code loadAny()}, causing only the exact class to be loaded.
 *
 * @author Jeff Nelson
 */
public class LoadAnyHierarchyTest extends RunwayBaseClientServerTest {

    @Test
    public void testLoadAnyReturnsSubclasses() {
        Player player = new Player("Regular Player", 15);
        PointGuard pointGuard = new PointGuard("Magic Johnson", 25, 12);

        runway.save(player, pointGuard);

        Set<Player> allPlayers = runway.loadAny(Player.class);

        Assert.assertEquals(2, allPlayers.size());
        Assert.assertTrue(allPlayers.stream()
                .anyMatch(p -> p.name.equals("Regular Player")));
        Assert.assertTrue(allPlayers.stream()
                .anyMatch(p -> p.name.equals("Magic Johnson")));
    }

    @Test
    public void testLoadAnyWithOrderReturnsSubclasses() {
        Player player1 = new Player("Zach", 15);
        Player player2 = new Player("Aaron", 10);
        PointGuard pointGuard = new PointGuard("Magic", 25, 12);

        runway.save(player1, player2, pointGuard);

        Order order = Order.by("name").ascending();
        Set<Player> allPlayers = runway.loadAny(Player.class, order);

        Assert.assertEquals(3, allPlayers.size());

        String[] names = allPlayers.stream().map(p -> p.name)
                .toArray(String[]::new);
        Assert.assertEquals("Aaron", names[0]);
        Assert.assertEquals("Magic", names[1]);
        Assert.assertEquals("Zach", names[2]);
    }

    @Test
    public void testLoadAnyWithPageReturnsSubclasses() {
        Player player1 = new Player("Player1", 15);
        Player player2 = new Player("Player2", 10);
        PointGuard pg1 = new PointGuard("PG1", 25, 12);
        PointGuard pg2 = new PointGuard("PG2", 20, 8);

        runway.save(player1, player2, pg1, pg2);

        com.cinchapi.concourse.lang.paginate.Page page = com.cinchapi.concourse.lang.paginate.Page
                .sized(2);
        Set<Player> pagedPlayers = runway.loadAny(Player.class, page);

        Assert.assertEquals(2, pagedPlayers.size());
    }

    @Test
    public void testLoadAnyWithOrderAndPageReturnsSubclasses() {
        Player player1 = new Player("Zach", 15);
        Player player2 = new Player("Aaron", 10);
        PointGuard pg1 = new PointGuard("Magic", 25, 12);
        PointGuard pg2 = new PointGuard("Isiah", 20, 8);

        runway.save(player1, player2, pg1, pg2);

        Order order = Order.by("name").ascending();
        com.cinchapi.concourse.lang.paginate.Page page = com.cinchapi.concourse.lang.paginate.Page
                .sized(2);
        Set<Player> pagedPlayers = runway.loadAny(Player.class, order, page);

        Assert.assertEquals(2, pagedPlayers.size());

        String[] names = pagedPlayers.stream().map(p -> p.name)
                .toArray(String[]::new);
        Assert.assertEquals("Aaron", names[0]);
        Assert.assertEquals("Isiah", names[1]);
    }

    @Test
    public void testLoadAnyWithRealmsReturnsSubclasses() {
        Player player = new Player("Regular Player", 15);
        player.addRealm("test-realm");

        PointGuard pointGuard = new PointGuard("Magic Johnson", 25, 12);
        pointGuard.addRealm("test-realm");

        Player otherPlayer = new Player("Other Player", 5);
        otherPlayer.addRealm("other-realm");

        runway.save(player, pointGuard, otherPlayer);

        Set<Player> realmPlayers = runway.loadAny(Player.class,
                Realms.only("test-realm"));

        Assert.assertEquals(2, realmPlayers.size());
        Assert.assertTrue(realmPlayers.stream()
                .anyMatch(p -> p.name.equals("Regular Player")));
        Assert.assertTrue(realmPlayers.stream()
                .anyMatch(p -> p.name.equals("Magic Johnson")));
    }
}
