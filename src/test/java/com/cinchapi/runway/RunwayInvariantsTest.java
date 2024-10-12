/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

/**
 * Tests for {@link Runway} invariants
 *
 * @author Jeff Nelson
 */
public class RunwayInvariantsTest extends RunwayBaseClientServerTest {

    @Test(expected = IllegalStateException.class)
    public void testLoadNonExistingRecord() {
        runway.load(Player.class, 1);
    }
    
    @Test
    public void testDeleteMultipleRecordsAtOnce() {
        Player p1 = new Player("LeBron James", 32);
        Player p2 = new Player("Derrick Rose", 26);
        Player p3 = new Player("Michael Jordan", 45);
        Assert.assertTrue(runway.save(p1, p2, p3));
        p1.score = 29;
        p3.score = 63;
        p2.deleteOnSave();
        Assert.assertTrue(runway.save(p1, p2, p3));
        Set<Player> players = runway.load(Player.class);
        Assert.assertTrue(players.contains(p1));
        Assert.assertFalse(players.contains(p2));
        Assert.assertTrue(players.contains(p3));
    }

}
