/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import com.google.common.collect.ImmutableSet;

/**
 * Unit test for the {@link Realms} feature support in {@link Runway}.
 *
 * @author Jeff Nelson
 */
public class RunwayRealmsTest extends RunwayBaseClientServerTest {

    @Test
    public void testDefaultLoadIsRealmAgnostic() {
        Player a = new Player("a", 1);
        Player b = new Player("b", 2);
        Player c = new Player("c", 3);
        a.addRealm("test");
        c.addRealm("test");
        runway.save(a, b, c);
        Set<Player> records = runway.load(Player.class);
        Assert.assertEquals(ImmutableSet.of(a, b, c), records);
    }

    @Test
    public void testLoadRecordsInSingleRealm() {
        Player a = new Player("a", 1);
        Player b = new Player("b", 2);
        Player c = new Player("c", 3);
        a.addRealm("test");
        c.addRealm("test");
        runway.save(a, b, c);
        Set<Player> records = runway.load(Player.class, Realms.only("test"));
        Assert.assertEquals(ImmutableSet.of(a, c), records);
    }

    @Test
    public void testLoadRecordsInMultipleRealms() {
        Player a = new Player("a", 1);
        Player b = new Player("b", 2);
        Player c = new Player("c", 3);
        Player d = new Player("d", 4);
        a.addRealm("test");
        c.addRealm("test");
        b.addRealm("prod");
        runway.save(a, b, c, d);
        Set<Player> records = runway.load(Player.class,
                Realms.anyOf("test", "prod"));
        Assert.assertEquals(ImmutableSet.of(a, b, c), records);
    }

    @Test
    public void testLoadRecordsFromEmptyRealm() {
        Player a = new Player("a", 1);
        Player b = new Player("b", 2);
        Player c = new Player("c", 3);
        Player d = new Player("d", 4);
        a.addRealm("test");
        c.addRealm("test");
        b.addRealm("test");
        runway.save(a, b, c, d);
        Set<Player> records = runway.load(Player.class, Realms.only("prod"));
        Assert.assertEquals(ImmutableSet.of(), records);
    }

    @Test
    public void testLoadRecordFromWrongRealm() {
        Player a = new Player("a", 1);
        a.addRealm("test");
        a.save();
        a = runway.load(Player.class, a.id(), Realms.only("prod"));
        Assert.assertNull(a);
    }

    @Test
    public void testLoadRecordFromCorrectRealm() {
        Player a = new Player("a", 1);
        a.addRealm("test");
        a.save();
        a = runway.load(Player.class, a.id(), Realms.only("test"));
        Assert.assertNotNull(a);
    }

    @Test
    public void testFindUniqueFromRealm() {

    }

    @Test
    public void testFindUniqueFromRealmDuplicateInDifferentRealm() {

    }

    @Test
    public void testFindAnyUniqueFromRealm() {

    }

    @Test
    public void testFindAnyUniqueFromRealmDuplicateInDifferentRealm() {

    }

    // findAny* tests

    // find* tests
    
    // count tests
    
    // TODO: need to test legacy paths...

}
