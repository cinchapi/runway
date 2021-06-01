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

import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
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
        Player a = new Player("a", 1);
        a.addRealm("test");
        a.save();
        a = runway.findUnique(
                Player.class, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("a"),
                Realms.only("test"));
        Assert.assertNotNull(a);
    }

    @Test
    public void testFindUniqueFromRealmImplicit() {
        Player a = new Player("a", 1);
        a.addRealm("test");
        a.save();
        a = runway.findUnique(Player.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("a"));
        Assert.assertNotNull(a);
    }

    @Test(expected = DuplicateEntryException.class)
    public void testFindUniqueFromRealmImplicitConflict() {
        Player a1 = new Player("a", 1);
        Player a2 = new Player("a", 1);
        a1.addRealm("test");
        a2.addRealm("fest");
        a1.save();
        a2.save();
        runway.findUnique(Player.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("a"));
    }

    @Test
    public void testFindUniqueFromRealmNotExists() {
        Player a = new Player("a", 1);
        Player b = new Player("b", 1);
        a.addRealm("test");
        b.addRealm("fest");
        runway.save(a, a);
        a = runway.findUnique(
                Player.class, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("a"),
                Realms.only("fest"));
        Assert.assertNull(a);
    }

    @Test
    public void testFindUniqueFromRealmDuplicateInDifferentRealm() {
        Player a1 = new Player("a", 1);
        Player a2 = new Player("a", 1);
        a1.addRealm("test");
        a2.addRealm("fest");
        runway.save(a1, a2);
        a1 = runway.findUnique(
                Player.class, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("a"),
                Realms.only("test"));
        Assert.assertNotNull(a1);
    }

    @Test(expected = DuplicateEntryException.class)
    public void testFindUniqueFromRealmDuplicateInDifferentRealmConflict() {
        Player a1 = new Player("a", 1);
        Player a2 = new Player("a", 1);
        a1.addRealm("test");
        a2.addRealm("fest");
        runway.save(a1, a2);
        a1 = runway.findUnique(Player.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("a"), Realms.all());
        Assert.assertNotNull(a1);
    }

    @Test
    public void testFindAnyUniqueFromRealm() {
        PointGuard pg = new PointGuard("a", 1, 1);
        pg.addRealm("test");
        Player a = new Player("a", 1);
        a.addRealm("fest");
        pg.save();
        a.save();
        a = runway.findAnyUnique(
                Player.class, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("a"),
                Realms.anyOf("test"));
        Assert.assertNotNull(a);
        a = runway.findAnyUnique(
                Player.class, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("a"),
                Realms.anyOf("fest"));
        Assert.assertNotNull(a);
    }
    
    @Test(expected = DuplicateEntryException.class)
    public void testFindAnyUniqueFromRealmConflict() {
        PointGuard pg = new PointGuard("a", 1, 1);
        pg.addRealm("test");
        Player a = new Player("a", 1);
        a.addRealm("fest");
        pg.save();
        a.save();
        a = runway.findAnyUnique(
                Player.class, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("a"),
                Realms.all());
    }

    // findAny* tests

    // find* tests

    // count tests

    // TODO: need to test legacy paths...

}
