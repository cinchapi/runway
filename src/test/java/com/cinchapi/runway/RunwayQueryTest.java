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

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests for {@link Runway} streaming, query operations, derived/computed data
 * queries, and static analysis.
 *
 * @author Jeff Nelson
 */
public class RunwayQueryTest extends AbstractRunwayTest {

    @Test
    public void testStreaminBulkSelect() {
        runway.bulkSelectTimeoutMillis = 0; // force
                                            // streaming
                                            // bulk select
        Set<Manager> expected = Sets.newHashSet();
        Reflection.set("streamingReadBufferSize",
                new java.util.Random().nextInt(10) + 1, runway);
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            Manager manager = new Manager("" + Time.now());
            manager.save();
            expected.add(manager);
        }
        Set<Manager> actual = runway.load(Manager.class);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testStreamingBulkSelectSkipSupport() {
        int bulkSelectTimeoutMillis = runway.bulkSelectTimeoutMillis;
        int streamingReadBufferSize = Reflection.get("streamingReadBufferSize",
                runway);
        try {
            runway.bulkSelectTimeoutMillis = 0; // force
                                                // streaming
                                                // bulk
                                                // select
            Set<Manager> expected = Sets.newLinkedHashSet();
            Reflection.set("streamingReadBufferSize",
                    new java.util.Random().nextInt(10) + 1, runway);
            for (int i = 0; i < Random.getScaleCount(); ++i) {
                Manager manager = new Manager("" + Time.now());
                manager.save();
                expected.add(manager);
            }
            Set<Manager> actual = runway.load(Manager.class);
            Set<Manager> $expected = Sets.newLinkedHashSet();
            int i = 0;
            int skip = expected.size() / 3;
            for (Manager manager : runway.load(Manager.class)) {
                if(i >= skip) {
                    $expected.add(manager);
                }
                ++i;
            }
            actual = actual.stream().skip(skip).collect(Collectors.toSet());
            Assert.assertEquals($expected, actual);
        }
        finally {
            runway.bulkSelectTimeoutMillis = bulkSelectTimeoutMillis;
            Reflection.set("streamingReadBufferSize", streamingReadBufferSize,
                    runway);
        }
    }

    @Test
    public void testFindAnyAndInstantiateBaseClass() {
        Manager user = new Manager("Jeff Nelson");
        user.save();
        User actual = runway.findAnyUnique(User.class, Criteria.where()
                .key("name").operator(Operator.LIKE).value("%Jeff%"));
        Assert.assertEquals(user, actual);
    }

    @Test
    public void testQueryDerivedData() {
        Player a = new Player("a", 30);
        Player b = new Player("b", 15);
        Player c = new Player("c", 20);
        Player d = new Player("d", 100);
        runway.save(a, b, c, d);
        Set<Player> players = runway.find(Player.class, Criteria.where()
                .key("isAllstar").operator(Operator.EQUALS).value(true));
        Assert.assertEquals(ImmutableSet.of(a, d), players);
    }

    @Test
    public void testQueryComputedData() {
        Player a = new Player("a", 30);
        Player b = new Player("b", 15);
        Player c = new Player("c", 20);
        Player d = new Player("d", 100);
        runway.save(a, b, c, d);
        Set<Player> players = runway.find(Player.class, Criteria.where()
                .key("isAboveAverage").operator(Operator.EQUALS).value(true));
        Assert.assertEquals(ImmutableSet.of(d), players);
        players = runway.find(Player.class, Criteria.where()
                .key("isBelowAverage").operator(Operator.EQUALS).value(true));
        Assert.assertEquals(ImmutableSet.of(a, b, c), players);
    }

    @Test
    public void testQueryNavigationKey() {
        Organization cinchapi = new Organization("Cinchapi");
        Organization blavity = new Organization("Blavity");
        Person a = new Person("a", cinchapi);
        Person b = new Person("a", blavity);
        runway.save(cinchapi, blavity, a, b);
        Set<Person> people = runway.find(Person.class,
                Criteria.where().key("organization.name")
                        .operator(Operator.EQUALS).value("Cinchapi"));
        Assert.assertEquals(ImmutableSet.of(a), people);
    }

    @Test
    public void testPreventOutOfSequenceResponse() {
        int bulkSelectTimeoutMillis = runway.bulkSelectTimeoutMillis;
        runway.bulkSelectTimeoutMillis = 1;
        List<Long> ids = Lists.newArrayList();
        try {
            for (int i = 0; i < 10000; ++i) {
                Admin admin = new Admin("Jeff Nelson", "foo");
                admin.save();
                ids.add(admin.id());
            }
            runway.load(Admin.class);
            AtomicBoolean done = new AtomicBoolean(false);
            AtomicBoolean failed = new AtomicBoolean(false);
            long now = System.currentTimeMillis();
            while (!done.get() && System.currentTimeMillis() - now <= 3000) {
                try {
                    runway.load(Admin.class,
                            ids.get(Math.abs(Random.getInt()) % ids.size()));
                }
                catch (Exception e) {
                    e.printStackTrace();
                    done.set(true);
                    failed.set(true);
                }
            }
            Assert.assertFalse(failed.get());
        }
        finally {
            runway.bulkSelectTimeoutMillis = bulkSelectTimeoutMillis;
        }
    }

    @Test
    public void testPreSelectWithDescendantDefinedFields() {
        Toddler child = new Toddler();
        child.name = "A. Nelson";
        child.age = 2;
        Parent parent = new Parent();
        parent.name = "Jeff Nelson";
        parent.child = child;
        child.save();
        parent.save();
        System.out.println(child.id());
        parent = runway.findAnyUnique(Parent.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Jeff Nelson"));
        System.out.println(parent);
        Assert.assertEquals(parent.child.name, "A. Nelson");
    }

    @Test
    public void testLocalConditionEvaluationWithNullValue() {
        Slayer slayer = new Slayer();
        slayer.name = "Jeff Nelson";
        slayer.save();
        Set<Slayer> slayers = runway.find(Slayer.class, Criteria.where()
                .key("isAllStar").operator(Operator.EQUALS).value(true));
        Assert.assertTrue(slayers.isEmpty());
    }

    @Test
    public void testStaticAnalysisHasFieldOfTypeRecordInClass() {
        Assert.assertFalse(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClass(Player.class));
        Assert.assertFalse(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClass(Jock.class));
        Assert.assertTrue(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClass(Person.class));
    }

    @Test
    public void testStaticAnalysisHasFieldOfTypeRecordInClassHierarchy() {
        Assert.assertFalse(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClass(Entity.class));
        Assert.assertFalse(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClass(Human.class));
        Assert.assertTrue(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClass(Parent.class));
        Assert.assertFalse(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClass(NonParent.class));
        Assert.assertTrue(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClassHierarchy(Entity.class));
        Assert.assertTrue(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClassHierarchy(Human.class));
        Assert.assertTrue(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClassHierarchy(Parent.class));
        Assert.assertFalse(Record.StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClassHierarchy(NonParent.class));
    }

}
