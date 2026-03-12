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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for {@link Runway} lifecycle hooks, failure handlers, and multi-set
 * persistence.
 *
 * @author Jeff Nelson
 */
public class RunwayLifecycleTest extends AbstractRunwayTest {

    @Test
    public void testOnLoadSimulateUpgradeTask() {
        Student stud = new Student();
        stud.ccat = 20.0f;
        stud.save();
        Assert.assertTrue(stud.scores.isEmpty());
        stud = runway.load(Student.class, stud.id());
        Assert.assertFalse(stud.scores.isEmpty());
        stud.save();
        stud = runway.load(Student.class, stud.id());
        Assert.assertEquals(1, stud.scores.size());
        System.out.println(stud);
    }

    @Test
    public void testLoadFailureHandlerWhenLoadingMany() throws Exception {
        runway.close();
        AtomicBoolean passed = new AtomicBoolean(false);
        runway = Runway.builder().port(server.getClientPort())
                .onLoadFailure((clazz, record, error) -> {
                    passed.set(true);
                    System.out.println(AnyStrings.format(
                            "Error when loading {} from {}: {}", record, clazz,
                            error.getMessage()));
                }).build();
        Adult a = new Adult("Jeff Nelson", "jeff@email.com");
        a.save();
        Adult b = new Adult("Ashleah Nelson", "ashleah@email.com");
        b.save();
        client.clear("name", a.id());
        try {
            Set<Adult> adults = runway.load(Adult.class);
            adults.forEach(adult -> {
                System.out.println(adult); // force the
                                           // record to be
                                           // loaded
            });
            Assert.fail();
        }
        catch (NullPointerException e) {
            Assert.assertTrue(passed.get());
        }
    }

    @Test
    public void testLoadFailureHandlerWhenFinding() throws Exception {
        runway.close();
        AtomicBoolean passed = new AtomicBoolean(false);
        runway = Runway.builder().port(server.getClientPort())
                .onLoadFailure((clazz, record, error) -> {
                    passed.set(true);
                    System.out.println(AnyStrings.format(
                            "Error when loading {} from {}: {}", record, clazz,
                            error.getMessage()));
                }).build();
        Adult a = new Adult("Jeff Nelson", "jeff@email.com");
        a.save();
        Adult b = new Adult("Ashleah Nelson", "ashleah@email.com");
        b.save();
        client.clear("name", a.id());
        try {
            Set<Adult> adults = runway.find(Adult.class, Criteria.where()
                    .key("email").operator(Operator.LIKE).value("%email.com%"));
            adults.forEach(adult -> {
                System.out.println(adult); // force the
                                           // record to be
                                           // loaded
            });
            Assert.fail();
        }
        catch (NullPointerException e) {
            Assert.assertTrue(passed.get());
        }
    }

    @Test
    public void testLoadFailureHandlerWhenLoadingSingle() throws Exception {
        runway.close();
        AtomicBoolean passed = new AtomicBoolean(false);
        runway = Runway.builder().port(server.getClientPort())
                .onLoadFailure((clazz, record, error) -> {
                    passed.set(true);
                    System.out.println(AnyStrings.format(
                            "Error when loading {} from {}: {}", record, clazz,
                            error.getMessage()));
                }).build();
        Adult a = new Adult("Jeff Nelson", "jeff@email.com");
        a.save();
        Adult b = new Adult("Ashleah Nelson", "ashleah@email.com");
        b.save();
        client.clear("name", a.id());
        try {
            runway.load(Adult.class, a.id());
            Assert.fail();
        }
        catch (NullPointerException e) {
            Assert.assertTrue(passed.get());
        }
    }

    @Test
    public void testMultiSetSaveAndLoadById() {
        Player record = new Player("Jeff Nelson", 36);
        record.set(ImmutableMap.of("name", "John Doe"));
        record.save();
        Player loaded = runway.load(Player.class, record.id());
        Assert.assertEquals("John Doe", loaded.get("name"));
    }

    @Test
    public void testMultiSetSaveAndFindByCriteria() {
        Player record1 = new Player("Jeff Nelson", 36);
        record1.set(ImmutableMap.of("name", "John Doe", "age", 25));
        Player record2 = new Player("Jeff Nelson", 36);
        record2.set(ImmutableMap.of("name", "Jane Smith", "age", 30));
        record1.save();
        record2.save();
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("John Doe");
        Set<Player> records = runway.find(Player.class, criteria);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals("John Doe", records.iterator().next().get("name"));
        Assert.assertTrue(runway
                .find(Player.class, Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("Jeff Nelson"))
                .isEmpty());
    }

}
