/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link FederatedRunway}.
 *
 * @author Jeff Nelson
 */
public class FederatedRunwayTest extends ClientServerTest {

    private Runway runway;

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

    @Override
    public void beforeEachTest() {
        runway = Runway.builder().port(server.getClientPort()).build();
    }

    @Override
    public void afterEachTest() {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    @Test
    public void testRoutesToDefaultForUnregisteredClass() {
        MockRecord record = new MockRecord();
        record.name = "Test";
        runway.save(record);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .build();

        Set<MockRecord> results = db.load(MockRecord.class);

        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testRoutesToRegisteredAdHocDatabase() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25));
        AdHocDatabase<MockAdHocRecord> adhocDb = new AdHocDatabase<>(
                MockAdHocRecord.class, () -> data);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .register(adhocDb)
                .build();

        Set<MockAdHocRecord> results = db.load(MockAdHocRecord.class);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testFederatesMultipleAdHocDatabases() {
        Collection<MockAdHocRecord> data1 = Arrays
                .asList(new MockAdHocRecord("Alice", 30));
        Collection<OtherAdHocRecord> data2 = Arrays
                .asList(new OtherAdHocRecord("Value1"),
                        new OtherAdHocRecord("Value2"));

        AdHocDatabase<MockAdHocRecord> adhocDb1 = new AdHocDatabase<>(
                MockAdHocRecord.class, () -> data1);
        AdHocDatabase<OtherAdHocRecord> adhocDb2 = new AdHocDatabase<>(
                OtherAdHocRecord.class, () -> data2);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .register(adhocDb1)
                .register(adhocDb2)
                .build();

        Assert.assertEquals(1, db.load(MockAdHocRecord.class).size());
        Assert.assertEquals(2, db.load(OtherAdHocRecord.class).size());
    }

    @Test
    public void testFindRoutesToCorrectDatabase() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25));
        AdHocDatabase<MockAdHocRecord> adhocDb = new AdHocDatabase<>(
                MockAdHocRecord.class, () -> data);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .register(adhocDb)
                .build();

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(26).build();
        Set<MockAdHocRecord> results = db.find(MockAdHocRecord.class, criteria);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Alice",
                results.iterator().next().name);
    }

    @Test
    public void testLoadAnyRoutesToAdHocDatabaseForSuperclass() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25));
        AdHocDatabase<MockAdHocRecord> adhocDb = new AdHocDatabase<>(
                MockAdHocRecord.class, () -> data);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .register(adhocDb)
                .build();

        Set<AdHocRecord> results = db.loadAny(AdHocRecord.class);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testPersistentAndAdHocCoexist() {
        // Save persistent record
        MockRecord persistent = new MockRecord();
        persistent.name = "Persistent";
        runway.save(persistent);

        // Create ad-hoc data
        Collection<MockAdHocRecord> adhocData = Arrays.asList(
                new MockAdHocRecord("AdHoc1", 10),
                new MockAdHocRecord("AdHoc2", 20));
        AdHocDatabase<MockAdHocRecord> adhocDb = new AdHocDatabase<>(
                MockAdHocRecord.class, () -> adhocData);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .register(adhocDb)
                .build();

        // Both work through same interface
        Assert.assertEquals(1, db.load(MockRecord.class).size());
        Assert.assertEquals(2, db.load(MockAdHocRecord.class).size());
    }

    @Test
    public void testLoadByIdRoutesToCorrectDatabase() {
        MockAdHocRecord alice = new MockAdHocRecord("Alice", 30);
        Collection<MockAdHocRecord> data = Arrays.asList(alice);
        AdHocDatabase<MockAdHocRecord> adhocDb = new AdHocDatabase<>(
                MockAdHocRecord.class, () -> data);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .register(adhocDb)
                .build();

        MockAdHocRecord result = db.load(MockAdHocRecord.class, alice.id());

        Assert.assertNotNull(result);
        Assert.assertEquals("Alice", result.name);
    }

    @Test
    public void testCountRoutesToCorrectDatabase() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDatabase<MockAdHocRecord> adhocDb = new AdHocDatabase<>(
                MockAdHocRecord.class, () -> data);

        FederatedRunway db = FederatedRunway.builder()
                .defaultTo(runway)
                .register(adhocDb)
                .build();

        Assert.assertEquals(3, db.count(MockAdHocRecord.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderRequiresDefaultRunway() {
        FederatedRunway.builder().build();
    }

    /**
     * A persistent mock record for testing.
     */
    static class MockRecord extends Record {

        String name;
    }

    /**
     * A mock {@link AdHocRecord} for testing.
     */
    static class MockAdHocRecord extends AdHocRecord {

        String name;
        int age;

        MockAdHocRecord(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    /**
     * Another mock {@link AdHocRecord} for testing.
     */
    static class OtherAdHocRecord extends AdHocRecord {

        String value;

        OtherAdHocRecord(String value) {
            this.value = value;
        }
    }

}

