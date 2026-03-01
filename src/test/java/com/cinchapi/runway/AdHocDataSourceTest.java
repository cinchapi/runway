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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link AdHocDataSource}.
 *
 * @author Jeff Nelson
 */
public class AdHocDataSourceTest {

    @Test
    public void testLoadAllRecords() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Set<MockAdHocRecord> results = db.load(MockAdHocRecord.class);

        Assert.assertEquals(3, results.size());
    }

    @Test
    public void testLoadById() {
        MockAdHocRecord alice = new MockAdHocRecord("Alice", 30);
        MockAdHocRecord bob = new MockAdHocRecord("Bob", 25);
        Collection<MockAdHocRecord> data = Arrays.asList(alice, bob);
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        MockAdHocRecord result = db.load(MockAdHocRecord.class, alice.id());

        Assert.assertNotNull(result);
        Assert.assertEquals("Alice", result.name);
    }

    @Test
    public void testLoadByIdNotFound() {
        Collection<MockAdHocRecord> data = Arrays
                .asList(new MockAdHocRecord("Alice", 30));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        MockAdHocRecord result = db.load(MockAdHocRecord.class, 99999L);

        Assert.assertNull(result);
    }

    @Test
    public void testFindWithCriteria() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(28).build();
        Set<MockAdHocRecord> results = db.find(MockAdHocRecord.class, criteria);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testFindWithOrder() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(0).build();
        Order order = Order.by("age").ascending();
        Set<MockAdHocRecord> results = db.find(MockAdHocRecord.class, criteria,
                order);

        MockAdHocRecord[] arr = results.toArray(new MockAdHocRecord[0]);
        Assert.assertEquals("Bob", arr[0].name);
        Assert.assertEquals("Alice", arr[1].name);
        Assert.assertEquals("Charlie", arr[2].name);
    }

    @Test
    public void testFindWithPagination() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35),
                new MockAdHocRecord("Diana", 28));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(0).build();
        Page page = Page.sized(2);
        Set<MockAdHocRecord> results = db.find(MockAdHocRecord.class, criteria,
                page);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testFindUnique() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Alice").build();
        MockAdHocRecord result = db.findUnique(MockAdHocRecord.class, criteria);

        Assert.assertNotNull(result);
        Assert.assertEquals("Alice", result.name);
    }

    @Test
    public void testFindUniqueNotFound() {
        Collection<MockAdHocRecord> data = Arrays
                .asList(new MockAdHocRecord("Alice", 30));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Bob").build();
        MockAdHocRecord result = db.findUnique(MockAdHocRecord.class, criteria);

        Assert.assertNull(result);
    }

    @Test(expected = DuplicateEntryException.class)
    public void testFindUniqueThrowsOnDuplicate() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Alice", 25));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Alice").build();
        db.findUnique(MockAdHocRecord.class, criteria);
    }

    @Test
    public void testUnregisteredClassReturnsEmpty() {
        Collection<MockAdHocRecord> data = Arrays
                .asList(new MockAdHocRecord("Alice", 30));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Set<OtherAdHocRecord> results = db.load(OtherAdHocRecord.class);

        Assert.assertTrue(results.isEmpty());
    }

    @Test
    public void testLoadAnyWithSuperclass() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Set<AdHocRecord> results = db.loadAny(AdHocRecord.class);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testSupplierIsEvaluatedOnEachQuery() {
        AtomicInteger counter = new AtomicInteger(0);
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> {
                    counter.incrementAndGet();
                    return Arrays.asList(new MockAdHocRecord("Alice", 30));
                });

        db.load(MockAdHocRecord.class);
        db.load(MockAdHocRecord.class);
        db.load(MockAdHocRecord.class);

        Assert.assertEquals(3, counter.get());
    }

    @Test
    public void testCount() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        int count = db.count(MockAdHocRecord.class);

        Assert.assertEquals(3, count);
    }

    @Test
    public void testCountWithCriteria() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(28).build();
        int count = db.count(MockAdHocRecord.class, criteria);

        Assert.assertEquals(2, count);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdHocRecordCannotBeDeleted() {
        MockAdHocRecord record = new MockAdHocRecord("Alice", 30);
        record.deleteOnSave();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdHocRecordCannotBeModified() {
        MockAdHocRecord record = new MockAdHocRecord("Alice", 30);
        record.set("name", "Bob");
    }

    @Test
    public void testGetRecordClass() {
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> Arrays.asList());

        Assert.assertEquals(MockAdHocRecord.class, db.type());
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
     * Another mock {@link AdHocRecord} for testing unregistered class behavior.
     */
    static class OtherAdHocRecord extends AdHocRecord {

        String value;
    }

}
