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
import java.util.function.Predicate;

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

    /**
     * <strong>Goal:</strong> Verify that {@code load} returns every record the
     * supplier provides.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * three {@link MockAdHocRecord MockAdHocRecords}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code load(MockAdHocRecord.class)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> All three records are returned.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code load} by id returns the record
     * that holds the id.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * two {@link MockAdHocRecord MockAdHocRecords}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code load(MockAdHocRecord.class, alice.id())}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned record is Alice.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code load} by id returns
     * {@code null} when no supplied record holds the id.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * one {@link MockAdHocRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code load(MockAdHocRecord.class, 99999L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testLoadByIdNotFound() {
        Collection<MockAdHocRecord> data = Arrays
                .asList(new MockAdHocRecord("Alice", 30));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        MockAdHocRecord result = db.load(MockAdHocRecord.class, 99999L);

        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code find} returns only the records
     * that match the {@link Criteria}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * three {@link MockAdHocRecord MockAdHocRecords} with ages 30, 25 and 35.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code find} with {@code age > 28}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Two records are returned.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code find} returns matching records
     * sorted by the supplied {@link Order}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * three {@link MockAdHocRecord MockAdHocRecords} in non-sorted order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code find} with {@code age > 0} ordered by {@code age}
     * ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The results iterate as Bob, Alice, Charlie.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code find} honors a {@link Page}
     * limit.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * four {@link MockAdHocRecord MockAdHocRecords} that all match the
     * {@link Criteria}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code find} with {@code age > 0} and a {@link Page} limit of
     * 2.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Two records are returned.
     */
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
        Page page = Page.limit(2);
        Set<MockAdHocRecord> results = db.find(MockAdHocRecord.class, criteria,
                page);

        Assert.assertEquals(2, results.size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUnique} returns the single
     * record that matches the {@link Criteria}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * two {@link MockAdHocRecord MockAdHocRecords} with distinct names.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUnique} with {@code name = "Alice"}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned record is Alice.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code findUnique} returns
     * {@code null} when no record matches the {@link Criteria}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * one {@link MockAdHocRecord} named Alice.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUnique} with {@code name = "Bob"}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code findUnique} throws when more
     * than one record matches the {@link Criteria}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * two {@link MockAdHocRecord MockAdHocRecords} named Alice.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUnique} with {@code name = "Alice"}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} against an
     * {@link AdHocDataSource} returns the record that sorts first under the
     * {@link Order} among the records that match the {@link Criteria}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * three {@link MockAdHocRecord MockAdHocRecords} in non-sorted order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findFirst} with {@code age > 0} ordered by {@code age}
     * ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned record is Bob, whose age of 25 is
     * the lowest.
     */
    @Test
    public void testFindFirstReturnsOrderFirstMatch() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(0).build();
        MockAdHocRecord result = db.findFirst(MockAdHocRecord.class, criteria,
                Order.by("age").ascending());

        Assert.assertNotNull(result);
        Assert.assertEquals("Bob", result.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} against an
     * {@link AdHocDataSource} returns {@code null} when no record matches the
     * {@link Criteria}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * two {@link MockAdHocRecord MockAdHocRecords} whose ages are below the
     * criteria threshold.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findFirst} with {@code age > 100} ordered by {@code age}
     * ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testFindFirstReturnsNullWhenNoMatch() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(100).build();
        MockAdHocRecord result = db.findFirst(MockAdHocRecord.class, criteria,
                Order.by("age").ascending());

        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link AdHocDataSource} applies a
     * client-side {@link Predicate} before the one-row limit, so a rejected
     * order-first row yields the next row that passes instead of {@code null}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * three {@link MockAdHocRecord MockAdHocRecords}, of which the youngest is
     * rejected by the filter.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findFirst} with {@code age > 0} ordered by {@code age}
     * ascending and a filter that rejects Bob, the order-first row.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned record is Alice, the next row
     * under the order that passes the filter.
     */
    @Test
    public void testFindFirstWithFilterSkipsRejectedHeadRow() {
        Collection<MockAdHocRecord> data = Arrays.asList(
                new MockAdHocRecord("Alice", 30),
                new MockAdHocRecord("Bob", 25),
                new MockAdHocRecord("Charlie", 35));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(0).build();
        Predicate<MockAdHocRecord> notBob = record -> !record.name
                .equals("Bob");
        MockAdHocRecord result = db.findFirst(MockAdHocRecord.class, criteria,
                Order.by("age").ascending(), notBob);

        Assert.assertNotNull(result);
        Assert.assertEquals("Alice", result.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code load} for a class the
     * {@link AdHocDataSource} does not serve returns an empty result.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} registered for
     * {@link MockAdHocRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code load(OtherAdHocRecord.class)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is empty.
     */
    @Test
    public void testUnregisteredClassReturnsEmpty() {
        Collection<MockAdHocRecord> data = Arrays
                .asList(new MockAdHocRecord("Alice", 30));
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> data);

        Set<OtherAdHocRecord> results = db.load(OtherAdHocRecord.class);

        Assert.assertTrue(results.isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code loadAny} matches records
     * through a superclass of the served type.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * two {@link MockAdHocRecord MockAdHocRecords}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code loadAny(AdHocRecord.class)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both records are returned.
     */
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

    /**
     * <strong>Goal:</strong> Verify that the supplier is evaluated on each
     * query, so every read observes fresh data.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} whose supplier
     * increments a counter each time it is evaluated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code load(MockAdHocRecord.class)} three times.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The counter reads 3.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code count} returns the number of
     * records the supplier provides.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * three {@link MockAdHocRecord MockAdHocRecords}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code count(MockAdHocRecord.class)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count is 3.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code count} with a {@link Criteria}
     * counts only the matching records.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} that supplies
     * three {@link MockAdHocRecord MockAdHocRecords} with ages 30, 25 and 35.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code count} with {@code age > 28}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count is 2.
     */
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

    /**
     * <strong>Goal:</strong> Verify that an {@link AdHocRecord} cannot be
     * marked for deletion.
     * <p>
     * <strong>Start state:</strong> One {@link MockAdHocRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code deleteOnSave()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testAdHocRecordCannotBeDeleted() {
        MockAdHocRecord record = new MockAdHocRecord("Alice", 30);
        record.deleteOnSave();
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link AdHocRecord} cannot be
     * modified through {@code set}.
     * <p>
     * <strong>Start state:</strong> One {@link MockAdHocRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code set("name", "Bob")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testAdHocRecordCannotBeModified() {
        MockAdHocRecord record = new MockAdHocRecord("Alice", 30);
        record.set("name", "Bob");
    }

    /**
     * <strong>Goal:</strong> Verify that {@code type} returns the
     * {@link AdHocRecord} class this source serves.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} registered for
     * {@link MockAdHocRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code type()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code MockAdHocRecord.class}.
     */
    @Test
    public void testGetRecordClass() {
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> Arrays.asList());

        Assert.assertEquals(MockAdHocRecord.class, db.type());
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link AdHocDataSource} refuses to
     * create a {@link Record}, because it serves the records its supplier
     * provides.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} over an empty
     * supplier.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code create}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testCreateIsUnsupported() {
        AdHocDataSource<MockAdHocRecord> db = new AdHocDataSource<>(
                MockAdHocRecord.class, () -> Arrays.asList());

        db.create(MockAdHocRecord.class);
    }

    /**
     * A mock {@link AdHocRecord} for testing.
     *
     * @author Jeff Nelson
     */
    static class MockAdHocRecord extends AdHocRecord {

        /**
         * The display name.
         */
        String name;

        /**
         * The age.
         */
        int age;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param age the age
         */
        MockAdHocRecord(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    /**
     * Another mock {@link AdHocRecord} for testing unregistered class behavior.
     *
     * @author Jeff Nelson
     */
    static class OtherAdHocRecord extends AdHocRecord {

        /**
         * An arbitrary value.
         */
        String value;
    }

}
