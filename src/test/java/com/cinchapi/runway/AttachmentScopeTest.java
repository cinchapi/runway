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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link Runway#attach(AdHocDataSource...)} and
 * {@link Runway#detach(AdHocDataSource)} functionality, including thread
 * isolation guarantees.
 *
 * @author Jeff Nelson
 */
public class AttachmentScopeTest extends RunwayBaseClientServerTest {

    // ========================================================================
    // Basic Attach/Detach Tests
    // ========================================================================

    @Test
    public void testAttachReturnsScopeThatServesAdHocRecords() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Set<TestAdHocRecord> results = scope.load(TestAdHocRecord.class);
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testOriginalRunwayHandleServesAttachedSources() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            // Query via original runway handle, not scope
            Set<TestAdHocRecord> results = runway.load(TestAdHocRecord.class);
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testBothHandlesReturnSameResults() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25),
                new TestAdHocRecord("Charlie", 35));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        Criteria criteria = Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(28).build();

        try (AttachmentScope scope = runway.attach(source)) {
            Set<TestAdHocRecord> scopeResults = scope
                    .find(TestAdHocRecord.class, criteria);
            Set<TestAdHocRecord> runwayResults = runway
                    .find(TestAdHocRecord.class, criteria);

            Assert.assertEquals(scopeResults.size(), runwayResults.size());
            Assert.assertEquals(2, scopeResults.size());
        }
    }

    @Test
    public void testAutoDetachOnClose() {
        Collection<TestAdHocRecord> data = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Assert.assertEquals(1, runway.load(TestAdHocRecord.class).size());
        }

        // After close, source should be detached
        // Since TestAdHocRecord isn't in the database, load should return empty
        Set<TestAdHocRecord> results = runway.load(TestAdHocRecord.class);
        Assert.assertTrue(results.isEmpty());
    }

    @Test
    public void testManualDetach() {
        Collection<TestAdHocRecord> data = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        AttachmentScope scope = runway.attach(source);

        // Verify attached
        Assert.assertEquals(1, runway.load(TestAdHocRecord.class).size());

        // Manual detach
        runway.detach(source);

        // Verify detached
        Assert.assertTrue(runway.load(TestAdHocRecord.class).isEmpty());

        scope.close(); // Clean up
    }

    @Test
    public void testDetachByClass() {
        Collection<TestAdHocRecord> data = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        AttachmentScope scope = runway.attach(source);

        // Detach by class
        runway.detach(TestAdHocRecord.class);

        // Verify detached
        Assert.assertTrue(runway.load(TestAdHocRecord.class).isEmpty());

        scope.close();
    }

    @Test
    public void testMultipleSources() {
        Collection<TestAdHocRecord> data1 = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        Collection<OtherAdHocRecord> data2 = Arrays
                .asList(new OtherAdHocRecord("Report1"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data1);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> data2);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            Assert.assertEquals(1, runway.load(TestAdHocRecord.class).size());
            Assert.assertEquals(1, runway.load(OtherAdHocRecord.class).size());
        }

        // Both detached on close
        Assert.assertTrue(runway.load(TestAdHocRecord.class).isEmpty());
        Assert.assertTrue(runway.load(OtherAdHocRecord.class).isEmpty());
    }

    // ========================================================================
    // Persistent Record Integration Tests
    // ========================================================================

    @Test
    public void testAttachedSourceDoesNotAffectPersistentRecords() {
        // Create a persistent record
        Person person = new Person();
        person.name = "TestPerson";
        person.age = 42;
        person.save();

        // Attach ad-hoc source
        Collection<TestAdHocRecord> adhocData = Arrays
                .asList(new TestAdHocRecord("AdHoc", 99));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> adhocData);

        try (AttachmentScope scope = runway.attach(source)) {
            // Query for persistent records should still work
            Set<Person> people = runway.load(Person.class);
            Assert.assertFalse(people.isEmpty());

            // Query for ad-hoc records also works
            Set<TestAdHocRecord> adhocs = runway.load(TestAdHocRecord.class);
            Assert.assertEquals(1, adhocs.size());
        }
    }

    // ========================================================================
    // Thread Isolation Tests
    // ========================================================================

    @Test
    public void testAttachedSourceIsThreadLocal() throws Exception {
        Collection<TestAdHocRecord> data = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Integer> otherThreadResult = new AtomicReference<>();

        AttachmentScope scope = runway.attach(source);

        // Verify main thread sees the data
        Assert.assertEquals(1, runway.load(TestAdHocRecord.class).size());

        // Start another thread
        Thread otherThread = new Thread(() -> {
            // This thread should NOT see the attached source
            otherThreadResult.set(runway.load(TestAdHocRecord.class).size());
            latch.countDown();
        });
        otherThread.start();

        // Wait for other thread
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Other thread should see empty result (source not attached)
        Assert.assertEquals(Integer.valueOf(0), otherThreadResult.get());

        scope.close();
    }

    @Test
    public void testDifferentThreadsCanHaveDifferentSources() throws Exception {
        Collection<TestAdHocRecord> data1 = Arrays
                .asList(new TestAdHocRecord("Main", 1));
        Collection<TestAdHocRecord> data2 = Arrays.asList(
                new TestAdHocRecord("Other1", 2),
                new TestAdHocRecord("Other2", 3));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data1);
        AdHocDataSource<TestAdHocRecord> source2 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data2);

        CountDownLatch threadReady = new CountDownLatch(1);
        CountDownLatch mainDone = new CountDownLatch(1);
        AtomicReference<Integer> otherThreadResult = new AtomicReference<>();

        // Start other thread first
        Thread otherThread = new Thread(() -> {
            try (AttachmentScope scope = runway.attach(source2)) {
                threadReady.countDown();
                // Wait for main thread to verify
                mainDone.await(5, TimeUnit.SECONDS);
                otherThreadResult
                        .set(runway.load(TestAdHocRecord.class).size());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        otherThread.start();

        // Wait for other thread to attach
        Assert.assertTrue(threadReady.await(5, TimeUnit.SECONDS));

        // Main thread attaches different source
        try (AttachmentScope scope = runway.attach(source1)) {
            // Main thread sees its own data (1 record)
            Assert.assertEquals(1, runway.load(TestAdHocRecord.class).size());
            mainDone.countDown();
        }

        // Wait for other thread
        otherThread.join(5000);

        // Other thread saw its own data (2 records)
        Assert.assertEquals(Integer.valueOf(2), otherThreadResult.get());
    }

    @Test
    public void testDetachOnOneThreadDoesNotAffectAnother() throws Exception {
        Collection<TestAdHocRecord> data = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        CountDownLatch attached = new CountDownLatch(1);
        CountDownLatch detached = new CountDownLatch(1);
        CountDownLatch checkDone = new CountDownLatch(1);
        AtomicReference<Integer> afterDetachResult = new AtomicReference<>();

        // Other thread attaches
        Thread otherThread = new Thread(() -> {
            try (AttachmentScope scope = runway.attach(source)) {
                attached.countDown();
                // Wait for main thread to verify, then detach its own source
                detached.await(5, TimeUnit.SECONDS);
                // After main thread detached its copy, verify our copy still
                // works
                afterDetachResult
                        .set(runway.load(TestAdHocRecord.class).size());
                checkDone.countDown();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        otherThread.start();

        // Wait for attachment
        Assert.assertTrue(attached.await(5, TimeUnit.SECONDS));

        // Main thread also attaches (separate thread-local copy)
        AttachmentScope mainScope = runway.attach(source);
        Assert.assertEquals(1, runway.load(TestAdHocRecord.class).size());

        // Main thread detaches
        mainScope.close();
        Assert.assertTrue(runway.load(TestAdHocRecord.class).isEmpty());

        // Signal other thread
        detached.countDown();

        // Wait for check
        Assert.assertTrue(checkDone.await(5, TimeUnit.SECONDS));

        // Other thread's source should still be attached (1 result)
        Assert.assertEquals(Integer.valueOf(1), afterDetachResult.get());

        otherThread.join(5000);
    }

    // ========================================================================
    // Query Method Coverage Tests
    // ========================================================================

    @Test
    public void testFindWithCriteria() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25),
                new TestAdHocRecord("Charlie", 35));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Criteria criteria = Criteria.where().key("age")
                    .operator(Operator.GREATER_THAN).value(28).build();
            Set<TestAdHocRecord> results = scope.find(TestAdHocRecord.class,
                    criteria);
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testFindUnique() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Criteria criteria = Criteria.where().key("name")
                    .operator(Operator.EQUALS).value("Alice").build();
            TestAdHocRecord result = runway.findUnique(TestAdHocRecord.class,
                    criteria);
            Assert.assertNotNull(result);
            Assert.assertEquals("Alice", result.name);
        }
    }

    @Test
    public void testLoadAnyWithHierarchy() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Set<AdHocRecord> results = runway.loadAny(AdHocRecord.class);
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testCountWithAttachedSource() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25),
                new TestAdHocRecord("Charlie", 35));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            int count = runway.count(TestAdHocRecord.class);
            Assert.assertEquals(3, count);
        }
    }

    @Test
    public void testCountAnyWithHierarchy() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            int count = runway.countAny(AdHocRecord.class);
            Assert.assertEquals(2, count);
        }
    }

    // ========================================================================
    // Multiple Sources in Same Hierarchy Tests
    // ========================================================================

    @Test
    public void testLoadAnyWithMultipleSourcesInHierarchy() {
        // Two different AdHocRecord subclasses
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<OtherAdHocRecord> otherData = Arrays.asList(
                new OtherAdHocRecord("Report1"),
                new OtherAdHocRecord("Report2"),
                new OtherAdHocRecord("Report3"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        // Attach both sources
        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // loadAny(AdHocRecord.class) should return records from BOTH
            // sources
            Set<AdHocRecord> results = runway.loadAny(AdHocRecord.class);
            Assert.assertEquals(5, results.size());
        }
    }

    @Test
    public void testFindAnyWithMultipleSourcesInHierarchy() {
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<OtherAdHocRecord> otherData = Arrays.asList(
                new OtherAdHocRecord("Alpha"), new OtherAdHocRecord("Beta"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // Find with a criteria that matches across both types
            // Using a field that exists in both (name/value starting with 'A')
            Criteria criteria = Criteria.where().key("name")
                    .operator(Operator.EQUALS).value("Alice").build();
            Set<AdHocRecord> results = runway.findAny(AdHocRecord.class,
                    criteria);
            // Only TestAdHocRecord has 'name' field matching
            Assert.assertEquals(1, results.size());
        }
    }
    
    @Test
    public void testFindAnyWithMultipleSourcesInHierarchyAlt() {
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<OtherAdHocRecord> otherData = Arrays.asList(
                new OtherAdHocRecord("Alpha"), new OtherAdHocRecord("Beta"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // Find with a criteria that matches across both types
            // Using a field that exists in both (name/value starting with 'A')
            Criteria criteria = Criteria.where().key("age")
                    .operator(Operator.LESS_THAN).value(30).build();
            Set<AdHocRecord> results = runway.findAny(AdHocRecord.class,
                    criteria);
            // Only TestAdHocRecord has 'name' field matching
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testCountAnyWithMultipleSourcesInHierarchy() {
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<OtherAdHocRecord> otherData = Arrays.asList(
                new OtherAdHocRecord("Report1"),
                new OtherAdHocRecord("Report2"),
                new OtherAdHocRecord("Report3"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            int count = runway.countAny(AdHocRecord.class);
            Assert.assertEquals(5, count);
        }
    }

    @Test
    public void testLoadAnyWithSingleSourceStillWorks() {
        // Regression test: single source in hierarchy should still work
        Collection<TestAdHocRecord> data = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Set<AdHocRecord> results = runway.loadAny(AdHocRecord.class);
            Assert.assertEquals(1, results.size());
        }
    }

    // ========================================================================
    // Multiple Sources for Same Type Tests
    // ========================================================================

    @Test
    public void testMultipleSourcesForSameTypeAggregatesResults() {
        // Two separate data sources for the same AdHocRecord type
        Collection<TestAdHocRecord> data1 = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<TestAdHocRecord> data2 = Arrays.asList(
                new TestAdHocRecord("Charlie", 35),
                new TestAdHocRecord("Diana", 40));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data1);
        AdHocDataSource<TestAdHocRecord> source2 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data2);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // load should aggregate from both sources
            Set<TestAdHocRecord> results = runway.load(TestAdHocRecord.class);
            Assert.assertEquals(4, results.size());

            // Verify all records present
            Set<String> names = results.stream().map(r -> r.name)
                    .collect(java.util.stream.Collectors.toSet());
            Assert.assertTrue(names.contains("Alice"));
            Assert.assertTrue(names.contains("Bob"));
            Assert.assertTrue(names.contains("Charlie"));
            Assert.assertTrue(names.contains("Diana"));
        }
    }

    @Test
    public void testMultipleSourcesForSameTypeFindAggregatesResults() {
        Collection<TestAdHocRecord> data1 = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<TestAdHocRecord> data2 = Arrays.asList(
                new TestAdHocRecord("Charlie", 35),
                new TestAdHocRecord("Diana", 28));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data1);
        AdHocDataSource<TestAdHocRecord> source2 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data2);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // find with criteria should search both sources
            Criteria criteria = Criteria.where().key("age")
                    .operator(Operator.GREATER_THAN).value(27).build();
            Set<TestAdHocRecord> results = runway.find(TestAdHocRecord.class,
                    criteria);

            // Alice (30), Charlie (35), Diana (28) match
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testMultipleSourcesForSameTypeCountAggregates() {
        Collection<TestAdHocRecord> data1 = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<TestAdHocRecord> data2 = Arrays
                .asList(new TestAdHocRecord("Charlie", 35));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data1);
        AdHocDataSource<TestAdHocRecord> source2 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data2);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            int count = runway.count(TestAdHocRecord.class);
            Assert.assertEquals(3, count);
        }
    }

    @Test
    public void testMultipleSourcesForSameTypeWithSortingAndPagination() {
        Collection<TestAdHocRecord> data1 = Arrays.asList(
                new TestAdHocRecord("Charlie", 35),
                new TestAdHocRecord("Alice", 30));
        Collection<TestAdHocRecord> data2 = Arrays.asList(
                new TestAdHocRecord("Bob", 25),
                new TestAdHocRecord("Diana", 40));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data1);
        AdHocDataSource<TestAdHocRecord> source2 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data2);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // Sort by name, get first page of 2
            Order order = Order.by("name").ascending().build();
            Page page = Page.sized(2).go(1);

            Set<TestAdHocRecord> results = runway.load(TestAdHocRecord.class,
                    order, page);

            Assert.assertEquals(2, results.size());

            // Should be Alice, Bob (sorted alphabetically, first 2)
            String[] names = results.stream().map(r -> r.name)
                    .toArray(String[]::new);
            Assert.assertEquals("Alice", names[0]);
            Assert.assertEquals("Bob", names[1]);
        }
    }

    // ========================================================================
    // Additional Coverage Tests
    // ========================================================================

    @Test
    public void testFindAnyUniqueWithAttachedSource() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Criteria criteria = Criteria.where().key("name")
                    .operator(Operator.EQUALS).value("Alice").build();
            AdHocRecord result = runway.findAnyUnique(AdHocRecord.class,
                    criteria);
            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof TestAdHocRecord);
            Assert.assertEquals("Alice", ((TestAdHocRecord) result).name);
        }
    }

    @Test
    public void testFindAnyUniqueWithMultipleSourcesInHierarchy() {
        Collection<TestAdHocRecord> testData = Arrays
                .asList(new TestAdHocRecord("Alice", 30));
        Collection<OtherAdHocRecord> otherData = Arrays
                .asList(new OtherAdHocRecord("Report1"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            Criteria criteria = Criteria.where().key("name")
                    .operator(Operator.EQUALS).value("Alice").build();
            AdHocRecord result = runway.findAnyUnique(AdHocRecord.class,
                    criteria);
            Assert.assertNotNull(result);
            Assert.assertEquals("Alice", ((TestAdHocRecord) result).name);
        }
    }

    @Test
    public void testLoadByIdWithAttachedSource() {
        TestAdHocRecord alice = new TestAdHocRecord("Alice", 30);
        TestAdHocRecord bob = new TestAdHocRecord("Bob", 25);
        Collection<TestAdHocRecord> data = Arrays.asList(alice, bob);
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            // Load by specific id
            TestAdHocRecord loaded = runway.load(TestAdHocRecord.class,
                    alice.id());
            Assert.assertNotNull(loaded);
            Assert.assertEquals("Alice", loaded.name);

            // Load non-existent id
            TestAdHocRecord notFound = runway.load(TestAdHocRecord.class,
                    999999L);
            Assert.assertNull(notFound);
        }
    }

    @Test
    public void testLoadAnyWithSortingAcrossMultipleSources() {
        // Create records with ages that interleave when sorted
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 20));
        Collection<OtherAdHocRecord> otherData = Arrays.asList(
                new OtherAdHocRecord("Charlie"), // no age field
                new OtherAdHocRecord("Dave"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // Sort by name ascending
            Order order = Order.by("name").ascending().build();
            Set<AdHocRecord> results = runway.loadAny(AdHocRecord.class, order);
            Assert.assertEquals(4, results.size());

            // Verify order: Alice, Bob, Charlie, Dave
            String[] expectedOrder = { "Alice", "Bob", "Charlie", "Dave" };
            int i = 0;
            for (AdHocRecord record : results) {
                String name = record instanceof TestAdHocRecord
                        ? ((TestAdHocRecord) record).name
                        : ((OtherAdHocRecord) record).value;
                Assert.assertEquals(expectedOrder[i], name);
                i++;
            }
        }
    }

    @Test
    public void testLoadAnyWithPaginationAcrossMultipleSources() {
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));
        Collection<OtherAdHocRecord> otherData = Arrays.asList(
                new OtherAdHocRecord("Report1"),
                new OtherAdHocRecord("Report2"),
                new OtherAdHocRecord("Report3"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // Total 5 records, get page 2 with size 2 (records 3-4)
            Page page = Page.sized(2).go(2);
            Set<AdHocRecord> results = runway.loadAny(AdHocRecord.class, page);
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testFindAnyWithSortingAndPaginationAcrossMultipleSources() {
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25),
                new TestAdHocRecord("Charlie", 35));
        Collection<OtherAdHocRecord> otherData = Arrays.asList(
                new OtherAdHocRecord("Dave"), new OtherAdHocRecord("Eve"));

        AdHocDataSource<TestAdHocRecord> source1 = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);
        AdHocDataSource<OtherAdHocRecord> source2 = new AdHocDataSource<>(
                OtherAdHocRecord.class, () -> otherData);

        try (AttachmentScope scope = runway.attach(source1, source2)) {
            // Find all with any name containing criteria, sort by name, page 1
            Criteria criteria = Criteria.where().key("name")
                    .operator(Operator.REGEX).value(".*").build();
            Order order = Order.by("name").ascending().build();
            Page page = Page.sized(2).go(1);

            Set<TestAdHocRecord> results = runway.findAny(TestAdHocRecord.class,
                    criteria, order, page);
            // Only TestAdHocRecords match, sorted: Alice, Bob, Charlie
            // Page 1 with size 2: Alice, Bob
            Assert.assertEquals(2, results.size());

            String[] names = results.stream().map(r -> r.name)
                    .toArray(String[]::new);
            Assert.assertEquals("Alice", names[0]);
            Assert.assertEquals("Bob", names[1]);
        }
    }

    @Test
    public void testFindAnyWithCriteriaAndOrderAppliesOrder() {
        // Regression test: findAny(Class, Criteria, Order) was ignoring order
        Collection<TestAdHocRecord> testData = Arrays.asList(
                new TestAdHocRecord("Charlie", 35),
                new TestAdHocRecord("Alice", 30),
                new TestAdHocRecord("Bob", 25));

        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> testData);

        try (AttachmentScope scope = runway.attach(source)) {
            Criteria criteria = Criteria.where().key("age")
                    .operator(Operator.GREATER_THAN).value(0).build();
            Order order = Order.by("name").ascending().build();

            // Use the 3-arg overload: findAny(Class, Criteria, Order)
            Set<TestAdHocRecord> results = runway.findAny(TestAdHocRecord.class,
                    criteria, order);

            Assert.assertEquals(3, results.size());

            // Verify results are sorted by name: Alice, Bob, Charlie
            String[] names = results.stream().map(r -> r.name)
                    .toArray(String[]::new);
            Assert.assertEquals("Alice", names[0]);
            Assert.assertEquals("Bob", names[1]);
            Assert.assertEquals("Charlie", names[2]);
        }
    }

    // ========================================================================
    // Test Record Classes
    // ========================================================================

    /**
     * A mock {@link AdHocRecord} for testing.
     */
    static class TestAdHocRecord extends AdHocRecord {

        String name;
        int age;

        TestAdHocRecord(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    /**
     * Another mock {@link AdHocRecord} for testing multiple sources.
     */
    static class OtherAdHocRecord extends AdHocRecord {

        String value;

        OtherAdHocRecord(String value) {
            this.value = value;
        }
    }

    /**
     * A persistent {@link Record} for integration testing.
     */
    static class Person extends Record {

        String name;
        int age;
    }

}
