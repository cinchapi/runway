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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link Runway#attach(FederatedDataSource...)} and
 * {@link Runway#detach(FederatedDataSource)} functionality, including
 * thread isolation guarantees.
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
            Set<TestAdHocRecord> scopeResults = scope.find(
                    TestAdHocRecord.class, criteria);
            Set<TestAdHocRecord> runwayResults = runway.find(
                    TestAdHocRecord.class, criteria);

            Assert.assertEquals(scopeResults.size(), runwayResults.size());
            Assert.assertEquals(2, scopeResults.size());
        }
    }

    @Test
    public void testAutoDetachOnClose() {
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30));
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
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30));
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
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30));
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
        Collection<TestAdHocRecord> data1 = Arrays.asList(
                new TestAdHocRecord("Alice", 30));
        Collection<OtherAdHocRecord> data2 = Arrays.asList(
                new OtherAdHocRecord("Report1"));

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
        Collection<TestAdHocRecord> adhocData = Arrays.asList(
                new TestAdHocRecord("AdHoc", 99));
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
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30));
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
        Collection<TestAdHocRecord> data1 = Arrays.asList(
                new TestAdHocRecord("Main", 1));
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
                otherThreadResult.set(runway.load(TestAdHocRecord.class).size());
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
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30));
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
                // After main thread detached its copy, verify our copy still works
                afterDetachResult.set(runway.load(TestAdHocRecord.class).size());
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
            Set<TestAdHocRecord> results = scope.find(
                    TestAdHocRecord.class, criteria);
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
            TestAdHocRecord result = runway.findUnique(
                    TestAdHocRecord.class, criteria);
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
            // loadAny(AdHocRecord.class) should return records from BOTH sources
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
                new OtherAdHocRecord("Alpha"),
                new OtherAdHocRecord("Beta"));

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
        Collection<TestAdHocRecord> data = Arrays.asList(
                new TestAdHocRecord("Alice", 30));
        AdHocDataSource<TestAdHocRecord> source = new AdHocDataSource<>(
                TestAdHocRecord.class, () -> data);

        try (AttachmentScope scope = runway.attach(source)) {
            Set<AdHocRecord> results = runway.loadAny(AdHocRecord.class);
            Assert.assertEquals(1, results.size());
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

