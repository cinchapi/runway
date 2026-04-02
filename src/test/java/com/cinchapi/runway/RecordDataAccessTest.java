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

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link Record} data access, filtering, computed/derived properties,
 * and path resolution.
 *
 * @author Jeff Nelson
 */
public class RecordDataAccessTest extends AbstractRecordTest {

    @Test
    public void testLoadPopulatesFields() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 100;
        runway.save(person);
        person = runway.load(Mock.class, person.id());
        Assert.assertEquals("Jeff Nelson", person.name);
        Assert.assertEquals((int) 100, (int) person.age);
    }

    @Test
    public void testLoadAllRecordsFromClass() {
        int count = Random.getScaleCount();
        for (int i = 0; i < count; ++i) {
            Mock mock = new Mock();
            mock.name = Random.getSimpleString();
            mock.age = Random.getInt();
            runway.save(mock);
        }
        Assert.assertEquals(count, runway.load(Mock.class).size());
    }

    @Test
    public void testLoadNonExistingRecord() {
        Assert.assertNull(runway.load(Mock.class, -2));
    }

    @Test
    public void testSetDynamicAttribute() {
        Mock person = new Mock();
        person.set("0_2_0", "foo");
        System.out.println(person);
    }

    @Test
    public void testSetDynamicValue() {
        Flock flock = new Flock("flock");
        String key = Random.getSimpleString();
        flock.set(key, 1);
        Assert.assertEquals(1, (int) flock.get(key));
        Assert.assertTrue(flock.map().containsKey(key));
    }

    @Test
    public void testCanGetReadablePrivateField() {
        Mock mock = new Mock();
        Assert.assertTrue(mock.map().containsKey("bar"));
        Assert.assertNotNull(mock.get("bar"));
    }

    @Test
    public void testCannotGetNonReadablePrivateField() {
        Mock mock = new Mock();
        Assert.assertFalse(mock.map().containsKey("foo"));
        Assert.assertNull(mock.get("foo"));
    }

    @Test
    public void testLoadRecordWithCollectionOfLinks() {
        Lock lock = new Lock(ImmutableList.of(new Dock("dock")));
        lock.save();
        Assert.assertEquals(lock, runway.load(Lock.class, lock.id()));
    }

    @Test
    public void testGetNoKeysReturnsAllData() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map();
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertTrue(data.containsKey("bar"));
        Assert.assertTrue(data.containsKey("city"));
    }

    @Test
    public void testGetNegativeFiltering() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map("-age", "-city");
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertTrue(data.containsKey("bar"));
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testGetNegativeAndPositiveFiltering() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map("-age", "alive", "-city");
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertFalse(data.containsKey("bar"));
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testGetPositiveFiltering() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map("age", "alive", "city");
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertFalse(data.containsKey("bar"));
        Assert.assertTrue(data.containsKey("city"));
    }

    @Test
    public void testGetIdUseGetMethod() {
        Nock nock = new Nock();
        Assert.assertEquals((long) nock.id(), (long) nock.get("id"));
    }

    @Test
    public void testIntrinsicMapDoesNotReturnComputedData() {
        Bock bock = new Bock();
        Map<String, Object> data = bock.intrinsic();
        Assert.assertFalse(data.containsKey("state"));
    }

    @Test
    public void testIntrinsicMapDoesNotReturnComputedDataEvenIfRequested() {
        Bock bock = new Bock();
        Map<String, Object> data = bock.intrinsic("state");
        Assert.assertFalse(data.containsKey("state"));
    }

    @Test
    public void testIntrinsicMapDoesNotReturnDerivedData() {
        Nock nock = new Nock();
        Map<String, Object> data = nock.intrinsic();
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testIntrinsicMapDoesNotReturnDerivedDataEvenIfRequested() {
        Nock nock = new Nock();
        Map<String, Object> data = nock.intrinsic("city");
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testInstrinsicMapAllNegativeFilters() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 100;
        Map<String, Object> data = nock.intrinsic("-age", "-name");
        Assert.assertFalse(data.containsKey("state"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertTrue(data.containsKey("bar"));
    }

    @Test
    public void testIntrinsicMapPositiveAndNegativeFilters() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 100;
        Map<String, Object> data = nock.intrinsic("-age", "name", "-bar");
        Assert.assertFalse(data.containsKey("state"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertFalse(data.containsKey("bar"));
        Assert.assertFalse(data.containsKey("alive"));
    }

    @Test
    public void testGetComputedValue() {
        Rock rock = new Rock();
        long start = System.currentTimeMillis();
        String state = rock.get("state");
        long end = System.currentTimeMillis();
        Assert.assertEquals("Georgia", state);
        Assert.assertTrue(end - start >= 1000);
    }

    @Test
    public void testGetAnnotatedComputedValue() {
        Rock rock = new Rock();
        long start = System.currentTimeMillis();
        String county = rock.get("county");
        long end = System.currentTimeMillis();
        Assert.assertEquals("Fulton", county);
        Assert.assertTrue(end - start >= 1000);
    }

    @Test
    public void testAnnotatedComputedValueIncludedInGetAll() {
        Rock rock = new Rock();
        Map<String, Object> data = rock.map();
        Assert.assertTrue(data.containsKey("state"));
        Assert.assertTrue(data.containsKey("county"));
    }

    @Test
    public void testComputedValueNotComputedIfNotNecessary() {
        Bock bock = new Bock();
        Map<String, Object> data = bock.map("-state");
        Assert.assertFalse(data.containsKey("state"));
    }

    @Test
    public void testAnnotatedComputedValueNotComputedIfNotNecessary() {
        Bock bock = new Bock();
        Map<String, Object> data = bock.map("-county");
        Assert.assertFalse(data.containsKey("county"));
    }

    @Test
    public void testGetAnnotatedDerivedProperty() {
        Nock nock = new Nock();
        Assert.assertEquals("Atlanta", nock.get("area"));
        Assert.assertEquals("30327", nock.get("zipcode"));
    }

    @Test
    public void testGetPathsWithDescendantDefinedFields() {
        boolean computePathsForDescendantDefinedFields = Record.StaticAnalysis.COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS;
        Record.StaticAnalysis.COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS = true;
        Record.StaticAnalysis.instance().computeAllPossiblePaths();
        try {
            Set<String> paths = Record.StaticAnalysis.instance()
                    .getPaths(Gock.class);
            long count = 0;

            /*
             * Detect a path that would by cyclic and terminate it
             */
            count = paths.stream().filter(path -> path.startsWith("gock"))
                    .count();
            Assert.assertEquals(1, count);
            count = paths.stream().filter(path -> path.startsWith("jock.testy"))
                    .count();
            Assert.assertEquals(1, count);
            count = paths.stream().filter(path -> path.startsWith("testy"))
                    .count();
            Assert.assertEquals(1, count);

            /*
             * Collection of Links is terminated (e.g. no numeric expansion
             * paths)
             */
            count = paths.stream()
                    .filter(path -> path.startsWith("stock.tock.stocks"))
                    .count();
            Assert.assertEquals(1, count);
            count = paths.stream()
                    .filter(path -> path.startsWith("node.friends")).count();
            Assert.assertEquals(1, count);
            count = paths.stream()
                    .filter(path -> path.startsWith("jock.friends")).count();
            Assert.assertEquals(1, count);
            count = paths.stream().filter(path -> path.startsWith("friends"))
                    .count();
            Assert.assertEquals(1, count);

            /*
             * Expected Paths
             */
            Assert.assertTrue(paths.contains("stock.tock.zombie"));
            Assert.assertTrue(paths.contains("node.label"));
            Assert.assertTrue(paths.contains("user.name"));
            Assert.assertTrue(paths.contains("user.email"));
            Assert.assertTrue(paths.contains("user.company.name"));
            Assert.assertTrue(paths.contains("sock.sock"));
            Assert.assertTrue(paths.contains("sock.dock.dock"));
            Assert.assertTrue(paths.contains("jock.name"));
            Assert.assertTrue(paths.contains("jock2.name"));
            Assert.assertTrue(paths.contains("name"));
            Assert.assertTrue(paths.contains("hock.a"));
            Assert.assertTrue(paths.contains("qock.a"));
            Assert.assertTrue(paths.contains("qock.b")); // descendant
                                                         // defined
                                                         // field

            /*
             * Deferred Reference Isn't Expanded
             */
            count = paths.stream()
                    .filter(path -> path.startsWith("jock.mentor")).count();
            Assert.assertEquals(1, count);
            count = paths.stream().filter(path -> path.startsWith("mentor"))
                    .count();
            Assert.assertEquals(1, count);
        }
        finally {
            Record.StaticAnalysis.COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS = computePathsForDescendantDefinedFields;
            Record.StaticAnalysis.instance().computeAllPossiblePaths();
        }
    }

    @Test
    public void testGetPathsWitouthDescendantDefinedFields() {
        boolean computePathsForDescendantDefinedFields = Record.StaticAnalysis.COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS;
        Record.StaticAnalysis.COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS = false;
        Record.StaticAnalysis.instance().computeAllPossiblePaths();
        try {
            Set<String> paths = Record.StaticAnalysis.instance()
                    .getPaths(Gock.class);
            Assert.assertTrue(paths.contains("stock.tock.zombie"));
            Assert.assertTrue(paths.contains("node.label"));
            Assert.assertTrue(paths.contains("user.name"));
            Assert.assertTrue(paths.contains("user.email"));
            Assert.assertTrue(paths.contains("user.company.name"));
            Assert.assertTrue(paths.contains("sock.sock"));
            Assert.assertTrue(paths.contains("sock.dock.dock"));
            Assert.assertTrue(paths.contains("jock"));
            Assert.assertTrue(paths.contains("jock2"));
            Assert.assertTrue(paths.contains("name"));
            Assert.assertTrue(paths.contains("hock.a"));
            Assert.assertTrue(paths.contains("qock")); // not expanded
                                                       // due to
                                                       // descendant
                                                       // defined
                                                       // fields
            System.out.println(paths);
        }
        finally {
            Record.StaticAnalysis.COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS = computePathsForDescendantDefinedFields;
            Record.StaticAnalysis.instance().computeAllPossiblePaths();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * evaluates a {@link Criteria} whose key navigates through a private field
     * into a linked {@link Record Record's} property.
     * <p>
     * <strong>Start state:</strong> A saved {@link Conversation} with a private
     * {@link Participant} reference.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Participant} with a known {@code userId}.</li>
     * <li>Create a {@link Conversation} whose private {@code participant} field
     * references that {@link Participant}.</li>
     * <li>Build a {@link Criteria} using the navigation key
     * {@code participant.userId}.</li>
     * <li>Call {@link Record#matches(Criteria)} on the
     * {@link Conversation}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the navigation traverses the private field to reach the linked
     * {@link Participant Participant's} {@code userId}.
     */
    @Test
    public void testMatchesWithNavigationKeyFromPrivateField() {
        Participant alice = new Participant("alice123");
        Conversation convo = new Conversation("Hello", alice);
        convo.save();
        Criteria criteria = Criteria.where().key("participant.userId")
                .operator(Operator.EQUALS).value("alice123").build();
        Assert.assertTrue(convo.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * returns {@code false} when the navigation key resolves to a value that
     * does not satisfy the {@link Criteria}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Conversation} with a private
     * {@link Participant} reference.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Participant} with {@code userId}
     * {@code "alice123"}.</li>
     * <li>Create a {@link Conversation} referencing that
     * {@link Participant}.</li>
     * <li>Build a {@link Criteria} that expects {@code participant.userId} to
     * equal {@code "bob456"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on the
     * {@link Conversation}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code false} because
     * the actual value does not match.
     */
    @Test
    public void testMatchesReturnsFalseWhenNavigationKeyDoesNotSatisfyCriteria() {
        Participant alice = new Participant("alice123");
        Conversation convo = new Conversation("Hello", alice);
        convo.save();
        Criteria criteria = Criteria.where().key("participant.userId")
                .operator(Operator.EQUALS).value("bob456").build();
        Assert.assertFalse(convo.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)} works
     * with simple (non-navigation) keys.
     * <p>
     * <strong>Start state:</strong> A saved {@link Conversation} with a known
     * {@code topic}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Conversation} with topic {@code "Hello"}.</li>
     * <li>Build a {@link Criteria} matching {@code topic} equals
     * {@code "Hello"}.</li>
     * <li>Call {@link Record#matches(Criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true}.
     */
    @Test
    public void testMatchesWithSimpleKey() {
        Participant alice = new Participant("alice123");
        Conversation convo = new Conversation("Hello", alice);
        convo.save();
        Criteria criteria = Criteria.where().key("topic")
                .operator(Operator.EQUALS).value("Hello").build();
        Assert.assertTrue(convo.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly resolves navigation keys through a collection of linked
     * {@link Record Records}.
     * <p>
     * <strong>Start state:</strong> A {@link Node} with a list of friend
     * {@link Node Nodes}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Node Nodes} {@code a}, {@code b}, {@code c}, and
     * {@code d}.</li>
     * <li>Add {@code b}, {@code c}, and {@code d} as friends of {@code a}.</li>
     * <li>Build a {@link Criteria} matching {@code friends.label} equals
     * {@code "b"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code a}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * one of {@code a}'s friends has label {@code "b"}.
     */
    @Test
    public void testMatchesWithCollectionNavigationKey() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        Node d = new Node("d");
        a.friends.add(b);
        a.friends.add(c);
        a.friends.add(d);
        Criteria criteria = Criteria.where().key("friends.label")
                .operator(Operator.EQUALS).value("b").build();
        Assert.assertTrue(a.matches(criteria));
    }

}
