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

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly evaluates a 3-hop navigation path through a collection-based
     * object graph, matching the pattern used by visibility scope filters
     * (e.g., {@code orgs.seats.member.userId == audienceUserId}).
     * <p>
     * <strong>Start state:</strong> Two {@link Member Members} who share an
     * {@link Org} through {@link OrgMembership OrgMemberships}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Member} {@code alice} with {@code userId "alice123"}
     * and {@link Member} {@code bob} with {@code userId "bob456"}.</li>
     * <li>Create an {@link Org} and add both members via {@link OrgMembership
     * OrgMemberships}.</li>
     * <li>Save all records.</li>
     * <li>Build a {@link Criteria} that matches the visibility scope pattern:
     * {@code userId == "alice123" OR
     *     orgs.seats.member.userId == "alice123"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code bob}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * {@code bob} shares an {@link Org} with {@code alice}, so the 3-hop
     * navigation {@code orgs.seats.member.userId} resolves to
     * {@code "alice123"}.
     */
    @Test
    public void testMatchesWithThreeHopNavigationKeyThroughSharedGroup() {
        Member alice = new Member("alice123");
        Member bob = new Member("bob456");
        Org org = new Org("Shared Org");
        new OrgMembership(org, alice);
        new OrgMembership(org, bob);
        runway.save(alice, bob, org);
        Criteria criteria = Criteria.where().group(Criteria.where()
                .group(Criteria.where().key("userId").operator(Operator.EQUALS)
                        .value("alice123"))
                .or().group(Criteria.where().key("orgs.seats.member.userId")
                        .operator(Operator.EQUALS).value("alice123")));

        // Verify the database-level query finds bob (proves Concourse can
        // resolve the 3-hop navigation)
        Set<Long> dbResults = client.find(criteria);
        Assert.assertTrue("DB should find bob via the 3-hop path, "
                + "but got: " + dbResults, dbResults.contains(bob.id()));

        // Now verify Record.matches() agrees with the DB
        Assert.assertTrue(
                "bob should match because he shares an Org "
                        + "with alice via the 3-hop path "
                        + "orgs.seats.member.userId, but "
                        + "Record.matches() disagrees with the DB",
                bob.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * returns {@code true} for a self-match on the simple {@code userId}
     * disjunct, independent of the 3-hop path.
     * <p>
     * <strong>Start state:</strong> A single saved {@link Member}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save {@link Member} {@code alice} with
     * {@code userId "alice123"}.</li>
     * <li>Build the same visibility-scope {@link Criteria}:
     * {@code userId == "alice123" OR
     *     orgs.seats.member.userId == "alice123"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code alice}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the first disjunct ({@code userId == "alice123"}) matches directly.
     */
    @Test
    public void testMatchesWithThreeHopNavigationKeySelfMatch() {
        Member alice = new Member("alice123");
        Org org = new Org("Solo Org");
        new OrgMembership(org, alice);
        runway.save(alice, org);
        Criteria criteria = Criteria.where().group(Criteria.where()
                .group(Criteria.where().key("userId").operator(Operator.EQUALS)
                        .value("alice123"))
                .or().group(Criteria.where().key("orgs.seats.member.userId")
                        .operator(Operator.EQUALS).value("alice123")));
        Assert.assertTrue(
                "alice should match on the direct userId " + "disjunct",
                alice.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * returns {@code false} for a {@link Member} who does NOT share any
     * {@link Org} with the target.
     * <p>
     * <strong>Start state:</strong> Two {@link Member Members} in separate
     * {@link Org Orgs} with no overlap.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Member} {@code alice} in {@code "Org A"} and
     * {@link Member} {@code charlie} in {@code "Org B"}.</li>
     * <li>Save all records.</li>
     * <li>Build the visibility-scope {@link Criteria}:
     * {@code userId == "alice123" OR
     *     orgs.seats.member.userId == "alice123"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code charlie}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code false} because
     * {@code charlie} does not share an {@link Org} with {@code alice} and has
     * a different {@code userId}.
     */
    @Test
    public void testMatchesWithThreeHopNavigationKeyNoSharedGroup() {
        Member alice = new Member("alice123");
        Member charlie = new Member("charlie789");
        Org orgA = new Org("Org A");
        Org orgB = new Org("Org B");
        new OrgMembership(orgA, alice);
        new OrgMembership(orgB, charlie);
        runway.save(alice, charlie, orgA, orgB);
        Criteria criteria = Criteria.where().group(Criteria.where()
                .group(Criteria.where().key("userId").operator(Operator.EQUALS)
                        .value("alice123"))
                .or().group(Criteria.where().key("orgs.seats.member.userId")
                        .operator(Operator.EQUALS).value("alice123")));
        Assert.assertFalse(
                "charlie should not match because he does "
                        + "not share any Org with alice",
                charlie.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly resolves a 2-hop navigation path through a collection to a
     * scalar field on records within a nested collection ({@code orgs.name}).
     * <p>
     * <strong>Start state:</strong> A {@link Member} belonging to two
     * {@link Org Orgs} with distinct names.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Member} {@code alice} in {@code "Org One"} and
     * {@code "Org Two"}.</li>
     * <li>Save all records.</li>
     * <li>Build a {@link Criteria} matching {@code orgs.name} equals
     * {@code "Org Two"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code alice}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the 2-hop path resolves to a flat collection of org names that includes
     * {@code "Org Two"}.
     */
    @Test
    public void testMatchesWithTwoHopNavigationKeyCollectionToScalar() {
        Member alice = new Member("alice123");
        Org org1 = new Org("Org One");
        Org org2 = new Org("Org Two");
        new OrgMembership(org1, alice);
        new OrgMembership(org2, alice);
        runway.save(alice, org1, org2);
        Criteria criteria = Criteria.where().key("orgs.name")
                .operator(Operator.EQUALS).value("Org Two").build();
        Assert.assertTrue("alice's orgs.name should contain 'Org Two' "
                + "via 2-hop navigation", alice.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly resolves a 2-hop navigation path through a collection to a
     * nested collection using {@link Operator#LINKS_TO}.
     * <p>
     * <strong>Start state:</strong> A {@link Member} with one {@link Org}
     * containing two {@link OrgMembership OrgMemberships}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Member} {@code alice} and {@code bob} in a shared
     * {@link Org}.</li>
     * <li>Save all records.</li>
     * <li>Build a {@link Criteria} matching {@code orgs.seats} that
     * {@link Operator#LINKS_TO LINKS_TO} bob's {@link OrgMembership} id.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code alice}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the 2-hop path resolves to a flat collection containing bob's
     * {@link OrgMembership}.
     */
    @Test
    public void testMatchesWithTwoHopNavigationKeyCollectionToCollection() {
        Member alice = new Member("alice123");
        Member bob = new Member("bob456");
        Org org = new Org("Shared Org");
        OrgMembership bobSeat = new OrgMembership(org, bob);
        new OrgMembership(org, alice);
        runway.save(alice, bob, org);
        Criteria criteria = Criteria.where().key("orgs.seats")
                .operator(Operator.LINKS_TO).value(bobSeat.id());
        Assert.assertTrue(
                "alice's orgs.seats should contain bob's "
                        + "membership via 2-hop navigation",
                alice.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly resolves a 3-hop navigation path from a collection through a
     * collection to a scalar field on a single {@link Record}
     * ({@code orgs.seats.member.userId}).
     * <p>
     * <strong>Start state:</strong> Two {@link Member Members} in a shared
     * {@link Org}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Member} {@code alice} and {@code bob} in a shared
     * {@link Org}.</li>
     * <li>Save all records.</li>
     * <li>Build a {@link Criteria} matching {@code orgs.seats.member.userId}
     * equals {@code "bob456"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code alice}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the 3-hop path resolves to a flat collection of userIds that includes
     * {@code "bob456"}.
     */
    @Test
    public void testMatchesWithThreeHopNavigationKeyCollectionToScalar() {
        Member alice = new Member("alice123");
        Member bob = new Member("bob456");
        Org org = new Org("Shared Org");
        new OrgMembership(org, alice);
        new OrgMembership(org, bob);
        runway.save(alice, bob, org);
        Criteria criteria = Criteria.where().key("orgs.seats.member.userId")
                .operator(Operator.EQUALS).value("bob456").build();
        Assert.assertTrue(
                "alice's orgs.seats.member.userId should "
                        + "include 'bob456' via 3-hop navigation",
                alice.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly resolves a 3-hop navigation path from a collection through a
     * collection to a single {@link Record} using {@link Operator#LINKS_TO}
     * ({@code orgs.seats.member}).
     * <p>
     * <strong>Start state:</strong> Two {@link Member Members} in a shared
     * {@link Org}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Member} {@code alice} and {@code bob} in a shared
     * {@link Org}.</li>
     * <li>Save all records.</li>
     * <li>Build a {@link Criteria} matching {@code orgs.seats.member} that
     * {@link Operator#LINKS_TO LINKS_TO} bob's id.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code alice}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the 3-hop path resolves to a flat collection of {@link Member Members}
     * that includes {@code bob}.
     */
    @Test
    public void testMatchesWithThreeHopNavigationKeyCollectionToRecordLinksTo() {
        Member alice = new Member("alice123");
        Member bob = new Member("bob456");
        Org org = new Org("Shared Org");
        new OrgMembership(org, alice);
        new OrgMembership(org, bob);
        runway.save(alice, bob, org);
        Criteria criteria = Criteria.where().key("orgs.seats.member")
                .operator(Operator.LINKS_TO).value(bob.id());
        Assert.assertTrue("alice's orgs.seats.member should include "
                + "bob via 3-hop navigation", alice.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly resolves a 5-hop navigation path that traverses multiple
     * collection boundaries: {@code orgs.seats.member.orgs.name}.
     * <p>
     * <strong>Start state:</strong> Two {@link Member Members} in a shared
     * {@link Org} named {@code "Shared Org"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create {@link Member} {@code alice} and {@code bob} in a shared
     * {@link Org} named {@code "Shared Org"}.</li>
     * <li>Save all records.</li>
     * <li>Build a {@link Criteria} matching {@code orgs.seats.member.orgs.name}
     * equals {@code "Shared Org"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code bob}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the 5-hop path traverses bob's orgs, their seats, those seats' members,
     * those members' orgs, and finally resolves the org name.
     */
    @Test
    public void testMatchesWithFiveHopNavigationKeyMultipleCollectionBoundaries() {
        Member alice = new Member("alice123");
        Member bob = new Member("bob456");
        Org org = new Org("Shared Org");
        new OrgMembership(org, alice);
        new OrgMembership(org, bob);
        runway.save(alice, bob, org);
        Criteria criteria = Criteria.where().key("orgs.seats.member.orgs.name")
                .operator(Operator.EQUALS).value("Shared Org").build();
        Assert.assertTrue(
                "5-hop path orgs.seats.member.orgs.name "
                        + "should resolve to 'Shared Org'",
                bob.matches(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#matches(Criteria)}
     * correctly resolves a 5-hop path across multiple {@link Org Orgs} where a
     * {@link Member} belongs to two {@link Org Orgs} with different names.
     * <p>
     * <strong>Start state:</strong> {@code alice} in {@code "Org Alpha"},
     * {@code bob} in both {@code "Org Alpha"} and {@code "Org Beta"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create the described {@link Org} and {@link Member} structure.</li>
     * <li>Save all records.</li>
     * <li>Build a {@link Criteria} matching {@code orgs.seats.member.orgs.name}
     * equals {@code "Org Beta"}.</li>
     * <li>Call {@link Record#matches(Criteria)} on {@code alice}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code matches} returns {@code true} because
     * the path reaches {@code bob} (a shared member in {@code "Org Alpha"}),
     * then navigates to bob's other org {@code "Org Beta"}.
     */
    @Test
    public void testMatchesWithFiveHopNavigationKeyCrossOrgReachability() {
        Member alice = new Member("alice123");
        Member bob = new Member("bob456");
        Org orgAlpha = new Org("Org Alpha");
        Org orgBeta = new Org("Org Beta");
        new OrgMembership(orgAlpha, alice);
        new OrgMembership(orgAlpha, bob);
        new OrgMembership(orgBeta, bob);
        runway.save(alice, bob, orgAlpha, orgBeta);
        Criteria criteria = Criteria.where().key("orgs.seats.member.orgs.name")
                .operator(Operator.EQUALS).value("Org Beta").build();
        Assert.assertTrue("5-hop path should reach 'Org Beta' through "
                + "shared member bob", alice.matches(criteria));
    }

}
