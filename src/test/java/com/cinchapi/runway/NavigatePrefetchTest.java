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
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tests for the navigate-based prefetching optimization that eliminates N+1
 * loading for {@link java.util.Collection Collection&lt;Record&gt;} fields.
 * <p>
 * This test class covers {@link Record.StaticAnalysis} navigate path
 * computation, end-to-end loading across the {@code load}, {@code find}, and
 * bulk-load pipelines, and regression scenarios for {@link Record Records} that
 * do not have {@link java.util.Collection Collection&lt;Record&gt;} fields.
 *
 * @author Jeff Nelson
 */
public class NavigatePrefetchTest extends RunwayBaseClientServerTest {

    // ---------------------------------------------------------------
    // StaticAnalysis path computation
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that {@link Record.StaticAnalysis} computes
     * navigate paths for {@link java.util.Collection Collection&lt;Record&gt;}
     * fields.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Lock}, which has a
     * {@code List<Dock>} field.</li>
     * <li>Assert that the paths include nested destination paths (e.g.,
     * {@code docks._} and {@code docks.$id$}).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Navigate paths contain the nested paths for
     * the {@link Dock} destination type, prefixed with the collection field
     * name.
     */
    @Test
    public void testNavigatePathsComputedForCollectionRecordField() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Lock.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertFalse(navigatePaths.isEmpty());
        Assert.assertTrue(navigatePaths.contains("docks._"));
        Assert.assertTrue(navigatePaths.contains("docks.$id$"));
        Assert.assertTrue(navigatePaths.contains("docks.dock"));
    }

    /**
     * <strong>Goal:</strong> Verify that navigate paths are not computed for
     * classes without {@link java.util.Collection Collection&lt;Record&gt;}
     * fields.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Simple}, which has no
     * {@link java.util.Collection Collection&lt;Record&gt;} fields.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Navigate paths are {@code null} or empty.
     */
    @Test
    public void testNoNavigatePathsForClassWithoutCollectionRecordFields() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Simple.class);
        Assert.assertTrue(navigatePaths == null || navigatePaths.isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that navigate paths handle self-referential
     * {@link java.util.Collection Collection&lt;Record&gt;} fields without
     * infinite recursion and still include the destination type's non-recursive
     * fields.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Node}, which has a
     * {@code List<Node>} self-referential field.</li>
     * <li>Assert that the paths include destination metadata and non-recursive
     * fields.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Navigate paths include {@code friends._},
     * {@code friends.$id$}, and {@code friends.label}. The self-referential
     * {@code friends.friends} is terminated (not recursed into further).
     */
    @Test
    public void testNavigatePathsForSelfReferentialCollectionField() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Node.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("friends._"));
        Assert.assertTrue(navigatePaths.contains("friends.$id$"));
        Assert.assertTrue(navigatePaths.contains("friends.label"));
        // Self-referential field is terminated — no deep
        // recursion into friends.friends.*
        long deepPaths = navigatePaths.stream()
                .filter(p -> p.startsWith("friends.friends.")).count();
        Assert.assertEquals(0, deepPaths);
    }

    /**
     * <strong>Goal:</strong> Verify that navigate paths are computed for a
     * class that has both a single {@link Record} field and a
     * {@link java.util.Collection Collection&lt;Record&gt;} field.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Vessel}, which has a
     * {@code List<Cargo>} field and a single {@code Port} field.</li>
     * <li>Assert that the navigate paths only cover the collection field (the
     * single {@link Record} field is handled by the existing pre-select path
     * mechanism).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Navigate paths contain {@code cargo._} and
     * {@code cargo.description} but do not contain paths for the single
     * {@code home} field.
     */
    @Test
    public void testNavigatePathsForMixedRecordAndCollectionFields() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Vessel.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("cargo._"));
        Assert.assertTrue(navigatePaths.contains("cargo.description"));
        Assert.assertTrue(navigatePaths.contains("cargo.weight"));
        // Navigate paths should NOT include the single
        // Record field — that is handled by computePaths
        boolean hasHomePath = navigatePaths.stream()
                .anyMatch(p -> p.startsWith("home."));
        Assert.assertFalse(hasHomePath);
    }

    // ---------------------------------------------------------------
    // End-to-end loading — field value correctness
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that loading a {@link Record} with a
     * {@link java.util.Collection Collection&lt;Record&gt;} field correctly
     * populates every element's field values, not just the collection size.
     * <p>
     * <strong>Start state:</strong> A {@link Lock} with three {@link Dock
     * Docks} saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Lock} with three {@link Dock} elements having
     * distinct values and save it.</li>
     * <li>Load the {@link Lock} via {@code runway.load(Lock.class, id)}.</li>
     * <li>Assert the size and verify each {@link Dock Dock's} {@code dock}
     * field value is present in the loaded collection.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded collection has three elements whose
     * {@code dock} values match the original {@code "alpha"}, {@code "beta"},
     * and {@code "gamma"}.
     */
    @Test
    public void testLoadCollectionElementFieldValuesArePopulated() {
        Lock lock = new Lock(ImmutableList.of(new Dock("alpha"),
                new Dock("beta"), new Dock("gamma")));
        lock.save();
        Lock loaded = runway.load(Lock.class, lock.id());
        Assert.assertEquals(3, loaded.docks.size());
        Set<String> dockValues = loaded.docks.stream().map(d -> d.dock)
                .collect(Collectors.toSet());
        Assert.assertTrue(dockValues.contains("alpha"));
        Assert.assertTrue(dockValues.contains("beta"));
        Assert.assertTrue(dockValues.contains("gamma"));
    }

    /**
     * <strong>Goal:</strong> Verify that loading a {@link Record} with an empty
     * {@link java.util.Collection Collection&lt;Record&gt;} field does not
     * cause errors and results in an empty collection.
     * <p>
     * <strong>Start state:</strong> A {@link Lock} with an empty docks list
     * saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Lock} with an empty {@link Dock} list and save
     * it.</li>
     * <li>Load the {@link Lock} via {@code runway.load(Lock.class, id)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Lock} has an empty
     * {@code docks} list and no exceptions are thrown.
     */
    @Test
    public void testLoadRecordWithEmptyCollectionField() {
        Lock lock = new Lock(ImmutableList.of());
        // NOTE: A Lock with zero docks and no other data would
        // be detected as a "zombie" (only the section key
        // exists). A tag gives the record meaningful data.
        lock.tag = "empty";
        lock.save();
        Lock loaded = runway.load(Lock.class, lock.id());
        Assert.assertNotNull(loaded);
        Assert.assertTrue(loaded.docks.isEmpty());
        Assert.assertEquals("empty", loaded.tag);
    }

    /**
     * <strong>Goal:</strong> Verify that the self-referential loading populates
     * friends-of-friends, not just the top-level friends.
     * <p>
     * <strong>Start state:</strong> A {@link Node} graph where A has friends B
     * and C, and B has friend C.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create the graph, save A (cascading to B and C).</li>
     * <li>Load A via {@code runway.load(Node.class, id)}.</li>
     * <li>Find friend B in the loaded graph and verify that B's own friends
     * list is populated with C.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Loaded A has 2 friends. The friend with label
     * {@code "b"} has 1 friend whose label is {@code "c"}.
     */
    @Test
    public void testSelfReferentialFriendsOfFriendsArePopulated() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        b.friends.add(c);
        a.save();
        Node loadedA = runway.load(Node.class, a.id());
        Assert.assertEquals(2, loadedA.friends.size());
        Node loadedB = loadedA.friends.stream().filter(n -> "b".equals(n.label))
                .findFirst().orElse(null);
        Assert.assertNotNull(loadedB);
        Assert.assertEquals(1, loadedB.friends.size());
        Assert.assertEquals("c", loadedB.friends.get(0).label);
    }

    // ---------------------------------------------------------------
    // Mixed field types — single Record + Collection<Record>
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} with both a single
     * {@link Record} field and a {@link java.util.Collection
     * Collection&lt;Record&gt;} field loads both correctly.
     * <p>
     * <strong>Start state:</strong> A {@link Vessel} with a {@code home}
     * {@link Port} and two {@link Cargo} items saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Vessel} with a {@link Port} and two {@link Cargo}
     * items and save it.</li>
     * <li>Load the {@link Vessel} via
     * {@code runway.load(Vessel.class, id)}.</li>
     * <li>Assert the single {@link Port} field and the {@link Cargo} collection
     * are both populated with correct values.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Vessel} has a {@code home}
     * {@link Port} with name {@code "harbor"} and two {@link Cargo} items with
     * the correct descriptions and weights.
     */
    @Test
    public void testLoadRecordWithBothSingleAndCollectionRecordFields() {
        Port harbor = new Port("harbor");
        Vessel vessel = new Vessel(harbor, ImmutableList
                .of(new Cargo("lumber", 500), new Cargo("steel", 2000)));
        vessel.save();
        Vessel loaded = runway.load(Vessel.class, vessel.id());
        Assert.assertNotNull(loaded.home);
        Assert.assertEquals("harbor", loaded.home.name);
        Assert.assertEquals(2, loaded.cargo.size());
        Set<String> descriptions = loaded.cargo.stream().map(c -> c.description)
                .collect(Collectors.toSet());
        Assert.assertTrue(descriptions.contains("lumber"));
        Assert.assertTrue(descriptions.contains("steel"));
    }

    // ---------------------------------------------------------------
    // find() pipeline
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that the {@code find()} pipeline (which
     * uses a different {@code instantiateAll} code path than single-record
     * load) correctly populates {@link java.util.Collection
     * Collection&lt;Record&gt;} fields.
     * <p>
     * <strong>Start state:</strong> Two {@link Lock} instances saved to the
     * database, one with tag {@code "red"} and one with tag {@code "blue"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save two {@link Lock} instances with different tags and
     * distinct {@link Dock} lists.</li>
     * <li>Use {@code runway.find()} with a {@link Criteria} that matches only
     * the {@code "red"} {@link Lock}.</li>
     * <li>Assert that the matched {@link Lock} has its {@link Dock} collection
     * fully populated with correct values.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Exactly one {@link Lock} matches, its
     * {@code docks} list has two elements with values {@code "port"} and
     * {@code "starboard"}.
     */
    @Test
    public void testFindWithCriteriaPopulatesCollectionFields() {
        Lock red = new Lock(
                ImmutableList.of(new Dock("port"), new Dock("starboard")));
        red.tag = "red";
        Lock blue = new Lock(ImmutableList.of(new Dock("bow")));
        blue.tag = "blue";
        red.save();
        blue.save();
        Set<Lock> found = runway.find(Lock.class, Criteria.where().key("tag")
                .operator(Operator.EQUALS).value("red").build());
        Assert.assertEquals(1, found.size());
        Lock loaded = found.iterator().next();
        Assert.assertEquals(2, loaded.docks.size());
        Set<String> dockValues = loaded.docks.stream().map(d -> d.dock)
                .collect(Collectors.toSet());
        Assert.assertTrue(dockValues.contains("port"));
        Assert.assertTrue(dockValues.contains("starboard"));
    }

    // ---------------------------------------------------------------
    // Bulk loading
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that bulk loading multiple {@link Record
     * Records} with {@link java.util.Collection Collection&lt;Record&gt;}
     * fields populates every instance's collection with the correct field
     * values, not just non-empty collections.
     * <p>
     * <strong>Start state:</strong> Three {@link Lock} instances with known
     * {@link Dock} values saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save three {@link Lock} instances with 2, 1, and 3
     * {@link Dock} elements respectively.</li>
     * <li>Load all via {@code runway.load(Lock.class)}.</li>
     * <li>For each loaded {@link Lock}, verify the exact dock values match what
     * was saved.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Three {@link Lock} instances load with dock
     * counts 2, 1, and 3 respectively, and the union of all dock values matches
     * {@code {a, b, c, d, e, f}}.
     */
    @Test
    public void testBulkLoadPopulatesAllCollectionFieldValues() {
        Lock lock1 = new Lock(ImmutableList.of(new Dock("a"), new Dock("b")));
        Lock lock2 = new Lock(ImmutableList.of(new Dock("c")));
        Lock lock3 = new Lock(
                ImmutableList.of(new Dock("d"), new Dock("e"), new Dock("f")));
        lock1.save();
        lock2.save();
        lock3.save();
        Set<Lock> locks = runway.load(Lock.class);
        Assert.assertEquals(3, locks.size());
        Set<String> allDockValues = locks.stream()
                .flatMap(l -> l.docks.stream()).map(d -> d.dock)
                .collect(Collectors.toSet());
        Assert.assertEquals(6, allDockValues.size());
        Assert.assertTrue(allDockValues.contains("a"));
        Assert.assertTrue(allDockValues.contains("b"));
        Assert.assertTrue(allDockValues.contains("c"));
        Assert.assertTrue(allDockValues.contains("d"));
        Assert.assertTrue(allDockValues.contains("e"));
        Assert.assertTrue(allDockValues.contains("f"));
    }

    /**
     * <strong>Goal:</strong> Verify that when two parent {@link Record Records}
     * reference the same child {@link Record} in their
     * {@link java.util.Collection Collection&lt;Record&gt;} fields, the shared
     * child is loaded correctly for both parents.
     * <p>
     * <strong>Start state:</strong> Two {@link Lock} instances sharing a common
     * {@link Dock} saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a shared {@link Dock} and two {@link Lock} instances that each
     * include it in their docks list.</li>
     * <li>Save both {@link Lock} instances.</li>
     * <li>Bulk-load all {@link Lock} instances.</li>
     * <li>Assert that both loaded {@link Lock} instances contain the shared
     * {@link Dock} with the correct value.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both {@link Lock} instances have a dock with
     * value {@code "shared"}, and each also has its own unique dock.
     */
    @Test
    public void testBulkLoadWithSharedChildRecord() {
        Dock shared = new Dock("shared");
        Lock lock1 = new Lock(ImmutableList.of(shared, new Dock("only1")));
        Lock lock2 = new Lock(ImmutableList.of(shared, new Dock("only2")));
        lock1.save();
        lock2.save();
        Set<Lock> locks = runway.load(Lock.class);
        Assert.assertEquals(2, locks.size());
        for (Lock loaded : locks) {
            Assert.assertEquals(2, loaded.docks.size());
            Set<String> dockValues = loaded.docks.stream().map(d -> d.dock)
                    .collect(Collectors.toSet());
            Assert.assertTrue(dockValues.contains("shared"));
        }
    }

    // ---------------------------------------------------------------
    // Regression — no Collection<Record> fields
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that loading a {@link Record} without any
     * {@link java.util.Collection Collection&lt;Record&gt;} fields still works
     * correctly after the navigate prefetching changes (regression).
     * <p>
     * <strong>Start state:</strong> A {@link Simple} instance saved to the
     * database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Simple} with a known name.</li>
     * <li>Load it back via {@code runway.load(Simple.class, id)}.</li>
     * <li>Assert the name field is populated.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Simple} has name
     * {@code "plain"}.
     */
    @Test
    public void testLoadRecordWithoutCollectionFieldsStillWorks() {
        Simple simple = new Simple();
        simple.name = "plain";
        simple.save();
        Simple loaded = runway.load(Simple.class, simple.id());
        Assert.assertNotNull(loaded);
        Assert.assertEquals("plain", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that bulk-loading a class without
     * {@link java.util.Collection Collection&lt;Record&gt;} fields (where
     * {@code destinations} is {@code null}) returns all records correctly.
     * <p>
     * <strong>Start state:</strong> Multiple {@link Simple} instances saved to
     * the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save three {@link Simple} instances with different
     * names.</li>
     * <li>Load all via {@code runway.load(Simple.class)}.</li>
     * <li>Assert that all three load with correct field values.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Three {@link Simple} instances are returned
     * with names {@code "x"}, {@code "y"}, and {@code "z"}.
     */
    @Test
    public void testBulkLoadWithoutCollectionFieldsStillWorks() {
        Simple s1 = new Simple();
        s1.name = "x";
        Simple s2 = new Simple();
        s2.name = "y";
        Simple s3 = new Simple();
        s3.name = "z";
        s1.save();
        s2.save();
        s3.save();
        Set<Simple> all = runway.load(Simple.class);
        Assert.assertEquals(3, all.size());
        Set<String> names = all.stream().map(s -> s.name)
                .collect(Collectors.toSet());
        Assert.assertTrue(names.contains("x"));
        Assert.assertTrue(names.contains("y"));
        Assert.assertTrue(names.contains("z"));
    }

    // ---------------------------------------------------------------
    // BULK_SELECT correctness
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that
     * {@link CollectionPreSelectStrategy#BULK_SELECT} correctly loads
     * {@link java.util.Collection Collection&lt;Record&gt;} fields with the
     * same values as the default strategy.
     * <p>
     * <strong>Start state:</strong> A {@link Lock} with three {@link Dock
     * Docks} saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Lock} with three {@link Dock} elements.</li>
     * <li>Switch to {@link CollectionPreSelectStrategy#BULK_SELECT}.</li>
     * <li>Load the {@link Lock} and verify each {@link Dock Dock's} value.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded collection has three elements whose
     * {@code dock} values match the originals.
     */
    @Test
    public void testBulkSelectLoadsCollectionFieldsCorrectly() {
        Lock lock = new Lock(ImmutableList.of(new Dock("one"), new Dock("two"),
                new Dock("three")));
        lock.save();
        CollectionPreSelectStrategy previous = runway.collectionPreSelectStrategy;
        runway.collectionPreSelectStrategy = CollectionPreSelectStrategy.BULK_SELECT;
        try {
            Lock loaded = runway.load(Lock.class, lock.id());
            Assert.assertEquals(3, loaded.docks.size());
            Set<String> dockValues = loaded.docks.stream().map(d -> d.dock)
                    .collect(Collectors.toSet());
            Assert.assertTrue(dockValues.contains("one"));
            Assert.assertTrue(dockValues.contains("two"));
            Assert.assertTrue(dockValues.contains("three"));
        }
        finally {
            runway.collectionPreSelectStrategy = previous;
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link CollectionPreSelectStrategy#BULK_SELECT} correctly handles
     * multi-level link chains (two hops deep).
     * <p>
     * <strong>Start state:</strong> A {@link Vessel} with a {@link Port} and
     * {@link Cargo} saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Vessel} with both single and collection
     * {@link Record} fields.</li>
     * <li>Switch to {@link CollectionPreSelectStrategy#BULK_SELECT}.</li>
     * <li>Load the {@link Vessel} and verify both field types.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Vessel} has a populated
     * {@code home} {@link Port} and correct {@link Cargo} values.
     */
    @Test
    public void testBulkSelectHandlesMultiLevelLinks() {
        Port port = new Port("marina");
        Vessel vessel = new Vessel(port,
                ImmutableList.of(new Cargo("grain", 100)));
        vessel.save();
        CollectionPreSelectStrategy previous = runway.collectionPreSelectStrategy;
        runway.collectionPreSelectStrategy = CollectionPreSelectStrategy.BULK_SELECT;
        try {
            Vessel loaded = runway.load(Vessel.class, vessel.id());
            Assert.assertNotNull(loaded.home);
            Assert.assertEquals("marina", loaded.home.name);
            Assert.assertEquals(1, loaded.cargo.size());
            Assert.assertEquals("grain", loaded.cargo.get(0).description);
            Assert.assertEquals(100, loaded.cargo.get(0).weight);
        }
        finally {
            runway.collectionPreSelectStrategy = previous;
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link CollectionPreSelectStrategy#BULK_SELECT} correctly handles
     * self-referential {@link java.util.Collection Collection&lt;Record&gt;}
     * fields.
     * <p>
     * <strong>Start state:</strong> A {@link Node} graph where A has friend B.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a two-node graph.</li>
     * <li>Switch to {@link CollectionPreSelectStrategy#BULK_SELECT}.</li>
     * <li>Load the root and verify the friend is populated.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Node} has 1 friend with the
     * correct label.
     */
    @Test
    public void testBulkSelectHandlesSelfReferentialCollections() {
        Node a = new Node("alpha");
        Node b = new Node("beta");
        a.friends.add(b);
        a.save();
        CollectionPreSelectStrategy previous = runway.collectionPreSelectStrategy;
        runway.collectionPreSelectStrategy = CollectionPreSelectStrategy.BULK_SELECT;
        try {
            Node loaded = runway.load(Node.class, a.id());
            Assert.assertEquals(1, loaded.friends.size());
            Assert.assertEquals("beta", loaded.friends.get(0).label);
        }
        finally {
            runway.collectionPreSelectStrategy = previous;
        }
    }

    // ---------------------------------------------------------------
    // NAVIGATE single-record load
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that
     * {@link CollectionPreSelectStrategy#NAVIGATE} correctly populates
     * {@link java.util.Collection Collection&lt;Record&gt;} fields when loading
     * a single {@link Record} via {@code runway.load(Class, id)}.
     * <p>
     * <strong>Start state:</strong> A {@link Lock} with three {@link Dock
     * Docks} saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Lock} with three {@link Dock} elements.</li>
     * <li>Switch to {@link CollectionPreSelectStrategy#NAVIGATE}.</li>
     * <li>Load the {@link Lock} via single-record
     * {@code runway.load(Lock.class, id)}.</li>
     * <li>Verify each {@link Dock Dock's} value.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded collection has three elements whose
     * {@code dock} values match the originals.
     */
    @Test
    public void testNavigateSingleRecordLoadPopulatesCollectionFields() {
        Lock lock = new Lock(ImmutableList.of(new Dock("one"), new Dock("two"),
                new Dock("three")));
        lock.save();
        CollectionPreSelectStrategy previous = runway.collectionPreSelectStrategy;
        runway.collectionPreSelectStrategy = CollectionPreSelectStrategy.NAVIGATE;
        try {
            Lock loaded = runway.load(Lock.class, lock.id());
            Assert.assertEquals(3, loaded.docks.size());
            Set<String> dockValues = loaded.docks.stream().map(d -> d.dock)
                    .collect(Collectors.toSet());
            Assert.assertTrue(dockValues.contains("one"));
            Assert.assertTrue(dockValues.contains("two"));
            Assert.assertTrue(dockValues.contains("three"));
        }
        finally {
            runway.collectionPreSelectStrategy = previous;
        }
    }

    // ---------------------------------------------------------------
    // Builder wiring
    // ---------------------------------------------------------------

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway.Builder#collectionPreSelectStrategy(CollectionPreSelectStrategy)}
     * sets the strategy on the constructed {@link Runway}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Runway} with
     * {@link CollectionPreSelectStrategy#BULK_SELECT}.</li>
     * <li>Assert the field value.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code collectionPreSelectStrategy} is
     * {@link CollectionPreSelectStrategy#BULK_SELECT}.
     */
    @Test
    public void testBuilderSetsCollectionPreSelectStrategy() throws Exception {
        Runway custom = Runway.builder().port(server.getClientPort())
                .collectionPreSelectStrategy(
                        CollectionPreSelectStrategy.BULK_SELECT)
                .build();
        try {
            Assert.assertEquals(CollectionPreSelectStrategy.BULK_SELECT,
                    custom.collectionPreSelectStrategy);
        }
        finally {
            custom.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway.Builder#disablePreSelectLinkedRecords()} resets
     * {@code collectionPreSelectStrategy} to
     * {@link CollectionPreSelectStrategy#NONE}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Runway} with pre-select disabled.</li>
     * <li>Assert the strategy is {@code NONE}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code collectionPreSelectStrategy} is
     * {@link CollectionPreSelectStrategy#NONE}.
     */
    @Test
    public void testDisablePreSelectResetsStrategy() throws Exception {
        Runway custom = Runway.builder().port(server.getClientPort())
                .disablePreSelectLinkedRecords().build();
        try {
            Assert.assertEquals(CollectionPreSelectStrategy.NONE,
                    custom.collectionPreSelectStrategy);
        }
        finally {
            custom.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway.Builder#disablePreSelectLinkedRecords()} overrides an
     * explicit {@link CollectionPreSelectStrategy} set earlier in the
     * {@link Runway.Builder} chain.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Runway} setting
     * {@link CollectionPreSelectStrategy#NAVIGATE} followed by
     * {@link Runway.Builder#disablePreSelectLinkedRecords()}.</li>
     * <li>Assert the strategy is {@code NONE}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code collectionPreSelectStrategy} is
     * {@link CollectionPreSelectStrategy#NONE} because the disable call takes
     * precedence.
     */
    @Test
    public void testDisablePreSelectOverridesExplicitStrategy()
            throws Exception {
        Runway custom = Runway.builder().port(server.getClientPort())
                .collectionPreSelectStrategy(
                        CollectionPreSelectStrategy.NAVIGATE)
                .disablePreSelectLinkedRecords().build();
        try {
            Assert.assertEquals(CollectionPreSelectStrategy.NONE,
                    custom.collectionPreSelectStrategy);
        }
        finally {
            custom.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway.Builder#disablePreSelectLinkedRecords()} overrides an
     * explicit {@link CollectionPreSelectStrategy} even when the disable call
     * comes <em>after</em> the explicit strategy in the builder chain.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Runway} calling
     * {@link Runway.Builder#disablePreSelectLinkedRecords()} after
     * {@link Runway.Builder#collectionPreSelectStrategy(CollectionPreSelectStrategy)
     * collectionPreSelectStrategy(NAVIGATE)}.</li>
     * <li>Assert the strategy is {@code NONE}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code collectionPreSelectStrategy} is
     * {@link CollectionPreSelectStrategy#NONE} because the disable call takes
     * precedence regardless of ordering.
     */
    @Test
    public void testDisablePreSelectOverridesExplicitStrategyReverseOrder()
            throws Exception {
        Runway custom = Runway.builder().port(server.getClientPort())
                .disablePreSelectLinkedRecords().collectionPreSelectStrategy(
                        CollectionPreSelectStrategy.NAVIGATE)
                .build();
        try {
            Assert.assertEquals(CollectionPreSelectStrategy.NONE,
                    custom.collectionPreSelectStrategy);
        }
        finally {
            custom.close();
        }
    }

    // ---------------------------------------------------------------
    // Model classes
    // ---------------------------------------------------------------

    // NOTE: Performance benchmark tests were removed because
    // timing-based assertions are inherently flaky on localhost
    // where server work dominates latency. The optimization
    // benefit is proven over network round trips.

    /**
     * A {@link Record} with a {@link java.util.Collection
     * Collection&lt;Record&gt;} field for testing navigate prefetching of
     * linked {@link Dock Docks}.
     */
    class Lock extends Record {

        /**
         * The collection of {@link Dock Docks} linked to this {@link Lock}.
         */
        public final List<Dock> docks;

        /**
         * An optional tag for criteria-based filtering.
         */
        public String tag;

        /**
         * Construct a new instance.
         *
         * @param docks the {@link Dock Docks} to associate
         */
        public Lock(List<Dock> docks) {
            this.docks = docks;
        }
    }

    /**
     * A simple {@link Record} that serves as the element type in {@link Lock
     * Lock's} collection field.
     */
    class Dock extends Record {

        /**
         * The dock value.
         */
        public final String dock;

        /**
         * Construct a new instance.
         *
         * @param dock the dock value
         */
        public Dock(String dock) {
            this.dock = dock;
        }
    }

    /**
     * A self-referential {@link Record} used to test navigate path computation
     * and loading for recursive {@link java.util.Collection
     * Collection&lt;Record&gt;} structures.
     */
    class Node extends Record {

        /**
         * A human-readable label for this {@link Node}.
         */
        public String label;

        /**
         * The {@link Node Nodes} that are friends of this {@link Node}.
         */
        public List<Node> friends = Lists.newArrayList();

        /**
         * Construct a new instance.
         *
         * @param label the label for this {@link Node}
         */
        public Node(String label) {
            this.label = label;
        }
    }

    /**
     * A {@link Record} with both a single {@link Record} field ({@code home})
     * and a {@link java.util.Collection Collection&lt;Record&gt;} field
     * ({@code cargo}), used to verify both optimization paths work together.
     */
    class Vessel extends Record {

        /**
         * The home {@link Port} for this {@link Vessel}.
         */
        public Port home;

        /**
         * The {@link Cargo} carried by this {@link Vessel}.
         */
        public final List<Cargo> cargo;

        /**
         * Construct a new instance.
         *
         * @param home the home {@link Port}
         * @param cargo the {@link Cargo} items
         */
        public Vessel(Port home, List<Cargo> cargo) {
            this.home = home;
            this.cargo = cargo;
        }
    }

    /**
     * A simple {@link Record} representing a port, used as a single
     * {@link Record} field in {@link Vessel}.
     */
    class Port extends Record {

        /**
         * The port name.
         */
        public final String name;

        /**
         * Construct a new instance.
         *
         * @param name the port name
         */
        public Port(String name) {
            this.name = name;
        }
    }

    /**
     * A {@link Record} representing cargo, used as the element type in
     * {@link Vessel Vessel's} collection field.
     */
    class Cargo extends Record {

        /**
         * A description of this {@link Cargo}.
         */
        public final String description;

        /**
         * The weight of this {@link Cargo} in kilograms.
         */
        public final int weight;

        /**
         * Construct a new instance.
         *
         * @param description the cargo description
         * @param weight the cargo weight in kilograms
         */
        public Cargo(String description, int weight) {
            this.description = description;
            this.weight = weight;
        }
    }

    /**
     * A {@link Record} with no {@link java.util.Collection
     * Collection&lt;Record&gt;} fields, used to verify that the navigate
     * prefetching changes do not regress loading for plain {@link Record
     * Records}.
     */
    class Simple extends Record {

        /**
         * A simple string field.
         */
        public String name;
    }

}
