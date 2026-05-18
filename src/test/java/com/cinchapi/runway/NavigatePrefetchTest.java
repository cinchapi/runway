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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.CountingConcourseConnectionPool.CountingConcourse;
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
     * <strong>Goal:</strong> Verify that navigate paths for a self-referential
     * {@link java.util.Collection Collection&lt;Record&gt;} field emit the
     * {@code *} transitive modifier and do not re-emit the same cyclic edge in
     * the lineage.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Node}, which has a
     * {@code List<Node>} self-referential field.</li>
     * <li>Assert that the paths use the {@code *} modifier on the cyclic field
     * name.</li>
     * <li>Assert that the bare (non-{@code *}) cyclic-field prefix is
     * absent.</li>
     * <li>Assert that the same cyclic edge does not appear with a chained
     * {@code *} suffix.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Paths include {@code friends*._},
     * {@code friends*.$id$}, {@code friends*.label}, and the terminal
     * {@code friends*.friends} Link value. The bare {@code friends._} is not
     * present, and no path begins with {@code friends*.friends*.}.
     */
    @Test
    public void testNavigatePathsForSelfReferentialCollectionField() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Node.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("friends*._"));
        Assert.assertTrue(navigatePaths.contains("friends*.$id$"));
        Assert.assertTrue(navigatePaths.contains("friends*.label"));
        Assert.assertTrue(navigatePaths.contains("friends*.friends"));
        Assert.assertFalse(navigatePaths.contains("friends._"));
        long deepStarPaths = navigatePaths.stream()
                .filter(p -> p.startsWith("friends*.friends*.")).count();
        Assert.assertEquals(0, deepStarPaths);
    }

    /**
     * <strong>Goal:</strong> Verify that navigate paths recurse through a
     * non-cyclic {@link java.util.Collection Collection&lt;Record&gt;} field
     * into the destination type's own {@link Collection
     * Collection&lt;Record&gt;} fields and emit the {@code *} modifier on
     * cyclic stops at the destination.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Conversation}, which has
     * {@code root: List<Exchange>}.</li>
     * <li>Assert that {@link Exchange Exchange's} non-cyclic
     * single-{@link Record} fields ({@code prompt}, {@code response}) appear
     * under the {@code root.} prefix.</li>
     * <li>Assert that {@link Exchange Exchange's} cyclic {@code children:
     * Set<Exchange>} field emits {@code root.children*.} paths.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Paths include {@code root._},
     * {@code root.prompt._}, {@code root.prompt.text}, {@code root.response._},
     * {@code root.children*._}, {@code root.children*.$id$}, and
     * {@code root.children*.children}; no {@code root.children._} appears
     * without the {@code *}.
     */
    @Test
    public void testNavigatePathsForConversationWithCyclicChildren() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Conversation.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("root._"));
        Assert.assertTrue(navigatePaths.contains("root.$id$"));
        Assert.assertTrue(navigatePaths.contains("root.prompt._"));
        Assert.assertTrue(navigatePaths.contains("root.prompt.text"));
        Assert.assertTrue(navigatePaths.contains("root.response._"));
        Assert.assertTrue(navigatePaths.contains("root.children*._"));
        Assert.assertTrue(navigatePaths.contains("root.children*.$id$"));
        Assert.assertTrue(navigatePaths.contains("root.children*.children"));
        Assert.assertFalse(navigatePaths.contains("root.children._"));
    }

    /**
     * <strong>Goal:</strong> Verify that navigate paths recurse through a
     * non-cyclic single-{@link Record} field to discover and enumerate
     * {@link java.util.Collection Collection&lt;Record&gt;} fields nested
     * behind it.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Document}, which has
     * {@code metadata: Metadata} (single-{@link Record}) and {@link Metadata}
     * has {@code tags: List<TagRecord>} (Collection&lt;Record&gt;).</li>
     * <li>Assert that the nested {@code metadata.tags.*} paths are
     * emitted.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Paths include {@code metadata.tags._},
     * {@code metadata.tags.$id$}, {@code metadata.tags._realms}, and
     * {@code metadata.tags.label}.
     */
    @Test
    public void testNavigatePathsRecurseThroughSingleRecordEdge() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Document.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("metadata.tags._"));
        Assert.assertTrue(navigatePaths.contains("metadata.tags.$id$"));
        Assert.assertTrue(navigatePaths.contains("metadata.tags._realms"));
        Assert.assertTrue(navigatePaths.contains("metadata.tags.label"));
    }

    /**
     * <strong>Goal:</strong> Verify that a non-cyclic
     * {@link java.util.Collection Collection&lt;Record&gt;} field reached under
     * a transitive ({@code *}) stop is still fully enumerated.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Conversation}, which transitively
     * reaches {@link Exchange Exchange's} non-cyclic {@code citations:
     * List<Citation>} field via {@code root.children*}.</li>
     * <li>Assert that {@code root.children*.citations.*} paths are
     * emitted.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Paths include
     * {@code root.children*.citations._},
     * {@code root.children*.citations.$id$}, and
     * {@code root.children*.citations.source}.
     */
    @Test
    public void testNavigatePathsForNonCyclicCollectionUnderTransitiveStop() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Conversation.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("root.children*.citations._"));
        Assert.assertTrue(
                navigatePaths.contains("root.children*.citations.$id$"));
        Assert.assertTrue(
                navigatePaths.contains("root.children*.citations.source"));
    }

    /**
     * <strong>Goal:</strong> Verify that a cyclic single-{@link Record} field
     * on the navigate-root class emits {@code *}-suffixed paths from
     * {@code computeNavigatePaths} itself.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Exchange}, which has both
     * {@code parent: Exchange} (cyclic single-{@link Record}) and
     * {@code children: List<Exchange>} (cyclic Collection&lt;Record&gt;).</li>
     * <li>Assert that both {@code parent*.X} and {@code children*.X} paths are
     * present in the result.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Paths include {@code parent*._},
     * {@code parent*.$id$}, {@code parent*.text}, {@code children*._},
     * {@code children*.$id$}, and {@code children*.text}.
     */
    @Test
    public void testNavigatePathsForCyclicSingleRecordAtRoot() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Exchange.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("parent*._"));
        Assert.assertTrue(navigatePaths.contains("parent*.$id$"));
        Assert.assertTrue(navigatePaths.contains("parent*.text"));
        Assert.assertTrue(navigatePaths.contains("children*._"));
        Assert.assertTrue(navigatePaths.contains("children*.$id$"));
        Assert.assertTrue(navigatePaths.contains("children*.text"));
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code visitedEdges} set permits
     * crossing different cyclic edges in the same lineage but blocks
     * re-emission of the same cyclic edge.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Exchange}, which has two cyclic
     * edges: a single-{@link Record} {@code parent: Exchange} and a
     * {@link java.util.Collection Collection&lt;Record&gt;} {@code children:
     * List<Exchange>}.</li>
     * <li>Assert that paths crossing the two distinct edges in either order are
     * emitted.</li>
     * <li>Assert that no path re-emits the same cyclic edge twice in its
     * lineage.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> At least one path begins with
     * {@code parent*.children*.} and at least one with
     * {@code children*.parent*.}; no path begins with {@code parent*.parent*.}
     * or {@code children*.children*.}.
     */
    @Test
    public void testNavigatePathsCrossEdgesButNotSameEdgeUnderTransitiveStop() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Exchange.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.stream()
                .anyMatch(p -> p.startsWith("parent*.children*.")));
        Assert.assertTrue(navigatePaths.stream()
                .anyMatch(p -> p.startsWith("children*.parent*.")));
        Assert.assertTrue(navigatePaths.stream()
                .noneMatch(p -> p.startsWith("parent*.parent*.")));
        Assert.assertTrue(navigatePaths.stream()
                .noneMatch(p -> p.startsWith("children*.children*.")));
    }

    /**
     * <strong>Goal:</strong> Verify that a cyclic single-{@link Record} field
     * encountered deep inside a non-cyclic chain emits {@code *}-suffixed paths
     * through {@code computePaths}.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Conversation}, which reaches
     * {@link Exchange Exchange's} cyclic {@code parent: Exchange} field via
     * {@code root}.</li>
     * <li>Assert that {@code root.parent*.X} paths are emitted, including the
     * terminal bare {@code root.parent*.parent} Link value.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Paths include {@code root.parent},
     * {@code root.parent*._}, {@code root.parent*.$id$},
     * {@code root.parent*.text}, and the terminal {@code root.parent*.parent}.
     */
    @Test
    public void testNavigatePathsForCyclicSingleRecordInChain() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Conversation.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("root.parent"));
        Assert.assertTrue(navigatePaths.contains("root.parent*._"));
        Assert.assertTrue(navigatePaths.contains("root.parent*.$id$"));
        Assert.assertTrue(navigatePaths.contains("root.parent*.text"));
        Assert.assertTrue(navigatePaths.contains("root.parent*.parent"));
    }

    /**
     * <strong>Goal:</strong> Verify that the select-side path set is
     * byte-identical after the transitive-modifier changes, so that the
     * select-side consumer's lookup of {@code <field>.$id$} still resolves
     * pre-fetched data.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve select-side paths for {@link Exchange}, which has a cyclic
     * {@code parent: Exchange} field.</li>
     * <li>Assert that no path contains the {@code *} modifier.</li>
     * <li>Assert that the {@code parent} field is present as a bare leaf in the
     * path set.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code parent} is present as a bare leaf; no
     * path matches {@code parent*.*}.
     */
    @Test
    public void testSelectSidePathsUnchangedForCyclicSingleRecord() {
        Set<String> paths = Record.StaticAnalysis.instance()
                .getPaths(Exchange.class);
        Assert.assertNotNull(paths);
        Assert.assertTrue(paths.contains("parent"));
        long starPaths = paths.stream().filter(p -> p.contains("*")).count();
        Assert.assertEquals(0, starPaths);
    }

    /**
     * <strong>Goal:</strong> Verify that the navigate gate fires for a class
     * whose only {@link Record}-typed field is a cyclic single-{@link Record}
     * self-reference (no direct {@link java.util.Collection
     * Collection&lt;Record&gt;} field).
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Runway#getNavigatePathsForClassIfSupported(Class)} for
     * {@link TreeNode}, which has only a cyclic {@code parent: TreeNode}
     * field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned path set is non-{@code null} and
     * contains {@code parent*._}.
     */
    @Test
    public void testNavigateGateFiresForCyclicSingleRecordOnlyClass() {
        Set<String> paths = runway
                .getNavigatePathsForClassIfSupported(TreeNode.class);
        Assert.assertNotNull(paths);
        Assert.assertTrue(paths.contains("parent*._"));
    }

    /**
     * <strong>Goal:</strong> Verify that the navigate gate fires for a class
     * whose pre-fetchable destinations are only reachable through a non-cyclic
     * single-{@link Record} edge (no direct {@link java.util.Collection
     * Collection&lt;Record&gt;} field).
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Runway#getNavigatePathsForClassIfSupported(Class)} for
     * {@link Document}, which has {@code metadata: Metadata} and
     * {@link Metadata} contains {@code tags: List<TagRecord>}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned path set is non-{@code null} and
     * contains {@code metadata.tags._}.
     */
    @Test
    public void testNavigateGateFiresForClassReachingCollectionThroughSingleRecord() {
        Set<String> paths = runway
                .getNavigatePathsForClassIfSupported(Document.class);
        Assert.assertNotNull(paths);
        Assert.assertTrue(paths.contains("metadata.tags._"));
    }

    /**
     * <strong>Goal:</strong> Verify that the navigate gate returns {@code null}
     * for classes whose pre-fetchable destination set is empty (no
     * {@link Record}-typed fields at all).
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Runway#getNavigatePathsForClassIfSupported(Class)} for
     * {@link Simple}, which has only a {@link String} field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned value is {@code null}.
     */
    @Test
    public void testNavigateGateReturnsNullForClassWithoutDestinations() {
        Assert.assertNull(
                runway.getNavigatePathsForClassIfSupported(Simple.class));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#getNavigatePathsForClassIfSupported(Class)} drops
     * transitive ({@code *}) paths but keeps non-transitive paths when the
     * connected server does not support transitive navigation.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} connected to a server that
     * supports transitive navigation.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve the navigate paths for {@link Conversation} and confirm at
     * least one bears the {@code *} modifier.</li>
     * <li>Reflectively clear the {@code supportsTransitiveNavigation}
     * flag.</li>
     * <li>Retrieve the navigate paths for {@link Conversation} again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second result contains no {@code *} path
     * yet still includes the non-transitive {@code root.prompt.text} path.
     */
    @Test
    public void testNavigateGateOmitsTransitivePathsWhenUnsupported() {
        Set<String> supported = runway
                .getNavigatePathsForClassIfSupported(Conversation.class);
        Assert.assertNotNull(supported);
        Assert.assertTrue(supported.stream().anyMatch(p -> p.contains("*")));
        // (authorized)
        Reflection.set("supportsTransitiveNavigation", false, runway);
        Set<String> unsupported = runway
                .getNavigatePathsForClassIfSupported(Conversation.class);
        Assert.assertNotNull(unsupported);
        Assert.assertTrue(unsupported.stream().noneMatch(p -> p.contains("*")));
        Assert.assertTrue(unsupported.contains("root.prompt.text"));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#getNavigatePathsForClassHierarchyIfSupported(Class)}
     * returns {@code null} for a class whose every navigate path is transitive
     * when the connected server does not support transitive navigation.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} connected to a server that
     * supports transitive navigation.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Reflectively clear the {@code supportsTransitiveNavigation}
     * flag.</li>
     * <li>Retrieve the hierarchy navigate paths for {@link Node}, whose only
     * navigate paths bear the {@code *} modifier.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned value is {@code null}, since
     * dropping the transitive paths leaves nothing to navigate.
     */
    @Test
    public void testNavigateGateHierarchyNullWhenTransitiveUnsupported() {
        // (authorized)
        Reflection.set("supportsTransitiveNavigation", false, runway);
        Set<String> paths = runway
                .getNavigatePathsForClassHierarchyIfSupported(Node.class);
        Assert.assertNull(paths);
    }

    /**
     * <strong>Goal:</strong> Verify that loading a cyclic single-{@link Record}
     * chain populates every level of the chain via the {@code *} transitive
     * modifier.
     * <p>
     * <strong>Start state:</strong> A three-level {@link TreeNode} chain (leaf
     * &rarr; mid &rarr; root) saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a chain of three {@link TreeNode TreeNodes}.</li>
     * <li>Load the leaf {@link TreeNode} and verify that every ancestor is
     * populated.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded leaf's {@code parent} chain has
     * every level populated with the correct names.
     */
    @Test
    public void testNavigateStrategyPopulatesCyclicSingleRecordChain() {
        TreeNode root = new TreeNode();
        root.name = "root";
        TreeNode mid = new TreeNode();
        mid.name = "mid";
        mid.parent = root;
        TreeNode leaf = new TreeNode();
        leaf.name = "leaf";
        leaf.parent = mid;
        leaf.save();
        TreeNode loadedLeaf = runway.load(TreeNode.class, leaf.id());
        Assert.assertEquals("leaf", loadedLeaf.name);
        Assert.assertNotNull(loadedLeaf.parent);
        Assert.assertEquals("mid", loadedLeaf.parent.name);
        Assert.assertNotNull(loadedLeaf.parent.parent);
        Assert.assertEquals("root", loadedLeaf.parent.parent.name);
    }

    /**
     * <strong>Goal:</strong> Verify that loading a self-referential
     * {@link java.util.Collection Collection&lt;Record&gt;} chain populates
     * every level of the chain via the {@code *} transitive modifier in a
     * single navigate RPC.
     * <p>
     * <strong>Start state:</strong> A three-level {@link Node} chain (a &rarr;
     * b &rarr; c) saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a chain of {@link Node Nodes} where each {@link Node}
     * has exactly one friend at the next level.</li>
     * <li>Load the root {@link Node} and verify that the entire chain is
     * populated through every level.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The root {@link Node Node's} {@code friends}
     * chain has every level populated with the correct labels.
     */
    @Test
    public void testNavigateStrategyPopulatesDeepSelfReferentialChain() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        b.friends.add(c);
        a.save();
        Node loadedA = runway.load(Node.class, a.id());
        Assert.assertEquals(1, loadedA.friends.size());
        Node loadedB = loadedA.friends.get(0);
        Assert.assertEquals("b", loadedB.label);
        Assert.assertEquals(1, loadedB.friends.size());
        Assert.assertEquals("c", loadedB.friends.get(0).label);
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

    /**
     * <strong>Goal:</strong> Verify that navigate pre-fetch correctly populates
     * {@link java.util.Collection Collection&lt;Record&gt;} fields when loading
     * a single {@link Record} via {@code runway.load(Class, id)}.
     * <p>
     * <strong>Start state:</strong> A {@link Lock} with three {@link Dock
     * Docks} saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save a {@link Lock} with three {@link Dock} elements.</li>
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
        Lock loaded = runway.load(Lock.class, lock.id());
        Assert.assertEquals(3, loaded.docks.size());
        Set<String> dockValues = loaded.docks.stream().map(d -> d.dock)
                .collect(Collectors.toSet());
        Assert.assertTrue(dockValues.contains("one"));
        Assert.assertTrue(dockValues.contains("two"));
        Assert.assertTrue(dockValues.contains("three"));
    }

    /**
     * <strong>Goal:</strong> Verify that the unified resolve path populates a
     * mutual-reference graph deeper than static path enumeration can reach, so
     * the cleanup pass that follows the {@code navigate()} call actually fills
     * in the tail.
     * <p>
     * <strong>Start state:</strong> A mutual chain of {@link Alpha Alphas} and
     * {@link Beta Betas} of length seven
     * ({@code A1 -> B1 -> A2 -> B2 -> A3 -> B3 -> A4}) saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the chain through {@code A1}.</li>
     * <li>Load {@code A1} and walk every level of the chain.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Every {@link Alpha} and {@link Beta} along the
     * chain loads with its correct label, including the deep tail that the
     * static path enumeration alone cannot cover.
     */
    @Test
    public void testNavigateStrategyPopulatesDeepMutualReferenceChain() {
        Alpha a1 = new Alpha();
        a1.label = "a1";
        Beta b1 = new Beta();
        b1.label = "b1";
        Alpha a2 = new Alpha();
        a2.label = "a2";
        Beta b2 = new Beta();
        b2.label = "b2";
        Alpha a3 = new Alpha();
        a3.label = "a3";
        Beta b3 = new Beta();
        b3.label = "b3";
        Alpha a4 = new Alpha();
        a4.label = "a4";
        a1.betas.add(b1);
        b1.alphas.add(a2);
        a2.betas.add(b2);
        b2.alphas.add(a3);
        a3.betas.add(b3);
        b3.alphas.add(a4);
        a1.save();
        Alpha loadedA1 = runway.load(Alpha.class, a1.id());
        Assert.assertEquals("a1", loadedA1.label);
        Beta loadedB1 = loadedA1.betas.get(0);
        Assert.assertEquals("b1", loadedB1.label);
        Alpha loadedA2 = loadedB1.alphas.get(0);
        Assert.assertEquals("a2", loadedA2.label);
        Beta loadedB2 = loadedA2.betas.get(0);
        Assert.assertEquals("b2", loadedB2.label);
        Alpha loadedA3 = loadedB2.alphas.get(0);
        Assert.assertEquals("a3", loadedA3.label);
        Beta loadedB3 = loadedA3.betas.get(0);
        Assert.assertEquals("b3", loadedB3.label);
        Alpha loadedA4 = loadedB3.alphas.get(0);
        Assert.assertEquals("a4", loadedA4.label);
    }

    /**
     * <strong>Goal:</strong> Verify that the unified resolve path supports
     * untyped loads &mdash; each record's class is recovered from its section
     * key, then a class-aware {@code navigate()} dispatches for the grouped
     * ids, and linked destinations are populated.
     * <p>
     * <strong>Start state:</strong> A {@link Lock} with three {@link Dock
     * Docks} saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the {@link Lock} with a distinguishing tag.</li>
     * <li>Invoke {@code findAny(Lock.class, criteria)}, which takes the untyped
     * {@code instantiateAll(Set)} path with {@code any=true}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The matching {@link Lock} loads with every
     * {@link Dock} populated to its original value.
     */
    @Test
    public void testNavigateStrategySupportsUntypedLoadAnyDispatch() {
        Lock lock = new Lock(ImmutableList.of(new Dock("alpha"),
                new Dock("beta"), new Dock("gamma")));
        lock.tag = "untyped-lock-tag";
        lock.save();
        Set<Lock> loaded = runway.findAny(Lock.class,
                Criteria.where().key("tag").operator(Operator.EQUALS)
                        .value("untyped-lock-tag").build());
        Assert.assertEquals(1, loaded.size());
        Lock loadedLock = loaded.iterator().next();
        Assert.assertEquals(3, loadedLock.docks.size());
        Set<String> dockValues = loadedLock.docks.stream().map(d -> d.dock)
                .collect(Collectors.toSet());
        Assert.assertTrue(dockValues.contains("alpha"));
        Assert.assertTrue(dockValues.contains("beta"));
        Assert.assertTrue(dockValues.contains("gamma"));
    }

    /**
     * <strong>Goal:</strong> Verify that navigate path computation terminates
     * and emits the {@code *} transitive modifier for a polymorphic
     * {@link Record} whose self-referential edges are declared with a base type
     * that has subtypes.
     * <p>
     * <strong>Start state:</strong> Default {@link Record.StaticAnalysis}
     * instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Retrieve navigate paths for {@link Manager}, whose
     * single-{@link Record} {@code supervisor} and {@link java.util.Collection
     * Collection&lt;Record&gt;} {@code directReports} fields are both declared
     * as the base type {@link Employee}, which {@link Manager} extends.</li>
     * <li>Assert that both edges emit the {@code *} transitive modifier.</li>
     * <li>Assert that neither edge re-emits itself under a chained {@code *}
     * suffix.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Paths include {@code supervisor*.name} and
     * {@code directReports*.name}; no path begins with
     * {@code supervisor*.supervisor*.} or
     * {@code directReports*.directReports*.}. Computation terminates without a
     * {@link StackOverflowError}.
     */
    @Test
    public void testNavigatePathsForPolymorphicSelfReferentialRecord() {
        Set<String> navigatePaths = Record.StaticAnalysis.instance()
                .getNavigatePaths(Manager.class);
        Assert.assertNotNull(navigatePaths);
        Assert.assertTrue(navigatePaths.contains("supervisor*.name"));
        Assert.assertTrue(navigatePaths.contains("directReports*.name"));
        Assert.assertTrue(navigatePaths.stream()
                .noneMatch(p -> p.startsWith("supervisor*.supervisor*.")));
        Assert.assertTrue(navigatePaths.stream().noneMatch(
                p -> p.startsWith("directReports*.directReports*.")));
    }

    /**
     * <strong>Goal:</strong> Verify that the number of read RPCs a load issues
     * does not grow with the depth of a self-referential
     * {@link java.util.Collection Collection&lt;Record&gt;} chain.
     * <p>
     * <strong>Start state:</strong> A two-{@link Node} chain and a
     * six-{@link Node} chain saved to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a shallow chain and a deep chain of {@link Node Nodes} linked
     * through the {@code friends} collection.</li>
     * <li>Reflectively replace the {@link Runway} connection pool with a
     * {@link CountingConcourseConnectionPool} that tallies read RPCs.</li>
     * <li>Load the root of each chain, recording the RPC count for each.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The deep chain loads fully, and its load
     * issues the same number of read RPCs as the shallow chain &mdash; proving
     * the prefetch collapses N+1 loading into a depth-independent constant.
     */
    @Test
    public void testLoadRpcCountIsIndependentOfChainDepth() {
        Node s1 = new Node("s1");
        Node s2 = new Node("s2");
        s1.friends.add(s2);
        s1.save();
        Node d1 = new Node("d1");
        Node d2 = new Node("d2");
        Node d3 = new Node("d3");
        Node d4 = new Node("d4");
        Node d5 = new Node("d5");
        Node d6 = new Node("d6");
        d1.friends.add(d2);
        d2.friends.add(d3);
        d3.friends.add(d4);
        d4.friends.add(d5);
        d5.friends.add(d6);
        d1.save();
        ConnectionPool pool = new CountingConcourseConnectionPool(
                Concourse.connect("localhost", server.getClientPort(), "admin",
                        "admin"));
        Reflection.set("connections", pool, runway); // (authorized)
        Concourse connection = pool.request();
        AtomicInteger rpcs = ((CountingConcourse) connection).rpcs();
        pool.release(connection);
        rpcs.set(0);
        runway.load(Node.class, s1.id());
        int shallow = rpcs.get();
        rpcs.set(0);
        Node deep = runway.load(Node.class, d1.id());
        int deepRpcs = rpcs.get();
        Node node = deep;
        for (String label : new String[] { "d2", "d3", "d4", "d5", "d6" }) {
            node = node.friends.get(0);
            Assert.assertEquals(label, node.label);
        }
        Assert.assertTrue(shallow > 0);
        Assert.assertEquals(shallow, deepRpcs);
    }

    /**
     * <strong>Goal:</strong> Verify that the number of read RPCs a multi-record
     * {@code find()} issues does not grow with the depth of the
     * self-referential {@link java.util.Collection Collection&lt;Record&gt;}
     * chains reachable from the matched {@link Node Nodes}.
     * <p>
     * <strong>Start state:</strong> Three {@link Node Nodes} with a
     * single-friend chain and three {@link Node Nodes} with a five-deep friend
     * chain saved to the database, distinguished by label.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@code "shallow"} roots, each linked to one friend, and
     * three {@code "deep"} roots, each linked to a five-deep friend chain.</li>
     * <li>Reflectively replace the {@link Runway} connection pool with a
     * {@link CountingConcourseConnectionPool} that tallies read RPCs.</li>
     * <li>Run a {@code find()} for the shallow roots and a {@code find()} for
     * the deep roots, walking every matched chain and recording the RPC count
     * of each.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The deep {@code find()} fully populates every
     * matched chain and issues the same number of read RPCs as the shallow
     * {@code find()} &mdash; proving the criteria query and its navigate
     * pre-fetch stay depth-independent.
     */
    @Test
    public void testFindRpcCountIsIndependentOfChainDepth() {
        for (int i = 0; i < 3; ++i) {
            Node root = new Node("shallow");
            root.friends.add(new Node("shallowfriend"));
            root.save();
        }
        for (int i = 0; i < 3; ++i) {
            Node root = new Node("deep");
            Node tail = root;
            for (int depth = 0; depth < 5; ++depth) {
                Node next = new Node("deepfriend");
                tail.friends.add(next);
                tail = next;
            }
            root.save();
        }
        ConnectionPool pool = new CountingConcourseConnectionPool(
                Concourse.connect("localhost", server.getClientPort(), "admin",
                        "admin"));
        Reflection.set("connections", pool, runway); // (authorized)
        Concourse connection = pool.request();
        AtomicInteger rpcs = ((CountingConcourse) connection).rpcs();
        pool.release(connection);
        rpcs.set(0);
        Set<Node> shallowRoots = runway.find(Node.class,
                Criteria.where().key("label").operator(Operator.EQUALS)
                        .value("shallow").build());
        for (Node root : shallowRoots) {
            Assert.assertEquals(1, root.friends.size());
        }
        int shallow = rpcs.get();
        rpcs.set(0);
        Set<Node> deepRoots = runway.find(Node.class, Criteria.where()
                .key("label").operator(Operator.EQUALS).value("deep").build());
        for (Node root : deepRoots) {
            Node node = root;
            for (int depth = 0; depth < 5; ++depth) {
                Assert.assertEquals(1, node.friends.size());
                node = node.friends.get(0);
            }
        }
        int deep = rpcs.get();
        Assert.assertEquals(3, shallowRoots.size());
        Assert.assertEquals(3, deepRoots.size());
        Assert.assertTrue(shallow > 0);
        Assert.assertEquals(shallow, deep);
    }

    /**
     * <strong>Goal:</strong> Verify that a paginated load whose page falls
     * beyond the result set returns an empty {@link Set} rather than throwing,
     * for a class that has navigate paths.
     * <p>
     * <strong>Start state:</strong> A single {@link Lock} saved to the
     * database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save one {@link Lock}.</li>
     * <li>Load {@link Lock} with a {@link Page} that begins past the only saved
     * record.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Set} is empty; the empty
     * page does not drive a {@code navigate()} from zero records.
     */
    @Test
    public void testPaginatedLoadBeyondResultsReturnsEmpty() {
        Lock lock = new Lock(ImmutableList.of(new Dock("solo")));
        lock.save();
        Set<Lock> page = runway.load(Lock.class, Page.limit(10).goTo(2));
        Assert.assertTrue(page.isEmpty());
    }

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

    /**
     * The root container in the conversation graph used to test transitive
     * navigation across non-cyclic and cyclic edges combined.
     */
    class Conversation extends Record {

        /**
         * The top-level {@link Exchange Exchanges} in this
         * {@link Conversation}.
         */
        public List<Exchange> root = Lists.newArrayList();
    }

    /**
     * A self-referential {@link Record} with both a cyclic
     * single-{@link Record} field ({@code parent}) and a cyclic
     * {@link java.util.Collection Collection&lt;Record&gt;} field
     * ({@code children}), used to exercise transitive navigation for both kinds
     * of cyclic edges plus non-cyclic edges to {@link Prompt},
     * {@link Response}, and {@link Citation}.
     */
    class Exchange extends Record {

        /**
         * The parent {@link Exchange} in the conversation tree, or {@code null}
         * for a root {@link Exchange}.
         */
        public Exchange parent;

        /**
         * The child {@link Exchange Exchanges} in the conversation tree.
         */
        public List<Exchange> children = Lists.newArrayList();

        /**
         * The {@link Prompt} that initiated this {@link Exchange}.
         */
        public Prompt prompt;

        /**
         * The {@link Response} produced by this {@link Exchange}.
         */
        public Response response;

        /**
         * Supporting {@link Citation Citations} for this {@link Exchange}.
         */
        public List<Citation> citations = Lists.newArrayList();

        /**
         * A human-readable identifier for this {@link Exchange}.
         */
        public String text;
    }

    /**
     * A non-cyclic single-{@link Record} target of {@link Exchange#prompt},
     * used to verify that non-cyclic single-{@link Record} edges are walked by
     * {@code computePaths}.
     */
    class Prompt extends Record {

        /**
         * The prompt text.
         */
        public String text;
    }

    /**
     * A non-cyclic single-{@link Record} target of {@link Exchange#response},
     * used to verify that non-cyclic single-{@link Record} edges are walked by
     * {@code computePaths}.
     */
    class Response extends Record {

        /**
         * The response text.
         */
        public String text;
    }

    /**
     * A non-cyclic {@link java.util.Collection Collection&lt;Record&gt;}
     * element type used to verify that non-cyclic collections reached under a
     * transitive ({@code *}) stop are still enumerated.
     */
    class Citation extends Record {

        /**
         * The citation source identifier.
         */
        public String source;
    }

    /**
     * A {@link Record} with a single-{@link Record} edge into a type that has
     * its own {@link java.util.Collection Collection&lt;Record&gt;} field, used
     * to verify that {@code computeNavigatePaths} recurses through
     * single-{@link Record} edges.
     */
    class Document extends Record {

        /**
         * The {@link Metadata} for this {@link Document}.
         */
        public Metadata metadata;
    }

    /**
     * The single-{@link Record} target of {@link Document#metadata}, with its
     * own {@link java.util.Collection Collection&lt;Record&gt;} field that must
     * be enumerated through the navigate-path traversal.
     */
    class Metadata extends Record {

        /**
         * The {@link TagRecord Tags} attached to this {@link Metadata}.
         */
        public List<TagRecord> tags = Lists.newArrayList();

        /**
         * A summary string.
         */
        public String summary;
    }

    /**
     * A leaf {@link Record} type used as the element type of
     * {@link Metadata#tags}.
     */
    class TagRecord extends Record {

        /**
         * A human-readable label.
         */
        public String label;
    }

    /**
     * A {@link Record} with only a cyclic single-{@link Record} field
     * ({@code parent}) and no {@link java.util.Collection
     * Collection&lt;Record&gt;} fields, used to verify that the navigate gate
     * fires for classes whose only pre-fetchable destinations come from cyclic
     * single-{@link Record} edges.
     */
    class TreeNode extends Record {

        /**
         * The parent {@link TreeNode}, or {@code null} for the root.
         */
        public TreeNode parent;

        /**
         * A human-readable name for this {@link TreeNode}.
         */
        public String name;
    }

    /**
     * One half of a mutual-reference test pair. {@link Alpha} links to
     * {@link Beta Betas} which link back to {@link Alpha Alphas}, exercising
     * the cleanup pass that closes the gap left when Concourse's same-field
     * transitive modifier cannot traverse alternating field names.
     */
    class Alpha extends Record {

        /**
         * The {@link Beta Betas} this {@link Alpha} references.
         */
        public List<Beta> betas = Lists.newArrayList();

        /**
         * A label for this {@link Alpha}.
         */
        public String label;
    }

    /**
     * One half of a mutual-reference test pair. See {@link Alpha}.
     */
    class Beta extends Record {

        /**
         * The {@link Alpha Alphas} this {@link Beta} references.
         */
        public List<Alpha> alphas = Lists.newArrayList();

        /**
         * A label for this {@link Beta}.
         */
        public String label;
    }

    /**
     * The base of a polymorphic {@link Record} hierarchy. Because
     * {@link Manager} extends {@link Employee}, a field declared as
     * {@link Employee} may link to either an {@link Employee} or a
     * {@link Manager}.
     */
    class Employee extends Record {

        /**
         * A human-readable name for this {@link Employee}.
         */
        public String name;
    }

    /**
     * A subclass of {@link Employee} whose self-referential fields are declared
     * with the polymorphic base type {@link Employee}, exercising navigate path
     * cycle detection across edges that close through a subtype.
     */
    class Manager extends Employee {

        /**
         * The {@link Employee} this {@link Manager} reports to.
         */
        public Employee supervisor;

        /**
         * The {@link Employee Employees} who report to this {@link Manager}.
         */
        public List<Employee> directReports = Lists.newArrayList();
    }

}
