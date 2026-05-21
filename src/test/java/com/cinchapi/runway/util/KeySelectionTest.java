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
package com.cinchapi.runway.util;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.util.KeySelection.Kind;
import com.cinchapi.runway.util.KeySelection.Partition;
import com.cinchapi.runway.util.KeySelection.RootedPartition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for {@link KeySelection}, the shared parser for the
 * {@code +}/{@code -}/bare key conventions used by
 * {@link com.cinchapi.runway.Record#map Record#map} and
 * {@link com.cinchapi.runway.access.Audience#frame Audience#frame}.
 *
 * @author Jeff Nelson
 */
public class KeySelectionTest {

    /**
     * <strong>Goal:</strong> Verify that {@link KeySelection#kindOf(String)
     * kindOf} returns {@link Kind#ADDITIVE} for {@code +}-prefixed keys,
     * {@link Kind#EXCLUDE} for {@code -}-prefixed keys, and {@link Kind#BARE}
     * for everything else.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code kindOf} with a {@code +}-prefixed key, a
     * {@code -}-prefixed key, a bare key, and an empty string.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Kind} matches the input
     * prefix; an empty string is {@link Kind#BARE}.
     */
    @Test
    public void testKindOfClassifiesByPrefix() {
        Assert.assertEquals(Kind.ADDITIVE, KeySelection.kindOf("+foo"));
        Assert.assertEquals(Kind.EXCLUDE, KeySelection.kindOf("-foo"));
        Assert.assertEquals(Kind.BARE, KeySelection.kindOf("foo"));
        Assert.assertEquals(Kind.BARE, KeySelection.kindOf(""));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link KeySelection#stripPrefix(String) stripPrefix} removes a leading
     * {@code +} or {@code -} and leaves all other inputs unchanged.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code stripPrefix} with a {@code +}-prefixed key, a
     * {@code -}-prefixed key, a bare key, and an empty string.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is the input with at most one
     * leading {@code +}/{@code -} removed; bare and empty inputs are returned
     * unchanged.
     */
    @Test
    public void testStripPrefixRemovesLeadingMarker() {
        Assert.assertEquals("foo", KeySelection.stripPrefix("+foo"));
        Assert.assertEquals("foo", KeySelection.stripPrefix("-foo"));
        Assert.assertEquals("foo", KeySelection.stripPrefix("foo"));
        Assert.assertEquals("", KeySelection.stripPrefix(""));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link KeySelection#partition(Iterable)} drops their leading prefixes
     * onto each bucket and applies the exclusion-wins rule for both exact and
     * root matches against {@code exclude}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition a list containing one bare key, two additive keys whose
     * roots are excluded by sibling {@code -} entries (one exact match, one
     * root match against a navigation additive), and an unrelated additive that
     * survives.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code bare} contains the bare key unchanged;
     * {@code additive} contains only the surviving additive; {@code exclude}
     * contains the two stripped exclusions.
     */
    @Test
    public void testPartitionAppliesExclusionWinsForExactAndRootMatches() {
        Partition p = KeySelection.partition(ImmutableList.of("name", "+foo",
                "-foo", "+bar.baz", "-bar", "+kept"));

        Assert.assertEquals(ImmutableList.of("name"), p.bare());
        Assert.assertEquals(ImmutableList.of("kept"), p.additive());
        Assert.assertEquals(ImmutableSet.of("foo", "bar"),
                ImmutableSet.copyOf(p.exclude()));
    }

    /**
     * <strong>Goal:</strong> Verify that a bare key shared with an exclude
     * entry stays in {@link Partition#bare() bare}; the exclusion-wins rule
     * does not strip bare keys, because a bare positive must retain its power
     * to force whitelist mode at the call site.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition a list containing the same key both bare and as an
     * exclusion ({@code "name", "-name"}).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code bare} contains {@code name};
     * {@code exclude} also contains {@code name}; {@code additive} is empty.
     */
    @Test
    public void testPartitionBareKeyRetainsPowerWhenAlsoExcluded() {
        Partition p = KeySelection.partition(ImmutableList.of("name", "-name"));

        Assert.assertEquals(ImmutableList.of("name"), p.bare());
        Assert.assertEquals(ImmutableList.of("name"), p.exclude());
        Assert.assertTrue("additive should be empty", p.additive().isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Partition} accessors return
     * unmodifiable views &mdash; the contract is read-only.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition any list of keys.</li>
     * <li>Attempt to add to each of {@code bare()}, {@code additive()}, and
     * {@code exclude()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each attempted mutation throws an
     * {@link UnsupportedOperationException}.
     */
    @Test
    public void testPartitionAccessorsAreUnmodifiable() {
        Partition p = KeySelection.partition(ImmutableList.of("a", "+b", "-c"));

        try {
            p.bare().add("x");
            Assert.fail("bare() must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
        try {
            p.additive().add("x");
            Assert.fail("additive() must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
        try {
            p.exclude().add("x");
            Assert.fail("exclude() must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link KeySelection#partitionByRoot(Iterable)} buckets each input by its
     * bare root, surfaces navigation suffixes per root, and applies the
     * exclusion-wins rule on the bare roots.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition a list containing a bare navigation key, an additive
     * navigation key, an exclude on a different root, and an unrelated bare
     * key.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The bucket sets contain the bare roots (no
     * prefix, no suffix); the navigation map carries the suffixes for every
     * mentioned root, including an empty set for roots without a suffix.
     */
    @Test
    public void testPartitionByRootBucketsBareRootsAndTracksSuffixes() {
        RootedPartition p = KeySelection.partitionByRoot(ImmutableList
                .of("user.name", "+company.address", "-archived", "ok"));

        Assert.assertEquals(ImmutableSet.of("user", "ok"), p.bare());
        Assert.assertEquals(ImmutableSet.of("company"), p.additive());
        Assert.assertEquals(ImmutableSet.of("archived"), p.exclude());

        Map<String, Set<String>> nav = p.navigation();
        Assert.assertEquals(ImmutableSet.of("name"), nav.get("user"));
        Assert.assertEquals(ImmutableSet.of("address"), nav.get("company"));
        Assert.assertTrue("'-archived' has no suffix",
                nav.get("archived").isEmpty());
        Assert.assertTrue("'ok' has no suffix", nav.get("ok").isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link KeySelection#partitionByRoot(Iterable)} drops an additive entry
     * when its root is also excluded &mdash; including the case where the
     * exclude entry itself carries a navigation suffix ({@code "-a.c"} is
     * treated as a root-level exclude on {@code "a"} per the Audience
     * convention).
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition {@code "+a.b", "-a"} (root exact-match).</li>
     * <li>Partition {@code "+a.b", "-a.c"} (root match via the exclude's own
     * navigation key).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> In both cases, {@code additive} is empty and
     * {@code exclude} contains {@code a}.
     */
    @Test
    public void testPartitionByRootExclusionWinsOnRootMatch() {
        RootedPartition exact = KeySelection
                .partitionByRoot(ImmutableList.of("+a.b", "-a"));
        Assert.assertTrue("additive should be empty when root is excluded",
                exact.additive().isEmpty());
        Assert.assertEquals(ImmutableSet.of("a"), exact.exclude());

        RootedPartition navExclude = KeySelection
                .partitionByRoot(ImmutableList.of("+a.b", "-a.c"));
        Assert.assertTrue("'-a.c' is a root-level exclude that drops '+a.b'",
                navExclude.additive().isEmpty());
        Assert.assertEquals(ImmutableSet.of("a"), navExclude.exclude());
    }

    /**
     * <strong>Goal:</strong> Verify that the partition produced by
     * {@link KeySelection#partitionByRoot(Iterable)} is order-independent
     * &mdash; the same set of inputs in any order yields equal buckets and
     * equal navigation maps.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition {@code ["+x", "-x"]} and {@code ["-x", "+x"]}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both produce empty {@code additive},
     * {@code exclude = {x}}, and equivalent navigation maps.
     */
    @Test
    public void testPartitionByRootIsOrderIndependent() {
        RootedPartition forward = KeySelection
                .partitionByRoot(ImmutableList.of("+x", "-x"));
        RootedPartition reverse = KeySelection
                .partitionByRoot(ImmutableList.of("-x", "+x"));

        Assert.assertEquals(forward.bare(), reverse.bare());
        Assert.assertEquals(forward.additive(), reverse.additive());
        Assert.assertEquals(forward.exclude(), reverse.exclude());
        Assert.assertTrue("additive must be empty in both",
                forward.additive().isEmpty());
        Assert.assertEquals(ImmutableSet.of("x"), forward.exclude());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link RootedPartition} accessors
     * return unmodifiable views.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition any list of keys.</li>
     * <li>Attempt to mutate {@code bare()}, {@code additive()},
     * {@code exclude()}, and {@code navigation()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each attempted mutation throws an
     * {@link UnsupportedOperationException}.
     */
    @Test
    public void testRootedPartitionAccessorsAreUnmodifiable() {
        RootedPartition p = KeySelection
                .partitionByRoot(ImmutableList.of("a", "+b.c", "-d"));

        try {
            p.bare().add("x");
            Assert.fail("bare() must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
        try {
            p.additive().add("x");
            Assert.fail("additive() must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
        try {
            p.exclude().add("x");
            Assert.fail("exclude() must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
        try {
            p.navigation().put("x", ImmutableSet.of("y"));
            Assert.fail("navigation() must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
    }

    /**
     * <strong>Goal:</strong> Verify that the inner suffix {@link Set Sets}
     * stored in {@link RootedPartition#navigation() navigation} are themselves
     * unmodifiable &mdash; the outer-map wrapper must not leak live
     * {@link java.util.HashSet HashSets} that would break the
     * {@link javax.annotation.concurrent.Immutable @Immutable} contract.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Partition a list containing a navigation key.</li>
     * <li>Attempt to add a suffix to the inner {@link Set} returned by
     * {@code navigation().get(...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The mutation throws an
     * {@link UnsupportedOperationException}.
     */
    @Test
    public void testRootedPartitionNavigationInnerSetsAreUnmodifiable() {
        RootedPartition p = KeySelection
                .partitionByRoot(ImmutableList.of("a.b"));

        try {
            p.navigation().get("a").add("c");
            Assert.fail("navigation() inner Set must be unmodifiable");
        }
        catch (UnsupportedOperationException expected) {/* pass */}
    }

}
