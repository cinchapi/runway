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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Regression tests for the additive {@code +} key prefix on
 * {@link Record#map(String...) map} and {@link Record#json(String...) json}.
 * <p>
 * The behavior under test is the three-mode dispatch:
 * <ul>
 * <li><strong>Defaults mode</strong> when the call has no positive keys (only
 * {@code -}-prefixed exclusions, or no keys at all).</li>
 * <li><strong>Additive mode</strong> when every positive key is
 * {@code +}-prefixed; the result is defaults augmented with the additive keys,
 * minus any exclusions.</li>
 * <li><strong>Whitelist mode</strong> the moment any bare positive key is
 * present; defaults are dropped and only the listed keys (bare and
 * {@code +}-prefixed alike) appear in the result, minus exclusions.</li>
 * </ul>
 *
 * @author Jeff Nelson
 */
public class RecordAdditiveKeysTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@code map("+label")} returns the
     * default payload augmented with the {@link Computed} {@code label}
     * property, firing the supplier exactly once.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link Gadget} whose
     * computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Populate {@code name} and {@code tags} on the {@link Gadget}.</li>
     * <li>Invoke {@code gadget.map("+label")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code name},
     * {@code tags}, and the computed {@code label}; the supplier counter
     * reflects exactly one invocation.
     */
    @Test
    public void testAdditivePrefixIncludesComputedOnTopOfDefaults() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> result = gadget.map("+label");

        Assert.assertTrue("defaults include name", result.containsKey("name"));
        Assert.assertTrue("defaults include tags", result.containsKey("tags"));
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertEquals(1, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that mixing {@code +} and {@code -} keys
     * produces {@code (defaults − exclude) ∪ additive} and fires the computed
     * supplier exactly once.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name} and
     * {@code tags} populated; supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("+label", "-name")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result omits {@code name}, retains
     * {@code tags}, and includes the computed {@code label}; the supplier
     * counter reflects exactly one invocation.
     */
    @Test
    public void testAdditiveAndNegativeMix() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> result = gadget.map("+label", "-name");

        Assert.assertFalse("name excluded", result.containsKey("name"));
        Assert.assertTrue("tags retained", result.containsKey("tags"));
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertEquals(1, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code +} on a key already in the
     * defaults is a no-op &mdash; the result matches the bare-defaults payload
     * and the computed supplier never fires.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name} and
     * {@code tags} populated; supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map()} to capture the baseline payload.</li>
     * <li>Invoke {@code gadget.map("+name")}, where {@code name} is an
     * intrinsic field already present in the defaults.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code "+name"} result has the same key
     * set as the baseline {@code map()} call and the supplier counter remains
     * at {@code 0}.
     */
    @Test
    public void testAdditiveOnIntrinsicIsNoOp() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> baseline = gadget.map();
        Map<String, Object> result = gadget.map("+name");

        Assert.assertEquals(baseline.keySet(), result.keySet());
        Assert.assertEquals(0, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code +} on a Collection-typed
     * intrinsic field does not produce a duplicated/concatenated value. This
     * locks in the no-double-merge guarantee that motivates the narrower skip
     * predicate in the implementation (skip baseline only for bare-root
     * additives).
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} whose {@code tags} set
     * contains exactly two distinct elements.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("+tags")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code tags} entry in the result is a
     * Collection of size {@code 2} &mdash; the same content as
     * {@code gadget.tags}, not a concatenation that would surface size
     * {@code 4} if both baseline and additive contributions were upserted.
     */
    @Test
    public void testAdditiveOnIntrinsicCollectionDoesNotDuplicate() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> result = gadget.map("+tags");

        Object tags = result.get("tags");
        Assert.assertTrue("tags should be a Collection",
                tags instanceof java.util.Collection);
        Assert.assertEquals(2, ((java.util.Collection<?>) tags).size());
    }

    /**
     * <strong>Goal:</strong> Verify that when a key appears with both {@code +}
     * and {@code -}, exclusion wins and the supplier does not fire.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name} and
     * {@code tags} populated; supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("+label", "-label")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result omits {@code label} and the
     * supplier counter remains at {@code 0}.
     */
    @Test
    public void testNegativeWinsOverAdditiveForSameKey() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> result = gadget.map("+label", "-label");

        Assert.assertFalse("label excluded by -", result.containsKey("label"));
        Assert.assertEquals(0, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a bare positive key still triggers
     * whitelist mode when the same key is also excluded with {@code -}. The
     * bare presence locks the call into whitelist mode; the {@code -}-prefixed
     * twin filters the key out of the resulting whitelist, producing an empty
     * result rather than the defaults.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name} and
     * {@code tags} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("name", "-name")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is empty &mdash; bare {@code name}
     * forced whitelist mode and {@code -name} filtered it out of the whitelist.
     * Critically, the result must <strong>not</strong> contain {@code tags}
     * (which a defaults-mode demotion would have included).
     */
    @Test
    public void testBareKeyForcesWhitelistEvenWhenSameKeyIsExcluded() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> result = gadget.map("name", "-name");

        Assert.assertFalse("name excluded by -", result.containsKey("name"));
        Assert.assertFalse(
                "defaults must not appear; bare key forced whitelist mode",
                result.containsKey("tags"));
        Assert.assertTrue("result should be empty", result.isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that the whitelist branch does not fire
     * {@code resolveEntry} for a bare key the caller has also excluded. For a
     * {@link Computed} property named bare and then negated, this guarantees
     * the supplier never runs &mdash; the downstream {@code filter} would
     * discard the resolved entry anyway, so resolving it is wasted work.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link Gadget} with
     * {@code name} populated; the computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("label", "-label")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result does not contain {@code label} and
     * the supplier counter remains at {@code 0}.
     */
    @Test
    public void testWhitelistBranchSkipsResolverForExcludedBareKey() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";

        Map<String, Object> result = gadget.map("label", "-label");

        Assert.assertFalse("label excluded", result.containsKey("label"));
        Assert.assertEquals(
                "supplier must not fire for a bare key the caller also"
                        + " excluded",
                0, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a single bare positive key triggers
     * whitelist mode &mdash; defaults are dropped &mdash; even when
     * {@code +}-prefixed keys are also present. {@code +} in whitelist mode
     * degrades to a redundant annotation: the key is on the whitelist, nothing
     * more.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name},
     * {@code tags} populated; supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("+label", "name")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains exactly {@code name} and
     * {@code label}; {@code tags} (which is in defaults but not on the
     * whitelist) is <strong>not</strong> in the result. The supplier fires
     * exactly once because {@code label} is on the whitelist.
     */
    @Test
    public void testBareKeyWithPlusKeyIsWhitelistOnly() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> result = gadget.map("+label", "name");

        Assert.assertTrue("name on whitelist", result.containsKey("name"));
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertFalse("tags not on whitelist, defaults dropped",
                result.containsKey("tags"));
        Assert.assertEquals(1, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that naming the same key both bare and
     * {@code +}-prefixed resolves it exactly once. In whitelist mode the
     * {@code +} prefix is a redundant annotation &mdash; the resolver must
     * collapse the overlap so a {@link Computed @Computed} supplier never fires
     * twice (and, by extension, a navigation additive never loads its linked
     * record twice).
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name}
     * populated; the computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("+label", "label")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code label} mapped to
     * the computed value and the supplier counter is exactly {@code 1}.
     */
    @Test
    public void testBareAndAdditiveSameKeyResolvesOnce() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";

        Map<String, Object> result = gadget.map("+label", "label");

        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertEquals(
                "redundant + alongside bare must not double-fire the supplier",
                1, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that the existing legacy oddity &mdash; a
     * mix of bare and {@code -}-prefixed keys returns only the bare keys
     * &mdash; is preserved by the three-mode dispatch. The presence of a bare
     * positive (here, {@code "name"}) triggers whitelist mode; the
     * {@code -}-prefixed keys become no-ops because they were never on the
     * whitelist to begin with.
     * <p>
     * Mirrors {@code RecordDataAccessTest#testGetNegativeAndPositiveFiltering}
     * but exercises the new fixture to prove the new dispatch preserves the
     * established behavior.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name},
     * {@code tags} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("-tags", "name", "-label")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains only {@code name};
     * {@code tags} and {@code label} are absent.
     */
    @Test
    public void testLegacyOddityPreservedInWhitelistMode() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> result = gadget.map("-tags", "name", "-label");

        Assert.assertTrue("name on whitelist", result.containsKey("name"));
        Assert.assertFalse("tags not on whitelist", result.containsKey("tags"));
        Assert.assertFalse("label not on whitelist",
                result.containsKey("label"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code +}-prefixed navigation key
     * returns the outer record's defaults <em>and</em> a {@code linkedGadget}
     * entry whose Map slice carries the navigation target. Both the outer
     * defaults and the resolved slice must survive together: a regression that
     * drops either side &mdash; outer defaults missing, the slice missing, or
     * the slice resolved to the wrong value &mdash; should fail this test.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with a non-null
     * {@code linkedGadget} that has both {@code name} and {@code tags}
     * populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map("+linkedGadget.name")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains the outer record's
     * {@code name} and {@code tags}, and a {@code linkedGadget} entry that is a
     * {@link Map} containing {@code name} mapped to {@code "linked-alpha"}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAdditiveNavigationKeyMergesIntoBaseline() {
        Gadget linked = new Gadget();
        linked.name = "linked-alpha";
        linked.tags = new HashSet<>(ImmutableSet.of("z"));

        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));
        gadget.linkedGadget = linked;

        Map<String, Object> result = gadget.map("+linkedGadget.name");

        Assert.assertTrue("outer name preserved", result.containsKey("name"));
        Assert.assertTrue("outer tags preserved", result.containsKey("tags"));
        Object slice = result.get("linkedGadget");
        Assert.assertTrue(
                "linkedGadget slice should be a Map carrying the navigation"
                        + " target, was: " + slice,
                slice instanceof Map);
        Assert.assertEquals("linked-alpha",
                ((Map<String, Object>) slice).get("name"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code +}-prefixed navigation
     * additive whose root is also negated is dropped <em>before</em> any
     * resolver runs &mdash; the linked record is never traversed, its computed
     * supplier never fires, and the excluded root is absent from the result.
     * <p>
     * Without the fix, {@code additive.removeAll(exclude)} only catches exact
     * matches, so {@code "+linkedGadget.label"} survives past parsing alongside
     * {@code "-linkedGadget"} and triggers {@code get("linkedGadget")} plus
     * {@code linked.map("label")} before the downstream filter removes the
     * resolved entry &mdash; a wasted load on a root the caller already
     * excluded.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} whose {@code linkedGadget}
     * is populated; the linked {@code labelInvocations} counter is {@code 0}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke
     * {@code gadget.map("+linkedGadget.label", "-linkedGadget")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result does not contain
     * {@code linkedGadget} and the linked {@code labelInvocations} counter
     * remains at {@code 0}.
     */
    @Test
    public void testNavigationAdditiveDroppedWhenRootIsExcluded() {
        Gadget linked = new Gadget();
        linked.name = "linked-alpha";

        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));
        gadget.linkedGadget = linked;

        Map<String, Object> result = gadget.map("+linkedGadget.label",
                "-linkedGadget");

        Assert.assertFalse("excluded root must not appear",
                result.containsKey("linkedGadget"));
        Assert.assertEquals(
                "linked record's @Computed supplier must not fire for a"
                        + " navigation additive whose root is excluded",
                0, linked.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code json("+label")} propagates the
     * additive semantics into JSON serialization, since {@code json} delegates
     * to {@code map}.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name} and
     * {@code tags} populated; supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.json("+label")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The JSON string contains both the default
     * {@code "name"} key and the computed {@code "label"} key; the supplier
     * counter reflects exactly one invocation.
     */
    @Test
    public void testAdditiveWithJson() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        String json = gadget.json("+label");

        Assert.assertTrue("json includes name", json.contains("\"name\""));
        Assert.assertTrue("json includes label", json.contains("\"label\""));
        Assert.assertEquals(1, gadget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Sanity check that the defaults-mode branch is
     * unchanged: {@code map()} returns the default payload, and
     * {@code map("-name")} returns the default payload minus {@code name}.
     * <p>
     * <strong>Start state:</strong> A {@link Gadget} with {@code name} and
     * {@code tags} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code gadget.map()} and {@code gadget.map("-name")}
     * separately.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code map()} contains {@code name} and
     * {@code tags}; {@code map("-name")} contains {@code tags} but not
     * {@code name}. Neither includes the computed {@code label}.
     */
    @Test
    public void testNoPositiveKeysIsDefaultsBranch() {
        Gadget gadget = new Gadget();
        gadget.name = "alpha";
        gadget.tags = new HashSet<>(ImmutableSet.of("x", "y"));

        Map<String, Object> bare = gadget.map();
        Map<String, Object> negativeOnly = gadget.map("-name");

        Assert.assertTrue(bare.containsKey("name"));
        Assert.assertTrue(bare.containsKey("tags"));
        Assert.assertFalse(bare.containsKey("label"));

        Assert.assertFalse(negativeOnly.containsKey("name"));
        Assert.assertTrue(negativeOnly.containsKey("tags"));
        Assert.assertFalse(negativeOnly.containsKey("label"));
    }

    /**
     * A {@link Record} fixture with an intrinsic scalar, an intrinsic
     * collection, a self-referential link, and a counter-instrumented
     * {@link Computed} property. Exercises the three-mode dispatch (defaults,
     * additive, whitelist) and the navigation-merge invariants under
     * {@code map} and {@code json}.
     */
    class Gadget extends Record {

        public String name;

        public Set<String> tags = new HashSet<>();

        public Gadget linkedGadget;

        /**
         * Tracks how many times {@link #label()} has been invoked.
         */
        final AtomicInteger labelInvocations = new AtomicInteger(0);

        @Computed
        public String label() {
            labelInvocations.incrementAndGet();
            return "label-" + name;
        }
    }
}
