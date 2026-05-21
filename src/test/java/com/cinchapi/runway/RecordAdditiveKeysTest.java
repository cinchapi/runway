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
 * <p>
 * These tests are expected to fail prior to the implementation of GH-133 and
 * pass after.
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
     * <strong>Goal:</strong> Verify that {@code +}-prefixed navigation keys do
     * <strong>not</strong> erase the rest of the baseline root. This locks in
     * the narrower-skip-predicate fix: for a navigation additive like
     * {@code "+linkedGadget.name"}, the baseline {@code linkedGadget} entry
     * must remain in the pool so the navigation slice merges into it rather
     * than replacing it.
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
     * defaults ({@code name}, {@code tags}) and contains a non-null
     * {@code linkedGadget} entry. The result also has a way to surface the
     * linked record's {@code name} &mdash; whether via the link being expanded
     * inline or via the merged navigation slice. The test asserts the
     * conservative invariant that the outer defaults survive and the
     * {@code linkedGadget} key is present.
     */
    @Test
    public void testAdditiveNavigationKeyPreservesBaselineRoot() {
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
        Assert.assertTrue("linkedGadget key present",
                result.containsKey("linkedGadget"));
        Assert.assertNotNull(result.get("linkedGadget"));
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
