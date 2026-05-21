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
package com.cinchapi.runway.access;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.Computed;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.google.common.collect.ImmutableSet;

/**
 * Regression tests for the additive {@code +} key prefix on
 * {@link Audience#frame frame}.
 * <p>
 * Verifies that the {@code +}-prefix survives the access-control intersection
 * check inside {@code frame}: the prefix is stripped before matching against
 * the audience's readable set, then re-attached when delegating to
 * {@link Record#map map}. The prefix-strip behavior must not become a bypass
 * &mdash; if the bare key is <em>not</em> in the audience's readable set, the
 * {@code +key} must still be dropped.
 *
 * @author Jeff Nelson
 */
public class AudienceFrameAdditiveKeyAccessControlTest
        extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@code frame({+label}, widget)} under
     * an {@link AccessControl#ALL_KEYS} readable returns the default payload
     * augmented with the computed {@code label}.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link OpenBox} whose
     * computed supplier has not yet run, and an anonymous {@link Audience}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is non-null, contains {@code name}
     * (from defaults) and {@code label} (from the additive), and the supplier
     * counter reflects exactly one invocation.
     */
    @Test
    public void testFrameWithAdditiveKeyUnderAllKeysReadable() {
        OpenBox widget = new OpenBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label"), widget);

        Assert.assertNotNull(result);
        Assert.assertTrue("defaults include name", result.containsKey("name"));
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertEquals(1, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code frame({+label}, widget)} passes
     * the access-control intersection when the audience's restricted readable
     * set <em>includes</em> {@code label}, and the additive's bare-root is
     * matched correctly after the {@code +} is stripped for the intersection
     * check.
     * <p>
     * <strong>Start state:</strong> A freshly constructed
     * {@link LabelReadableBox} whose readable-by-anonymous set is {@code {name,
     * label}} (not {@link AccessControl#ALL_KEYS}); the computed supplier has
     * not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is non-null and contains
     * {@code label} mapped to the computed value; the supplier counter reflects
     * exactly one invocation.
     */
    @Test
    public void testFrameWithAdditiveKeyOnReadableField() {
        LabelReadableBox widget = new LabelReadableBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label"), widget);

        Assert.assertNotNull(result);
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertEquals(1, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that when the same key root is supplied
     * under both {@code +} and {@code -}, exclusion wins &mdash; regardless of
     * the order in which the input {@link Collection} happens to iterate. The
     * fix relies on categorizing each input key directly into a prefix bucket
     * during the iteration (rather than via a single per-root map, which would
     * collapse to last-write-wins).
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link OpenBox} whose
     * computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label",
     * "-label"), widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result does not contain {@code label} (the
     * {@code -} won) and the supplier counter remains at {@code 0} (no resolver
     * ever fired).
     */
    @Test
    public void testFrameWithSameKeyAdditiveAndNegativeExclusionWins() {
        OpenBox widget = new OpenBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label", "-label"), widget);

        Assert.assertNotNull(result);
        Assert.assertFalse("- wins over +", result.containsKey("label"));
        Assert.assertEquals(0, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that when the same key root is supplied
     * under both bare and {@code +}, the bare presence subsumes the {@code +}
     * so the call is treated as whitelist (not additive). The result contains
     * only the named key, not the audience's defaults.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link OpenBox} with
     * {@code name} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label",
     * "label"), widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code label} but
     * <strong>not</strong> {@code name} &mdash; the bare positive forced
     * whitelist mode, and {@code name} (a non-listed default) is therefore
     * omitted.
     */
    @Test
    public void testFrameWithSameKeyBareAndAdditiveBareSubsumes() {
        OpenBox widget = new OpenBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label", "label"), widget);

        Assert.assertNotNull(result);
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertFalse(
                "bare key forces whitelist; non-listed defaults must drop",
                result.containsKey("name"));
    }

    /**
     * <strong>Goal:</strong> Verify the same exclusion-wins precedence holds
     * under a <em>restricted</em> readable set (exercising the branch that
     * intersects the audience's allowlist with the user's request). The user's
     * {@code -} on a key the audience would otherwise expose must still drop
     * the key from the result.
     * <p>
     * <strong>Start state:</strong> A freshly constructed
     * {@link LabelReadableBox} whose {@code $readableByAnonymous} set is
     * {@code {name, label}}; the computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label",
     * "-label"), widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code name} (an audience
     * default the user did not exclude) but not {@code label} (excluded by
     * {@code -}); the supplier counter remains at {@code 0}.
     */
    @Test
    public void testFrameWithSameKeyAdditiveAndNegativeUnderRestrictedReadable() {
        LabelReadableBox widget = new LabelReadableBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label", "-label"), widget);

        Assert.assertNotNull(result);
        Assert.assertTrue("audience default name retained",
                result.containsKey("name"));
        Assert.assertFalse("- wins over + even under restricted readable",
                result.containsKey("label"));
        Assert.assertEquals(0, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a bare positive together with a
     * negative on the same root, under {@link AccessControl#ALL_KEYS} readable,
     * returns an empty data set &mdash; not the subject's other defaults. The
     * bare positive forces whitelist mode and the negative removes the only
     * whitelisted key, so no field of the subject should appear in the result.
     * <p>
     * Without the fix, the audience would forward only the negative to
     * {@code subject.map} (dropping the bare half of the request) and return
     * {@code defaults - {name}}, leaking the subject's other intrinsic defaults
     * past the user's whitelist intent.
     * <p>
     * <strong>Start state:</strong> A freshly constructed
     * {@link MultiFieldOpenBox} with both {@code name} and {@code description}
     * populated; readable-by-anonymous is {@link AccessControl#ALL_KEYS}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of(
     * "name", "-name"), widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains neither {@code name}
     * (excluded by {@code -name}) nor {@code description} (a default that the
     * bare {@code name} dropped from the whitelist).
     */
    @Test
    public void testFrameBareAndNegativeOnSameRootUnderAllKeysReadable() {
        MultiFieldOpenBox widget = new MultiFieldOpenBox();
        widget.name = "alpha";
        widget.description = "desc";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("name", "-name"), widget);

        Assert.assertNotNull(result);
        Assert.assertFalse("- wins over bare in whitelist",
                result.containsKey("name"));
        Assert.assertFalse(
                "bare key forced whitelist; non-listed default must drop",
                result.containsKey("description"));
    }

    /**
     * <strong>Goal:</strong> Verify the same bare-plus-same-key-negative
     * semantics hold under a <em>restricted</em> readable set (exercising the
     * inner whitelist sub-branch that intersects the audience's allowlist with
     * the user's positive request). The caller's {@code -name} must reach
     * {@code subject.map} alongside the bare {@code name} so the exclude filter
     * applies inside whitelist mode &mdash; otherwise the negative is silently
     * dropped and the call diverges from {@link Record#map}.
     * <p>
     * <strong>Start state:</strong> A freshly constructed
     * {@link LabelReadableBox} whose readable-by-anonymous set is {@code {name,
     * label}}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of(
     * "name", "-name"), widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result does not contain {@code name}; the
     * negative is honored inside whitelist mode just as it is on the
     * {@link Record#map} side.
     */
    @Test
    public void testFrameBareAndNegativeOnSameRootUnderRestrictedReadable() {
        LabelReadableBox widget = new LabelReadableBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("name", "-name"), widget);

        Assert.assertNotNull(result);
        Assert.assertFalse("- wins over bare in whitelist",
                result.containsKey("name"));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code +key} cannot bypass access
     * control. When the audience's readable set <em>excludes</em> the
     * additive's bare root, the {@code +key} must be dropped during the
     * intersection check and the supplier must not fire.
     * <p>
     * <strong>Start state:</strong> A freshly constructed
     * {@link LabelHiddenBox} whose readable-by-anonymous set is {@code {name}}
     * (intentionally omitting {@code label}); the computed supplier has not yet
     * run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result does not contain {@code label} and
     * the supplier counter remains at {@code 0}.
     */
    @Test
    public void testFrameDropsAdditiveKeyOnUnreadableField() {
        LabelHiddenBox widget = new LabelHiddenBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label"), widget);

        Assert.assertNotNull(result);
        Assert.assertFalse("label must not bypass access control",
                result.containsKey("label"));
        Assert.assertEquals(0, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code +}-prefixed key never
     * re-introduces fields outside the audience's readable set. When the
     * audience can see <em>only</em> the additive's target key and the subject
     * has other intrinsic fields, the result must contain just the additive's
     * target (plus {@code id}) &mdash; not the subject's full defaults.
     * <p>
     * Without the fix, {@code frame} re-attaches {@code +} to the key before
     * delegating to {@code subject.map}; that switches {@code subject.map} into
     * additive mode and pulls in the subject's intrinsic defaults, bypassing
     * the audience allowlist.
     * <p>
     * <strong>Start state:</strong> A freshly constructed
     * {@link LabelOnlyReadableBox} whose intrinsic {@code name} field is
     * populated but whose readable-by-anonymous set is {@code {label}} only.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code label} mapped to
     * the computed value and does <strong>not</strong> contain {@code name}
     * (the intrinsic field excluded from the audience's readable set).
     */
    @Test
    public void testFrameAdditiveDoesNotBypassRestrictedReadableDefaults() {
        LabelOnlyReadableBox widget = new LabelOnlyReadableBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label"), widget);

        Assert.assertNotNull(result);
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertFalse("intrinsic name must not leak through `+` bypass",
                result.containsKey("name"));
    }

    /**
     * Base fixture providing permissive defaults for every
     * {@link AccessControl} hook except readability, which subclasses override
     * to control what an anonymous audience can see.
     */
    abstract static class WidgetBase extends Record implements AccessControl {

        public String name;

        /**
         * Tracks how many times {@link #label()} has been invoked.
         */
        public final AtomicInteger labelInvocations = new AtomicInteger(0);

        @Computed
        public String label() {
            labelInvocations.incrementAndGet();
            return "label-" + name;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return true;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return true;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return ALL_KEYS;
        }

        @Override
        public void deleteAs(Audience audience) {
            audience.delete(this);
        }
    }

    /**
     * Fixture with {@link AccessControl#ALL_KEYS} readable for any audience,
     * including anonymous. Used to exercise the un-gated additive path through
     * {@code frame}.
     */
    static class OpenBox extends WidgetBase {

        @Override
        public Set<String> $readableByAnonymous() {
            return ALL_KEYS;
        }
    }

    /**
     * Fixture with {@link AccessControl#ALL_KEYS} readable and a second
     * intrinsic field beyond {@code name}. Used to detect leaks of the
     * subject's other defaults through prefix-handling bugs in the
     * {@link Audience#frame frame} delegation to {@link Record#map map}.
     */
    static class MultiFieldOpenBox extends WidgetBase {

        public String description;

        @Override
        public Set<String> $readableByAnonymous() {
            return ALL_KEYS;
        }
    }

    /**
     * Fixture with a restricted readable set that includes both {@code name}
     * and {@code label}. Used to verify that a {@code +label} additive survives
     * the intersection check when the underlying key is in the readable
     * allowlist.
     */
    static class LabelReadableBox extends WidgetBase {

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("name", "label");
        }
    }

    /**
     * Fixture with a restricted readable set that explicitly omits
     * {@code label}. Used to verify that a {@code +label} additive cannot
     * bypass access control: the prefix-strip behavior must still respect the
     * allowlist.
     */
    static class LabelHiddenBox extends WidgetBase {

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("name");
        }
    }

    /**
     * Fixture with a restricted readable set that exposes <em>only</em> the
     * computed {@code label} property, even though the subject also carries an
     * intrinsic {@code name} field. Used to verify that a {@code +label}
     * additive never silently re-engages additive mode on the subject and leaks
     * {@code name} into the framed result.
     */
    static class LabelOnlyReadableBox extends WidgetBase {

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("label");
        }
    }
}
