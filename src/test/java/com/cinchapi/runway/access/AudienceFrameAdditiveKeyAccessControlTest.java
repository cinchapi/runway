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
}
