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
 * Regression tests for {@link Audience#frame frame} against a denylist-only
 * {@code $readableBy*()} rule (a non-{@link AccessControl#ALL_KEYS} set that
 * contains only {@code -}-prefixed entries). The audience's semantic in this
 * mode is "every key the subject exposes, minus the entries in the denylist",
 * mirroring {@link AccessControlSupport#isPermittedAccess} which treats an
 * empty allowlist as a wildcard.
 *
 * @author Jeff Nelson
 */
public class AudienceFrameDenylistReadableTest
        extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@code frame({name}, widget)} against
     * a denylist-only readable returns the named key &mdash; the audience's
     * empty allowlist must behave as a wildcard, not a deny-all.
     * <p>
     * <strong>Start state:</strong> A freshly constructed
     * {@link SecretDenialBox} whose {@code $readableByAnonymous} set is
     * {@code {"-secret"}} (denylist-only) with {@code name} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("name"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code name} &mdash;
     * dispatching whitelist mode on the audience side must not strip the key
     * just because the audience has no positive allowlist.
     */
    @Test
    public void testFrameBareKeyAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("name"), widget);

        Assert.assertNotNull(result);
        Assert.assertEquals("alpha", result.get("name"));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code frame({+label}, widget)}
     * against a denylist-only readable returns the audience's defaults
     * augmented with the additive, minus the audience's denials.
     * <p>
     * <strong>Start state:</strong> A {@link SecretDenialBox} whose
     * {@code $readableByAnonymous} set is {@code {"-secret"}} with {@code name}
     * populated; the computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+label"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code name} (default
     * minus the denied {@code secret}) and {@code label} (additive), and does
     * not contain {@code secret}. The supplier fires exactly once.
     */
    @Test
    public void testFrameAdditiveAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";
        widget.secret = "hidden";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+label"), widget);

        Assert.assertNotNull(result);
        Assert.assertEquals("alpha", result.get("name"));
        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertFalse("denied secret must not leak",
                result.containsKey("secret"));
        Assert.assertEquals(1, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code frame({-name}, widget)} against
     * a denylist-only readable returns the audience's defaults minus both the
     * user's exclusion and the audience's denials.
     * <p>
     * <strong>Start state:</strong> A {@link SecretDenialBox} whose
     * {@code $readableByAnonymous} set is {@code {"-secret"}} with {@code name}
     * and {@code secret} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("-name"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result does not contain {@code name}
     * (excluded by the user) or {@code secret} (denied by the audience).
     */
    @Test
    public void testFrameNegativeAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";
        widget.secret = "hidden";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("-name"), widget);

        Assert.assertNotNull(result);
        Assert.assertFalse("user excluded name", result.containsKey("name"));
        Assert.assertFalse("audience denied secret",
                result.containsKey("secret"));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code frame({secret}, widget)}
     * against a denylist-only readable that denies {@code secret} drops the key
     * from the result and flags the call as restricted.
     * <p>
     * <strong>Start state:</strong> A {@link SecretDenialBox} whose
     * {@code $readableByAnonymous} set is {@code {"-secret"}} with
     * {@code secret} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().read(ImmutableSet.of("secret"),
     * widget)} and capture the thrown exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link RestrictedAccessException} &mdash; the user explicitly named a
     * denied key, so the audience must not silently honor it.
     */
    @Test(expected = RestrictedAccessException.class)
    public void testReadDeniedBareKeyAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";
        widget.secret = "hidden";

        Audience.anonymous().read(ImmutableSet.of("secret"), widget);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code +}-prefixing a denied key
     * cannot bypass the denylist. The additive is dropped at the access-control
     * intersection, the call is flagged as restricted, and the resulting map
     * carries the audience's defaults minus the denied key.
     * <p>
     * <strong>Start state:</strong> A {@link SecretDenialBox} whose
     * {@code $readableByAnonymous} set is {@code {"-secret"}} with both
     * {@code name} and {@code secret} populated; the computed supplier has not
     * yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+secret"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code name} (an audience
     * default the caller did not exclude) but does <strong>not</strong> contain
     * {@code secret} (denied by the audience even with the {@code +} prefix).
     * The computed supplier remains untouched.
     */
    @Test
    public void testFrameDropsAdditiveOnDeniedKeyAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";
        widget.secret = "hidden";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+secret"), widget);

        Assert.assertNotNull(result);
        Assert.assertFalse("+ prefix must not bypass denylist",
                result.containsKey("secret"));
        Assert.assertEquals("audience default name retained", "alpha",
                result.get("name"));
        Assert.assertEquals(
                "supplier must not fire for an unrequested computed key", 0,
                widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code read({"+secret"}, widget)}
     * against a denylist-only readable throws
     * {@link RestrictedAccessException}. A {@code +} prefix names a positive
     * request; the audience must surface a restriction signal when that request
     * collides with a denial, just as it does for the bare-key equivalent.
     * <p>
     * <strong>Start state:</strong> A {@link SecretDenialBox} whose
     * {@code $readableByAnonymous} set is {@code {"-secret"}} with
     * {@code secret} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().read(ImmutableSet.of("+secret"),
     * widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link RestrictedAccessException}.
     */
    @Test(expected = RestrictedAccessException.class)
    public void testReadAdditiveOnDeniedKeyAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";
        widget.secret = "hidden";

        Audience.anonymous().read(ImmutableSet.of("+secret"), widget);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code frame(widget)} (an
     * {@link AccessControl#ALL_KEYS}-requested call) against a denylist-only
     * readable returns the subject's defaults minus the audience's denied keys.
     * This exercises the {@code requested == ALL_KEYS && readable != ALL_KEYS}
     * forwarding path in {@link Audience#frame}.
     * <p>
     * <strong>Start state:</strong> A {@link SecretDenialBox} whose
     * {@code $readableByAnonymous} set is {@code {"-secret"}} with {@code name}
     * and {@code secret} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code name} (default) and
     * does not contain {@code secret} (denied by the audience).
     */
    @Test
    public void testFrameAllKeysRequestAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";
        widget.secret = "hidden";

        Map<String, Object> result = Audience.anonymous().frame(widget);

        Assert.assertNotNull(result);
        Assert.assertEquals("alpha", result.get("name"));
        Assert.assertFalse("denied secret must not leak",
                result.containsKey("secret"));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code frame({"name", "-name"},
     * widget)} against a denylist-only readable hits the whitelist branch (bare
     * positive triggers) and the caller's {@code -name} removes the only
     * whitelisted key, producing an empty data set. The audience's denials must
     * still be honored, so the denied {@code secret} must not surface either.
     * <p>
     * <strong>Start state:</strong> A {@link SecretDenialBox} whose
     * {@code $readableByAnonymous} set is {@code {"-secret"}} with both
     * {@code name} and {@code secret} populated.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("name",
     * "-name"), widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains neither {@code name}
     * (excluded by the user) nor {@code secret} (denied by the audience) nor
     * any other default field &mdash; the bare positive forced whitelist mode
     * and the negative removed the only whitelisted key.
     */
    @Test
    public void testFrameBareAndNegativeOnSameRootAgainstDenylistOnlyReadable() {
        SecretDenialBox widget = new SecretDenialBox();
        widget.name = "alpha";
        widget.secret = "hidden";

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("name", "-name"), widget);

        Assert.assertNotNull(result);
        Assert.assertFalse("- removes the whitelisted name",
                result.containsKey("name"));
        Assert.assertFalse("denied secret must not leak",
                result.containsKey("secret"));
    }

    /**
     * Fixture with a denylist-only readable rule that hides the {@code secret}
     * field. Every other intrinsic and computed field remains readable,
     * mirroring {@link AccessControlSupport#isPermittedAccess}'s
     * empty-allowlist-as-wildcard semantic.
     */
    static class SecretDenialBox extends Record implements AccessControl {

        public String name;

        public String secret;

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
            return ImmutableSet.of("-secret");
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("-secret");
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
}
