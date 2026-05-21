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
import com.cinchapi.runway.SerializationOptions;

/**
 * Tests for the options-aware {@link Audience#frame frame} overloads, verifying
 * that {@link SerializationOptions} thread from the {@link Audience}
 * {@code frame} pipeline into the underlying {@link Record#map map} calls, and
 * that the convenience overloads delegate with
 * {@link SerializationOptions#defaults() default options}.
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("unchecked")
public class AudienceFrameOptionsTest extends AudienceAccessControlBaseTest {

    /**
     * <strong>Goal:</strong> Verify that
     * {@link SerializationOptions#includeComputedValuesByDefault()
     * includeComputedValuesByDefault} = {@code false} suppresses
     * {@link Computed} properties when framing through an {@link Audience},
     * proving that {@link SerializationOptions} thread into the underlying
     * {@link Record#map map} calls.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link Admin} and a
     * freshly constructed {@link WidgetWithComputed} whose computed supplier
     * has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(false)}.</li>
     * <li>Invoke
     * {@code admin.frame(opts, AccessControl.ALL_KEYS, widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The framed result omits
     * {@code "computedLabel"} and the supplier invocation counter remains at
     * {@code 0}.
     */
    @Test
    public void testFrameWithFlagFalseSuppressesComputed() {
        Admin admin = new Admin();
        admin.email = "admin@example.com";
        admin.name = "Admin";

        WidgetWithComputed widget = new WidgetWithComputed();
        widget.label = "alpha";

        SerializationOptions opts = SerializationOptions.builder()
                .includeComputedValuesByDefault(false).build();

        Map<String, Object> result = admin.frame(opts, AccessControl.ALL_KEYS,
                widget);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.containsKey("computedLabel"));
        Assert.assertEquals(0, widget.computedInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that the two-arg convenience overload
     * {@link Audience#frame(java.util.Collection, Record) frame(keys, subject)}
     * delegates to the canonical method with default
     * {@link SerializationOptions}, which exclude {@link Computed} properties
     * from the framed result.
     * <p>
     * <strong>Start state:</strong> Same as
     * {@link #testFrameWithFlagFalseSuppressesComputed()}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code admin.frame(AccessControl.ALL_KEYS, widget)} with no
     * {@link SerializationOptions options}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result omits {@code "computedLabel"} and
     * the supplier invocation counter remains at {@code 0}.
     */
    @Test
    public void testTwoArgFrameSuppressesComputedByDefault() {
        Admin admin = new Admin();
        admin.email = "admin@example.com";
        admin.name = "Admin";

        WidgetWithComputed widget = new WidgetWithComputed();
        widget.label = "alpha";

        Map<String, Object> result = admin.frame(AccessControl.ALL_KEYS,
                widget);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.containsKey("computedLabel"));
        Assert.assertEquals(0, widget.computedInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that the one-arg convenience overload
     * {@link Audience#frame(Record) frame(record)} delegates to the canonical
     * method with default {@link SerializationOptions} and all keys, which
     * exclude {@link Computed} properties from the framed result.
     * <p>
     * <strong>Start state:</strong> Same as
     * {@link #testFrameWithFlagFalseSuppressesComputed()}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code admin.frame(widget)} with no keys and no
     * {@link SerializationOptions options}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result omits {@code "computedLabel"} and
     * the supplier invocation counter remains at {@code 0}.
     */
    @Test
    public void testOneArgFrameSuppressesComputedByDefault() {
        Admin admin = new Admin();
        admin.email = "admin@example.com";
        admin.name = "Admin";

        WidgetWithComputed widget = new WidgetWithComputed();
        widget.label = "alpha";

        Map<String, Object> result = admin.frame(widget);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.containsKey("computedLabel"));
        Assert.assertEquals(0, widget.computedInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code includeComputedValuesByDefault = true} on the
     * {@link SerializationOptions} passed to {@code frame} restores the legacy
     * behavior of materializing {@link Computed} properties even when the
     * caller does not positively name them.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link Admin} and a
     * freshly constructed {@link WidgetWithComputed} whose computed supplier
     * has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(true)}.</li>
     * <li>Invoke
     * {@code admin.frame(opts, AccessControl.ALL_KEYS, widget)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains {@code "computedLabel"}
     * and the supplier invocation counter reflects exactly one invocation.
     */
    @Test
    public void testFrameWithFlagTrueIncludesComputed() {
        Admin admin = new Admin();
        admin.email = "admin@example.com";
        admin.name = "Admin";

        WidgetWithComputed widget = new WidgetWithComputed();
        widget.label = "alpha";

        SerializationOptions opts = SerializationOptions.builder()
                .includeComputedValuesByDefault(true).build();

        Map<String, Object> result = admin.frame(opts, AccessControl.ALL_KEYS,
                widget);

        Assert.assertNotNull(result);
        Assert.assertEquals("computed-alpha", result.get("computedLabel"));
        Assert.assertEquals(1, widget.computedInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code includeComputedValuesByDefault = false} propagates from the outer
     * {@code frame} call into the recursive frame of a linked
     * {@link AccessControl} {@link Record}, proving that
     * {@link SerializationOptions} thread through every level of the framing
     * pipeline &mdash; not just the top-level {@link Record#map map} call.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link Admin} and two
     * freshly constructed {@link WidgetWithComputed} records, one linked as the
     * {@code child} of the other; neither computed supplier has yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(false)}.</li>
     * <li>Invoke {@code admin.frame(opts, AccessControl.ALL_KEYS, parent)},
     * which causes the framing pipeline to recurse into the linked
     * {@code child} via
     * {@link Audience#frame(SerializationOptions, java.util.Collection, Record)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The outer framed result omits
     * {@code "computedLabel"} and the nested framed result for the linked
     * {@code child} also omits {@code "computedLabel"}; both supplier
     * invocation counters remain at {@code 0}.
     */
    @Test
    public void testFrameWithFlagFalseSuppressesComputedOnLinkedRecord() {
        Admin admin = new Admin();
        admin.email = "admin@example.com";
        admin.name = "Admin";

        WidgetWithComputed child = new WidgetWithComputed();
        child.label = "child";

        WidgetWithComputed parent = new WidgetWithComputed();
        parent.label = "parent";
        parent.child = child;

        SerializationOptions opts = SerializationOptions.builder()
                .includeComputedValuesByDefault(false).build();

        Map<String, Object> result = admin.frame(opts, AccessControl.ALL_KEYS,
                parent);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.containsKey("computedLabel"));
        Assert.assertEquals(0, parent.computedInvocations.get());

        Map<String, Object> nested = (Map<String, Object>) result.get("child");
        Assert.assertNotNull(nested);
        Assert.assertFalse(nested.containsKey("computedLabel"));
        Assert.assertEquals(0, child.computedInvocations.get());
    }

    /**
     * An {@link AccessControl} {@link Record} with one regular field, one
     * self-referential {@code child} link, and one {@link Computed} property
     * whose supplier increments a counter. The access control rules are
     * intentionally permissive so the framing pipeline always reaches the
     * underlying {@link Record#map map} call, letting tests focus on
     * {@link SerializationOptions} threading.
     */
    protected static class WidgetWithComputed extends Record implements
            AccessControl {

        public String label;

        public WidgetWithComputed child;

        /**
         * Tracks how many times {@link #computedLabel()} has been invoked.
         */
        public final AtomicInteger computedInvocations = new AtomicInteger(0);

        @Computed
        public String computedLabel() {
            computedInvocations.incrementAndGet();
            return "computed-" + label;
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
        public Set<String> $readableByAnonymous() {
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
}
