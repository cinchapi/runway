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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for the {@link Record#computeOnce(String, java.util.function.Supplier)}
 * memoization primitive.
 *
 * @author Jeff Nelson
 */
public class RecordComputeOnceTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Computed} method using
     * {@link Record#computeOnce(String, java.util.function.Supplier)} executes
     * the supplier at most once when both a direct call and serialization
     * occur.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link MemoizedWidget}
     * with no prior invocations.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code widget.tags()} directly (simulating a {@link Derived}
     * method calling a {@link Computed} method).</li>
     * <li>Call {@code widget.map()} which triggers serialization and fires the
     * computed supplier for {@code "tags"}.</li>
     * <li>Check the invocation counter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The supplier executes exactly once despite
     * being triggered through two independent paths.
     */
    @Test
    public void testComputeOnceDeduplicatesDirectCallAndSerialization() {
        MemoizedWidget widget = new MemoizedWidget();
        // Path 1: direct call (simulates what a @Derived method
        // would do)
        Set<String> tags = widget.tags();
        Assert.assertNotNull(tags);
        Assert.assertEquals(1, widget.tagsInvocations.get());

        // Path 3: serialization fires the computed supplier
        Map<String, Object> data = widget.map();
        Assert.assertEquals(tags, data.get("tags"));

        // The supplier should NOT have run a second time
        Assert.assertEquals(1, widget.tagsInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Computed} method that does
     * <em>not</em> use
     * {@link Record#computeOnce(String, java.util.function.Supplier)} still
     * recomputes on each {@code map()} call, preserving backward compatibility.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link PlainCounter} with
     * no prior invocations.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code map()} twice.</li>
     * <li>Check the counter value each time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each {@code map()} call recomputes the value,
     * incrementing the counter.
     */
    @Test
    public void testNonMemoizedComputedStillRecomputes() {
        PlainCounter counter = new PlainCounter();

        Map<String, Object> data = counter.map();
        Assert.assertEquals(1, data.get("count"));

        data = counter.map();
        Assert.assertEquals(2, data.get("count"));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#clearComputeOnceCache()}
     * invalidates cached results so subsequent calls recompute.
     * <p>
     * <strong>Start state:</strong> A {@link MemoizedWidget} that has already
     * been serialized once.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code widget.tags()} to populate the cache.</li>
     * <li>Call {@code widget.resetCache()} which delegates to
     * {@code clearComputeOnceCache()}.</li>
     * <li>Call {@code widget.tags()} again.</li>
     * <li>Check the invocation counter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> After clearing, the supplier executes again,
     * bringing the counter to 2.
     */
    @Test
    public void testClearComputeOnceCacheAllowsRecomputation() {
        MemoizedWidget widget = new MemoizedWidget();

        widget.tags();
        Assert.assertEquals(1, widget.tagsInvocations.get());

        widget.resetCache();

        widget.tags();
        Assert.assertEquals(2, widget.tagsInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Derived} method calling a
     * {@link Computed} method with
     * {@link Record#computeOnce(String, java.util.function.Supplier)} does not
     * cause duplicate computation during serialization.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link DerivedComposer}
     * with no prior invocations.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code map()} which triggers both {@code $derived()} (eagerly
     * evaluating {@code summary()}) and the computed supplier for
     * {@code "items"}.</li>
     * <li>Check the invocation counter for the {@code items()} method.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code items()} supplier executes exactly
     * once even though both the {@link Derived} method and serialization invoke
     * it.
     */
    @Test
    public void testDerivedCallingComputedDeduplicatesViaSerialization() {
        DerivedComposer composer = new DerivedComposer();

        Map<String, Object> data = composer.map();

        // The derived "summary" should be present
        Assert.assertNotNull(data.get("summary"));

        // The computed "items" should be present
        Assert.assertNotNull(data.get("items"));

        // items() should only have been invoked once despite
        // being called from summary() (path 1) and from the
        // computed supplier (path 3)
        Assert.assertEquals(1, composer.itemsInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#computeOnce(String, java.util.function.Supplier)} also
     * deduplicates when accessed via {@link Record#get(String)}.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link MemoizedWidget}
     * with no prior invocations.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code widget.tags()} directly.</li>
     * <li>Call {@code widget.get("tags")} (path 2).</li>
     * <li>Check the invocation counter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The supplier executes exactly once;
     * {@code get()} returns the cached result.
     */
    @Test
    public void testComputeOnceDeduplicatesDirectCallAndGet() {
        MemoizedWidget widget = new MemoizedWidget();

        Set<String> direct = widget.tags();
        Set<String> viaGet = widget.get("tags");

        Assert.assertEquals(direct, viaGet);
        Assert.assertEquals(1, widget.tagsInvocations.get());
    }

    /**
     * A {@link Record} with a {@link Computed} method that uses
     * {@link Record#computeOnce(String, java.util.function.Supplier)} for
     * memoization.
     */
    class MemoizedWidget extends Record {

        /**
         * Tracks how many times the tags supplier has been invoked.
         */
        final AtomicInteger tagsInvocations = new AtomicInteger(0);

        @Computed
        public Set<String> tags() {
            return computeOnce("tags", () -> {
                tagsInvocations.incrementAndGet();
                return ImmutableSet.of("a", "b", "c");
            });
        }

        /**
         * Expose {@link #clearComputeOnceCache()} for testing.
         */
        void resetCache() {
            clearComputeOnceCache();
        }
    }

    /**
     * A {@link Record} with a {@link Computed} method that does <em>not</em>
     * use {@link Record#computeOnce(String, java.util.function.Supplier)},
     * verifying backward compatibility.
     */
    class PlainCounter extends Record {

        private final AtomicInteger count = new AtomicInteger(0);

        @Computed
        public int count() {
            return count.incrementAndGet();
        }
    }

    /**
     * A {@link Record} where a {@link Derived} method calls a {@link Computed}
     * method that uses
     * {@link Record#computeOnce(String, java.util.function.Supplier)},
     * simulating the cross-invocation-path duplication scenario.
     */
    class DerivedComposer extends Record {

        /**
         * Tracks how many times the items supplier has been invoked.
         */
        final AtomicInteger itemsInvocations = new AtomicInteger(0);

        @Computed
        public Set<String> items() {
            return computeOnce("items", () -> {
                itemsInvocations.incrementAndGet();
                return ImmutableSet.of("x", "y", "z");
            });
        }

        @Derived
        public String summary() {
            // This calls items() directly (path 1), which
            // will also be called via the computed supplier
            // during serialization (path 3).
            return "Items: " + items().size();
        }
    }

}
