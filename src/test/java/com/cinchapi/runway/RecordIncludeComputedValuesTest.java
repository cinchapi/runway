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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests covering how
 * {@link SerializationOptions#includeComputedValuesByDefault()} controls
 * whether a {@link Computed} property's supplier fires during {@code map()} and
 * {@code json()}. When the flag is {@code false}, the supplier is suppressed
 * for no-keys and negative-only key sets; when the flag is {@code false} but
 * the caller positively names the computed key, the supplier still fires and
 * the value appears in the result.
 *
 * @author Jeff Nelson
 */
public class RecordIncludeComputedValuesTest
        extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that the {@link Computed} supplier never
     * fires when {@code map()} is called with
     * {@code includeComputedValuesByDefault = false} and no keys.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link LabeledWidget}
     * whose computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(false)}.</li>
     * <li>Invoke {@code widget.map(opts)} with no keys, which exercises the
     * {@code data().entrySet()} iteration path.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The supplier invocation counter remains at
     * {@code 0}, proving the computed entry was filtered out structurally
     * before {@code getValue()} could fire the supplier.
     */
    @Test
    public void testComputedSupplierDoesNotFireWithNoKeysWhenFlagIsFalse() {
        LabeledWidget widget = new LabeledWidget();
        widget.name = "alpha";

        SerializationOptions opts = SerializationOptions.builder()
                .includeComputedValuesByDefault(false).build();

        widget.map(opts);

        Assert.assertEquals(0, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link Computed} supplier never
     * fires when {@code map()} is called with
     * {@code includeComputedValuesByDefault = false} and only negative
     * (exclusion) keys &mdash; the canonical scenario described in the
     * originating issue.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link LabeledWidget}
     * whose computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(false)}.</li>
     * <li>Invoke {@code widget.map(opts, "-name")}, supplying a negative-only
     * key set that still routes through the {@code data().entrySet()} iteration
     * path.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The supplier invocation counter remains at
     * {@code 0}.
     */
    @Test
    public void testComputedSupplierDoesNotFireWithNegativeKeysWhenFlagIsFalse() {
        LabeledWidget widget = new LabeledWidget();
        widget.name = "alpha";

        SerializationOptions opts = SerializationOptions.builder()
                .includeComputedValuesByDefault(false).build();

        widget.map(opts, "-name");

        Assert.assertEquals(0, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link Computed} supplier never
     * fires when {@code json()} is called with
     * {@code includeComputedValuesByDefault = false} and no keys, confirming
     * the JSON pipeline inherits the same structural-filter guarantee as
     * {@code map()}.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link LabeledWidget}
     * whose computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(false)}.</li>
     * <li>Invoke {@code widget.json(opts)} with no keys.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The supplier invocation counter remains at
     * {@code 0}.
     */
    @Test
    public void testComputedSupplierDoesNotFireDuringJsonWhenFlagIsFalse() {
        LabeledWidget widget = new LabeledWidget();
        widget.name = "alpha";

        SerializationOptions opts = SerializationOptions.builder()
                .includeComputedValuesByDefault(false).build();

        widget.json(opts);

        Assert.assertEquals(0, widget.labelInvocations.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a positive include list overrides
     * {@code includeComputedValuesByDefault = false} &mdash; explicitly naming
     * a {@link Computed} key still fires the supplier and produces the value,
     * which is the documented escape hatch for selective materialization.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link LabeledWidget}
     * whose computed supplier has not yet run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(false)}.</li>
     * <li>Invoke {@code widget.map(opts, "label")}, routing through the
     * positive-include branch of {@code map()} that resolves each key via
     * {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains the computed value under
     * {@code "label"} and the supplier invocation counter reflects exactly one
     * invocation.
     */
    @Test
    public void testPositiveIncludeOverridesFlagFalse() {
        LabeledWidget widget = new LabeledWidget();
        widget.name = "alpha";

        SerializationOptions opts = SerializationOptions.builder()
                .includeComputedValuesByDefault(false).build();

        Map<String, Object> result = widget.map(opts, "label");

        Assert.assertEquals("label-alpha", result.get("label"));
        Assert.assertEquals(1, widget.labelInvocations.get());
    }

    /**
     * A {@link Record} with a single {@link Computed} property whose supplier
     * increments a counter on every invocation, enabling assertions about
     * whether the supplier fired during framing.
     */
    class LabeledWidget extends Record {

        public String name;

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
