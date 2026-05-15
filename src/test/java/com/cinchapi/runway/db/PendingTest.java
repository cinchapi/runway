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
package com.cinchapi.runway.db;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Pending}.
 *
 * @author Jeff Nelson
 */
public class PendingTest {

    /**
     * <strong>Goal:</strong> Verify that {@link Pending#of(Object)} delivers
     * the supplied value synchronously to a sink registered via
     * {@link Pending#onResolve}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Pending} for the value {@code "hello"}.</li>
     * <li>Register a sink that captures the value into an
     * {@link AtomicReference}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The sink runs immediately and the captured
     * value is {@code "hello"}.
     */
    @Test
    public void testOfDeliversSynchronously() {
        AtomicReference<String> result = new AtomicReference<>();
        Pending.of("hello").onResolve(result::set);
        Assert.assertEquals("hello", result.get());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Pending#map(java.util.function.Function)} applied to an
     * already-resolved {@link Pending} produces a {@link Pending} of the
     * transformed value, delivered synchronously.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Pending} for the value {@code 7}.</li>
     * <li>Apply {@code .map(Object::toString)}.</li>
     * <li>Capture the chained value via {@link Pending#onResolve}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The captured value is {@code "7"}.
     */
    @Test
    public void testMapTransformsResolvedValue() {
        AtomicReference<String> result = new AtomicReference<>();
        Pending.of(7).map(Object::toString).onResolve(result::set);
        Assert.assertEquals("7", result.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Pending#then} chains a
     * follow-on {@link Pending} and delivers the chained value to a sink
     * registered on the chained {@link Pending}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Pending} for the value {@code "a"}.</li>
     * <li>Chain {@code .then(value -> Pending.of(value + "b"))}.</li>
     * <li>Capture the chained value.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The captured value is {@code "ab"}.
     */
    @Test
    public void testThenChainsFollowOnPending() {
        AtomicReference<String> result = new AtomicReference<>();
        Pending.of("a").then(value -> Pending.of(value + "b"))
                .onResolve(result::set);
        Assert.assertEquals("ab", result.get());
    }

    /**
     * <strong>Goal:</strong> Verify that multiple sinks registered against the
     * same already-resolved {@link Pending} all receive the value.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Pending} for the value {@code 42}.</li>
     * <li>Register two sinks, each capturing into its own
     * {@link AtomicReference}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both captured values are {@code 42}.
     */
    @Test
    public void testMultipleSinksReceiveValue() {
        AtomicReference<Integer> a = new AtomicReference<>();
        AtomicReference<Integer> b = new AtomicReference<>();
        Pending<Integer> pending = Pending.of(42);
        pending.onResolve(a::set);
        pending.onResolve(b::set);
        Assert.assertEquals(Integer.valueOf(42), a.get());
        Assert.assertEquals(Integer.valueOf(42), b.get());
    }

}
