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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.util.Obligations.Action;

/**
 * Unit tests for {@link Obligations}.
 *
 * @author Jeff Nelson
 */
public class ObligationsTest {

    /**
     * <strong>Goal:</strong> Verify that all actions run even when an earlier
     * action throws an exception.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create three actions: the first increments a counter, the second
     * throws, and the third increments the counter again.</li>
     * <li>Call {@link Obligations#runAll(Action...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The counter is {@code 2}, confirming both
     * non-throwing actions executed. An {@link Exception} is thrown.
     */
    @Test
    public void testAllActionsRunEvenWhenOneThrows() {
        AtomicInteger counter = new AtomicInteger(0);
        try {
            Obligations.runAll(() -> counter.incrementAndGet(), () -> {
                throw new Exception("fail");
            }, () -> counter.incrementAndGet());
            Assert.fail("Expected exception");
        }
        catch (Exception e) {
            Assert.assertEquals(2, counter.get());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that no exception is thrown when all
     * actions succeed.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create three actions that each add to a list.</li>
     * <li>Call {@link Obligations#runAll(Action...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> No exception is thrown and all three items are
     * in the list.
     */
    @Test
    public void testNoExceptionWhenAllActionsSucceed() throws Exception {
        List<String> items = new ArrayList<>();
        Obligations.runAll(() -> items.add("a"), () -> items.add("b"),
                () -> items.add("c"));
        Assert.assertEquals(3, items.size());
    }

    /**
     * <strong>Goal:</strong> Verify that when multiple actions throw, the first
     * exception is the primary and subsequent ones are suppressed.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create three actions that each throw a distinct exception.</li>
     * <li>Call {@link Obligations#runAll(Action...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The thrown exception has the first error's
     * message and two suppressed exceptions with the second and third messages.
     */
    @Test
    public void testMultipleFailuresUseSuppressedExceptions() {
        try {
            Obligations.runAll(() -> {
                throw new Exception("first");
            }, () -> {
                throw new Exception("second");
            }, () -> {
                throw new Exception("third");
            });
            Assert.fail("Expected exception");
        }
        catch (Exception e) {
            Assert.assertEquals("first", e.getMessage());
            Throwable[] suppressed = e.getSuppressed();
            Assert.assertEquals(2, suppressed.length);
            Assert.assertEquals("second", suppressed[0].getMessage());
            Assert.assertEquals("third", suppressed[1].getMessage());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that passing no actions completes without
     * error.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Obligations#runAll(Action...)} with no arguments.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> No exception is thrown.
     */
    @Test
    public void testEmptyActionsDoesNotThrow() throws Exception {
        Obligations.runAll();
    }

    /**
     * <strong>Goal:</strong> Verify that actions execute in the order they are
     * provided.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create three actions that each append a distinct value to a
     * list.</li>
     * <li>Call {@link Obligations#runAll(Action...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The list contains the values in the order the
     * actions were provided.
     */
    @Test
    public void testActionsExecuteInOrder() throws Exception {
        List<Integer> order = new ArrayList<>();
        Obligations.runAll(() -> order.add(1), () -> order.add(2),
                () -> order.add(3));
        Assert.assertEquals(Integer.valueOf(1), order.get(0));
        Assert.assertEquals(Integer.valueOf(2), order.get(1));
        Assert.assertEquals(Integer.valueOf(3), order.get(2));
    }

    /**
     * <strong>Goal:</strong> Verify that a checked {@link Exception} thrown
     * from an action is rethrown directly without wrapping.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create an action that throws a checked {@link Exception}.</li>
     * <li>Call {@link Obligations#runAll(Action...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exact {@link Exception} is thrown, not
     * wrapped in a {@link RuntimeException}.
     */
    @Test(expected = Exception.class)
    public void testCheckedExceptionRethrownDirectly() throws Exception {
        Obligations.runAll(() -> {
            throw new Exception("boom");
        });
    }

}
