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

import java.util.Set;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link BatchReader} that combine the shared {@link Reader}
 * contract with implementation-specific behavior. Pinned to a Concourse version
 * that exposes the {@code prepare()}/{@code submit()} Command API.
 *
 * @author Jeff Nelson
 */
public class BatchReaderTest extends ReaderTest {

    @Override
    protected Reader newReader() {
        return new BatchReader(concourse);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link BatchReader} defers each read
     * until the returned {@link Supplier} is resolved, so writes that occur
     * after recording but before resolution are reflected in the result.
     * <p>
     * <strong>Start state:</strong> One record is added with
     * {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} for {@code flag = true}.</li>
     * <li>Add another {@code flag = true} record directly via the underlying
     * {@link com.cinchapi.concourse.Concourse}.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved result contains both the original
     * id and the post-recording id, because the find is issued at resolution
     * time after the add has landed.
     */
    @Test
    public void testReadIsIssuedAtSupplierResolution() {
        long original = concourse.add("flag", true);

        Reader reader = newReader();
        Supplier<Set<Long>> supplier = reader.find(Criteria.where().key("flag")
                .operator(Operator.EQUALS).value(true));
        long postRecording = concourse.add("flag", true);

        Set<Long> ids = supplier.get();
        Assert.assertTrue(ids.contains(original));
        Assert.assertTrue(ids.contains(postRecording));
    }

    /**
     * <strong>Goal:</strong> Verify that when a batch's underlying submission
     * fails, every {@link Supplier} bound to that batch throws the same
     * {@link RuntimeException} instance on resolution &mdash; the failure is
     * latched onto the batch and never re-submitted.
     * <p>
     * <strong>Start state:</strong> One record is added with {@code score = 1}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record two finds on a fresh {@link BatchReader}.</li>
     * <li>Close the underlying {@link Concourse} connection so the next
     * {@code submit} call cannot succeed.</li>
     * <li>Resolve the first {@link Supplier}; capture the thrown
     * exception.</li>
     * <li>Resolve the second {@link Supplier}; capture the thrown
     * exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both invocations throw, and the second
     * invocation throws the same {@link RuntimeException} instance as the first
     * &mdash; demonstrating that the failure is latched and the batch is never
     * re-submitted.
     */
    @Test
    public void testFailedFlushLatchesAndRethrowsSameExceptionToSiblings() {
        concourse.add("score", 1);
        Reader reader = newReader();
        Supplier<Set<Long>> first = reader.find(Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0));
        Supplier<Set<Long>> second = reader.find(Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0));
        concourse.close();
        closed = true;

        RuntimeException firstFailure = null;
        try {
            first.get();
            Assert.fail("first.get() should have thrown");
        }
        catch (RuntimeException e) {
            firstFailure = e;
        }

        RuntimeException secondFailure = null;
        try {
            second.get();
            Assert.fail("second.get() should have thrown");
        }
        catch (RuntimeException e) {
            secondFailure = e;
        }

        Assert.assertSame(firstFailure, secondFailure);
    }

    @Override
    public void afterStartedTest() {
        if(!closed) {
            super.afterStartedTest();
        }
    }

    @Override
    protected String getServerVersion() {
        return "1.0.0-rc1778433818";
    }

    /**
     * Tracks whether a test closed the {@link Concourse} connection itself, so
     * {@link #afterStartedTest()} can skip the double-close.
     */
    private boolean closed = false;

}
