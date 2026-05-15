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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link BatchReader} that combine the shared {@link Reader}
 * contract with implementation-specific behavior. Requires a Concourse server
 * that exposes the {@code prepare()}/{@code submit()} Command API.
 *
 * @author Jeff Nelson
 */
public class BatchReaderTest extends ReaderTest {

    @Override
    protected Reader instantiateReader(Concourse connection) {
        return new BatchReader(connection);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link BatchReader} defers each read
     * until {@link Reader#drain()}, so writes that occur after recording but
     * before {@code drain()} are reflected in the resolved value.
     * <p>
     * <strong>Start state:</strong> One record is added with
     * {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} for {@code flag = true}.</li>
     * <li>Add another {@code flag = true} record directly via the underlying
     * {@link com.cinchapi.concourse.Concourse}.</li>
     * <li>Resolve the {@link Pending} via {@link Reader#drain()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved result contains both the original
     * id and the post-recording id, because the find is issued during
     * {@code drain()} after the post-recording add has landed.
     */
    @Test
    public void testReadIsIssuedAtDrain() {
        long original = client.add("flag", true);

        Reader reader = newReader();
        Pending<Set<Long>> pending = reader.find(Criteria.where().key("flag")
                .operator(Operator.EQUALS).value(true));
        long postRecording = client.add("flag", true);

        Set<Long> ids = resolve(reader, pending);
        Assert.assertTrue(ids.contains(original));
        Assert.assertTrue(ids.contains(postRecording));
    }

    /**
     * <strong>Goal:</strong> Verify that when a batch's underlying submission
     * fails, {@link Reader#drain()} throws and {@link Pending Pendings} bound
     * to the failed batch do not resolve.
     * <p>
     * <strong>Start state:</strong> One record is added with {@code score = 1}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record two finds on a fresh {@link BatchReader}.</li>
     * <li>Close the {@link BatchReader Reader's} underlying
     * {@link com.cinchapi.concourse.Concourse Concourse} connection so the next
     * {@code submit} call cannot succeed.</li>
     * <li>Call {@link Reader#drain()}; capture the thrown exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@link Reader#drain()} throws a
     * {@link RuntimeException} and neither {@link Pending Pending's} sink
     * fires.
     */
    @Test
    public void testFailedFlushDuringDrainThrows() {
        client.add("score", 1);
        Reader reader = newReader();
        boolean[] firstFired = new boolean[1];
        boolean[] secondFired = new boolean[1];
        reader.find(Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0))
                .onResolve(ids -> firstFired[0] = true);
        reader.find(Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0))
                .onResolve(ids -> secondFired[0] = true);
        reader.concourse().close();

        RuntimeException failure = null;
        try {
            reader.drain();
            Assert.fail("drain() should have thrown");
        }
        catch (RuntimeException e) {
            failure = e;
        }

        Assert.assertNotNull(failure);
        Assert.assertFalse("first sink should not have fired", firstFired[0]);
        Assert.assertFalse("second sink should not have fired", secondFired[0]);
    }

}
