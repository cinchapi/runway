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

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link CommandGroupReadHandle} that combine the shared
 * {@link ReadHandle} contract with implementation-specific behavior. Pinned to
 * a Concourse version that exposes the {@code prepare()}/{@code submit()}
 * Command API.
 *
 * @author Jeff Nelson
 */
public class CommandGroupReadHandleTest extends ReadHandleTest {

    @Override
    protected ReadHandle newReadHandle() {
        return new CommandGroupReadHandle(concourse);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link CommandGroupReadHandle} issues
     * each read against the wrapped {@link com.cinchapi.concourse.Concourse} at
     * {@link ReadHandle#materialize()} time, so writes that occur after
     * recording but before {@code materialize()} are reflected in the result.
     * <p>
     * <strong>Start state:</strong> One record is added with
     * {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} for {@code flag = true}.</li>
     * <li>Add another {@code flag = true} record directly via the underlying
     * {@link com.cinchapi.concourse.Concourse}.</li>
     * <li>Call {@code reader.materialize()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The materialized result contains both the
     * original id and the post-recording id, because the find is issued at
     * materialize time after the add has landed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testReadIsIssuedAtMaterializeTime() {
        long original = concourse.add("flag", true);

        ReadHandle reader = newReadHandle();
        reader.find(Criteria.where().key("flag").operator(Operator.EQUALS)
                .value(true));
        long postRecording = concourse.add("flag", true);
        List<Object> results = reader.materialize();

        Set<Long> ids = (Set<Long>) results.get(0);
        Assert.assertTrue(ids.contains(original));
        Assert.assertTrue(ids.contains(postRecording));
    }

}
