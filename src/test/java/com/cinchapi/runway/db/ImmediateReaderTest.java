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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link IncrementalReader} that combine the shared
 * {@link Reader} contract with implementation-specific behavior.
 *
 * @author Jeff Nelson
 */
public class ImmediateReaderTest extends ReaderTest {

    @Override
    protected Reader newReader() {
        return new IncrementalReader(concourse);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link IncrementalReader} issues each
     * read against the wrapped {@link com.cinchapi.concourse.Concourse} at
     * recording time, so writes that occur after recording but before the
     * {@link Supplier} is resolved do not affect the result.
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
     * <strong>Expected:</strong> The resolved result contains only the original
     * id; the post-recording add does not appear.
     */
    @Test
    public void testReadIsIssuedAtRecordingTime() {
        long original = concourse.add("flag", true);

        Reader reader = newReader();
        Supplier<Set<Long>> supplier = reader.find(Criteria.where().key("flag")
                .operator(Operator.EQUALS).value(true));
        long postRecording = concourse.add("flag", true);

        Set<Long> ids = supplier.get();
        Assert.assertTrue(ids.contains(original));
        Assert.assertFalse(ids.contains(postRecording));
    }

}
