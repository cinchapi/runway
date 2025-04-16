/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;

/**
 * Tests for computed values in {@link Record}.
 */
public class RecordComputedValueTest extends ClientServerTest {

    @Test
    public void testComputedValueGeneratedOnlyOncePerMapAndNotCached() {
        Counter counter = new Counter();

        // Map operation should only compute the value once
        Map<String, Object> data = counter.map();
        Assert.assertEquals(1, data.get("count"));

        // Second map operation should increment the count again
        // (proving we don't cache between operations)
        data = counter.map();
        Assert.assertEquals(2, data.get("count"));
    }

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

    class Counter extends Record {
        private final AtomicInteger count = new AtomicInteger(0);

        @Computed
        public int count() {
            return count.incrementAndGet();
        }
    }


}