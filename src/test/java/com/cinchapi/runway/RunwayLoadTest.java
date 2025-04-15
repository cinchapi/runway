/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for Runway's loading functionality
 *
 * @author Jeff Nelson
 */
public class RunwayLoadTest extends RunwayBaseClientServerTest {

    @Test
    public void testRecordIsOnlyLoadedOnceSanityCheck() {
        // Test that Runway will use the same object reference across links when
        // bulk loading records
        B b = new B();
        b.name = "b";

        A a1 = new A();
        a1.name = "a1";
        a1.b = b;

        A a2 = new A();
        a2.name = "a2";
        a2.b = b;

        runway.save(b, a1, a2);
        Set<A> as = runway.load(A.class);
        B expected = null;
        for (A a : as) {
            if(expected == null) {
                expected = a.b;
            }
            else {
                Assert.assertSame(expected, a.b);
            }
        }
    }

    class A extends Record {

        String name;
        B b;

    }

    class B extends Record {

        String name;
    }

}
