/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Record} with overloaded id via interface default methods.
 *
 * @author Jeff Nelson
 */
public class RecordOverloadedIdTest extends RunwayBaseClientServerTest {

    @Test
    public void testDerivedIdOverridesPrintedIdKey() {
        IdentifiableRecord record = new IdentifiableRecord();
        record.name = "Jeff Nelson";
        record.save();

        // Verify the actual object id is preserved
        long actualId = record.id();
        Assert.assertTrue(actualId > 0);

        // Test getting the derived id property directly
        Object derivedId = record.get("id");
        Assert.assertEquals("Jeff Nelson_Identifier", derivedId);

        // Test that the derived id is included in the map output
        Map<String, Object> recordMap = record.map();
        System.out.println(recordMap);
        Assert.assertTrue(recordMap.containsKey("id"));
        Assert.assertEquals("Jeff Nelson_Identifier", recordMap.get("id"));

        // Verify the actual id is still accessible via the object
        Assert.assertEquals(actualId, record.id());
    }

    public interface Identifiable {

        String getName();

        @Derived("id")
        default String identifier() {
            return getName() + "_Identifier";
        }
    }

    class IdentifiableRecord extends Record implements Identifiable {
        String name;

        @Override
        public String getName() {
            return name;
        }
    }

}