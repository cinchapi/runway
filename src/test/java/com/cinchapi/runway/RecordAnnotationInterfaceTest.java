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

import com.cinchapi.concourse.time.Time;

/**
 * Unit tests for {@link Record} interface annotations.
 *
 * @author Jeff Nelson
 */
public class RecordAnnotationInterfaceTest extends RunwayBaseClientServerTest {

    @Test
    public void testDerivedAnnotationOnInterfaceDefaultMethod() {
        TestRecord record = new TestRecord();
        record.firstName = "Jeff";
        record.lastName = "Nelson";
        record.save();

        // Test getting the derived property directly
        String fullName = record.get("fullName");
        Assert.assertEquals("Jeff Nelson", fullName);

        // Test that the derived property is included in the map output
        Map<String, Object> recordMap = record.map();
        Assert.assertTrue(recordMap.containsKey("fullName"));
        Assert.assertEquals("Jeff Nelson", recordMap.get("fullName"));
    }

    @Test
    public void testComputedAnnotationOnInterfaceDefaultMethod() {
        ComputedTestRecord record = new ComputedTestRecord();
        record.name = "Jeff";
        record.save();

        // Test getting the computed property directly
        String timestampedName = record.get("timestampedName");
        Assert.assertTrue(timestampedName.startsWith("Jeff_"));
        Assert.assertTrue(timestampedName.contains("_"));

        // Test that the computed property is included in the map output
        Map<String, Object> recordMap = record.map();
        Assert.assertTrue(recordMap.containsKey("timestampedName"));
        String mapTimestampedName = (String) recordMap.get("timestampedName");
        Assert.assertTrue(mapTimestampedName.startsWith("Jeff_"));
        Assert.assertTrue(mapTimestampedName.contains("_"));

        // Test that computed values are dynamic (not cached)
        String firstCall = record.get("timestampedName");
        String secondCall = record.get("timestampedName");
        Assert.assertNotEquals(firstCall, secondCall);
    }

    public interface Nameable {
        String getFirstName();

        String getLastName();

        @Derived
        default String fullName() {
            return getFirstName() + " " + getLastName();
        }
    }

    public interface Timestampable {
        String getName();

        @Computed
        default String timestampedName() {
            return getName() + "_" + Time.now();
        }
    }

    class TestRecord extends Record implements Nameable {
        String firstName;
        String lastName;

        @Override
        public String getFirstName() {
            return firstName;
        }

        @Override
        public String getLastName() {
            return lastName;
        }
    }

    class ComputedTestRecord extends Record implements Timestampable {
        String name;

        @Override
        public String getName() {
            return name;
        }
    }

}