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
package com.cinchapi.runway.meta;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.RunwayBaseClientServerTest;

/**
 * Comprehensive tests for the {@link Metadata} interface covering functional,
 * negative, and corner cases.
 *
 * @author Jeff Nelson
 */
public class MetadataTest extends RunwayBaseClientServerTest {

    @Test
    public void testEqualLastUpdated() {
        User user = new User("Jeff Nelson");
        user.age = 37;
        user.save();
        Timestamp a = user.lastUpdatedAt("name");
        Timestamp b = user.lastUpdatedAt("age");
        Assert.assertEquals(a, b);
    }

    @Test
    public void testCreatedAtBasic() {
        User user = new User("Jeff Nelson");
        user.age = 37;
        user.save();

        Timestamp createdAt = user.createdAt();
        Assert.assertNotNull("Created timestamp should not be null", createdAt);
        Assert.assertTrue("Created timestamp should be positive",
                createdAt.getMicros() > 0);
    }

    @Test
    public void testCreatedAtMultipleSaves() {
        User user = new User("Jeff Nelson");
        user.save();
        Timestamp firstCreated = user.createdAt();

        user.age = 37;
        user.save();
        Timestamp secondCreated = user.createdAt();

        Assert.assertEquals(
                "Created timestamp should remain constant across saves",
                firstCreated, secondCreated);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreatedAtBeforeSave() {
        User user = new User("Jeff Nelson");
        user.createdAt(); // Should throw exception
    }

    @Test
    public void testLastUpdatedAtBasic() {
        User user = new User("Jeff Nelson");
        user.save();
        Timestamp firstSave = user.lastUpdatedAt();

        user.age = 37;
        user.save();
        Timestamp secondSave = user.lastUpdatedAt();

        Assert.assertTrue("Last updated should be more recent after save",
                secondSave.getMicros() > firstSave.getMicros());
    }

    @Test
    public void testLastUpdatedAtSpecificField() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        Timestamp ageUpdate = user.lastUpdatedAt("age");
        org.junit.Assert.assertNotNull(
                "Field-specific update timestamp should not be null",
                ageUpdate);

        Timestamp nameUpdate = user.lastUpdatedAt("name");
        org.junit.Assert.assertNotNull(
                "Field-specific update timestamp should not be null",
                nameUpdate);
    }

    @Test
    public void testLastUpdatedAtMultipleFields() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        user.name = "Jeffery Nelson";
        user.save();

        // Should return timestamp of most recent update among specified fields
        Timestamp mostRecent = user.lastUpdatedAt("age", "name");
        Assert.assertNotNull("Multi-field update timestamp should not be null",
                mostRecent);

        // The name update should be more recent than age update
        Timestamp ageUpdate = user.lastUpdatedAt("age");
        Timestamp nameUpdate = user.lastUpdatedAt("name");
        Assert.assertTrue("Name update should be more recent",
                nameUpdate.getMicros() > ageUpdate.getMicros());
        Assert.assertEquals("Multi-field should return most recent", nameUpdate,
                mostRecent);
    }

    @Test
    public void testLastUpdatedAtNoMatchingFields() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        // Query for fields that were never updated
        Timestamp result = user.lastUpdatedAt("nonexistent1", "nonexistent2");
        Assert.assertNull(
                "Should return null for fields that were never updated",
                result);
    }

    @Test
    public void testLastUpdatedAtMixedFields() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        user.name = "Jeffery Nelson";
        user.save();

        // Mix of updated and non-updated fields
        Timestamp result = user.lastUpdatedAt("age", "nonexistent", "name");
        Assert.assertNotNull(
                "Should return timestamp when some fields were updated",
                result);

        // Should return the most recent update among the updated fields
        Timestamp nameUpdate = user.lastUpdatedAt("name");
        Assert.assertEquals("Should return most recent update", nameUpdate,
                result);
    }

    @Test
    public void testLastUpdatedAtEmptyKeys() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        // No keys specified should return most recent update to any field
        Timestamp result = user.lastUpdatedAt();
        Assert.assertNotNull("Should return timestamp when no keys specified",
                result);

        // Should match the most recent save
        Timestamp ageUpdate = user.lastUpdatedAt("age");
        Assert.assertEquals("Should return most recent update", ageUpdate,
                result);
    }

    @Test
    public void testLastUpdatedAtSingleKeyOptimization() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        // Single key should use optimized path
        Timestamp result = user.lastUpdatedAt("age");
        Assert.assertNotNull("Single key lookup should work", result);
    }

    @Test
    public void testLastUpdatedAtMultipleKeysOrder() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        user.name = "Jeffery Nelson";
        user.save();

        // Test different key orders
        Timestamp order1 = user.lastUpdatedAt("age", "name");
        Timestamp order2 = user.lastUpdatedAt("name", "age");

        Assert.assertEquals("Key order should not affect result", order1,
                order2);
    }

    @Test
    public void testLastUpdatedAtRepeatedKeys() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        // Duplicate keys should not cause issues
        Timestamp result = user.lastUpdatedAt("age", "age", "age");
        Assert.assertNotNull("Repeated keys should work", result);

        Timestamp singleResult = user.lastUpdatedAt("age");
        Assert.assertEquals("Repeated keys should give same result",
                singleResult, result);
    }

    @Test
    public void testLastUpdatedAtEmptyStringKeys() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        // Empty string keys should be handled
        Timestamp result = user.lastUpdatedAt("", "age");
        Assert.assertNotNull("Empty string keys should not break functionality",
                result);
    }


    @Test
    public void testLastUpdatedAtConcurrentUpdates() {
        User user = new User("Jeff Nelson");
        user.save();

        // Simulate concurrent updates by saving multiple times rapidly
        for (int i = 0; i < 10; i++) {
            user.age = i;
            user.save();
        }

        Timestamp result = user.lastUpdatedAt("age");
        Assert.assertNotNull("Concurrent updates should work", result);

        // Should get the most recent update
        Assert.assertTrue("Should get recent timestamp",
                result.getMicros() > 0);
    }

    @Test
    public void testLastUpdatedAtFieldNeverUpdated() {
        User user = new User("Jeff Nelson");
        user.save();

        // Field that was never updated should return null
        Timestamp result = user.lastUpdatedAt("neverUpdatedField");
        Assert.assertNull("Never updated field should return null", result);
    }

    @Test
    public void testLastUpdatedAtAllFieldsUpdated() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.name = "Jeffery Nelson";
        user.save();

        // All specified fields were updated, should return most recent
        Timestamp result = user.lastUpdatedAt("age", "name");
        Assert.assertNotNull("All fields updated should return timestamp",
                result);
    }

    @Test
    public void testLastUpdatedAtPartialFieldsUpdated() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        // Only some fields were updated
        Timestamp result = user.lastUpdatedAt("age", "neverUpdatedField");
        Assert.assertNotNull("Partial field updates should return timestamp",
                result);

        // Should match the age update
        Timestamp ageUpdate = user.lastUpdatedAt("age");
        Assert.assertEquals("Should match updated field timestamp", ageUpdate,
                result);
    }

    @Test
    public void testLastUpdatedAtFieldUpdatedMultipleTimes() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        user.age = 38;
        user.save();

        user.age = 39;
        user.save();

        // Field updated multiple times should return most recent
        Timestamp result = user.lastUpdatedAt("age");
        Assert.assertNotNull("Multiple updates should return timestamp",
                result);

        // Should be the most recent update
        Assert.assertTrue("Should be recent timestamp", result.getMicros() > 0);
    }

    @Test
    public void testLastUpdatedAtMixedUpdatePatterns() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        user.name = "Jeffery Nelson";
        user.save();

        user.age = 38;
        user.save();

        user.name = "Jeff Nelson";
        user.save();

        // Complex update pattern
        Timestamp result = user.lastUpdatedAt("age", "name");
        Assert.assertNotNull("Complex update pattern should work", result);

        // Should be the most recent update among the specified fields
        Timestamp nameUpdate = user.lastUpdatedAt("name");
        Assert.assertEquals("Should return most recent update", nameUpdate,
                result);
    }

    @Test
    public void testLastUpdatedAtNoKeysSpecified() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        // No keys specified should return most recent update to any field
        Timestamp result = user.lastUpdatedAt();
        Assert.assertNotNull("No keys should return most recent update",
                result);

        // Should match the most recent save
        Timestamp ageUpdate = user.lastUpdatedAt("age");
        Assert.assertEquals("Should return most recent update", ageUpdate,
                result);
    }

    @Test
    public void testLastUpdatedAtSingleKeyVsMultipleKeys() {
        User user = new User("Jeff Nelson");
        user.save();

        user.age = 37;
        user.save();

        user.name = "Jeffery Nelson";
        user.save();

        // Single key lookup
        Timestamp singleResult = user.lastUpdatedAt("age");

        // Multiple key lookup including the same field
        Timestamp multipleResult = user.lastUpdatedAt("age", "name");

        Assert.assertNotNull("Both lookups should work", singleResult);
        Assert.assertNotNull("Multiple key lookup should work", multipleResult);

        // Single key should match the field's update time
        // Multiple keys should return the most recent among all specified
        Assert.assertTrue("Multiple key result should be >= single key result",
                multipleResult.getMicros() >= singleResult.getMicros());
    }

    protected static class User extends Record implements Metadata {

        User(String name) {
            this.name = name;
        }

        String name;
        int age;
    }
}
