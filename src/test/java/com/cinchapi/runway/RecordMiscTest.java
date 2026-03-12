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

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Tests for {@link Record} comparison, multi-set, exception handling, and
 * miscellaneous operations.
 *
 * @author Jeff Nelson
 */
public class RecordMiscTest extends AbstractRecordTest {

    @Test
    public void testNoNoArgConstructor() {
        Flock flock = new Flock("Jeff Nelson");
        runway.save(flock); // TODO: change
        System.out.println(runway.load(Flock.class, flock.id()));
    }

    @Test
    public void testSetValueToNullRemovesFromDatabase() {
        Mock mock = new Mock();
        mock.alive = true;
        mock.age = 10;
        mock.name = "Mock";
        mock.save();
        mock.age = null;
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        Assert.assertNull(mock.age);
    }

    @Test
    public void testDefaultCompareToUsesId() {
        Nock a = new Nock();
        Nock b = new Nock();
        Assert.assertTrue(a.compareTo(b) < 0);
    }

    @Test
    public void testCompareToSingleKeyDefault() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 38;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "name") > 0);
        Assert.assertTrue(b.compareTo(c, "name") < 0);
        Assert.assertTrue(c.compareTo(d, "name") > 0);
        Assert.assertTrue(a.compareTo(c, "name") < 0); // When
                                                       // equal,
                                                       // the
                                                       // record
                                                       // id is
                                                       // used
                                                       // as a
                                                       // tie
                                                       // breaker
    }

    @Test
    public void testCompareToSingleKeyDescending() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 38;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "<name") < 0);
        Assert.assertTrue(b.compareTo(c, "<name") > 0);
        Assert.assertTrue(c.compareTo(d, "<name") < 0);
        Assert.assertTrue(a.compareTo(c, "<name") < 0); // When
                                                        // equal,
                                                        // the
                                                        // record
                                                        // id is
                                                        // used
                                                        // as a
                                                        // tie
                                                        // breaker
    }

    @Test
    public void testCompareToSingleKeyAscending() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 38;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "name") > 0);
        Assert.assertTrue(b.compareTo(c, "name") < 0);
        Assert.assertTrue(c.compareTo(d, "name") > 0);
        Assert.assertTrue(a.compareTo(c, "name") < 0); // When
                                                       // equal,
                                                       // the
                                                       // record
                                                       // id is
                                                       // used
                                                       // as a
                                                       // tie
                                                       // breaker
    }

    @Test
    public void testCompareToMultiKeys() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 41;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "name age") > 0);
        Assert.assertTrue(b.compareTo(c, "name age") < 0);
        Assert.assertTrue(c.compareTo(d, "name age") > 0);
        Assert.assertTrue(a.compareTo(c, "name <age") > 0);
    }

    @Test
    public void testThrowSupressedExceptions() {
        Mock a = new Mock();
        a.name = "Bob";
        Mock b = new Mock();
        b.name = "Bob";
        a.save();
        if(b.save()) {
            Assert.fail();
        }
        else {
            try {
                b.throwSupressedExceptions();
                Assert.fail();
            }
            catch (RuntimeException e) {
                Assert.assertTrue(
                        e.getMessage().startsWith("name must be unique"));
            }

        }
    }

    @Test
    public void testCompareToUsingNullValuesDoesNotNPE() {
        Mock a = new Mock();
        Mock b = new Mock();
        a.compareTo(b, "name");
        Assert.assertTrue(true); // lack of Exception means
                                 // that the test passes
    }

    @Test
    public void testMultiSetSingleKeyValuePair() {
        Mock person = new Mock();
        person.set(ImmutableMap.of("name", "Test Name"));
        Assert.assertEquals("Test Name", person.name);
    }

    @Test
    public void testMultiSetMultipleKeyValuePairs() {
        Mock person = new Mock();
        person.set(ImmutableMap.of("name", "Test Name", "age", 25));
        Assert.assertEquals("Test Name", person.name);
        Assert.assertEquals(Integer.valueOf(25), person.age);
    }

    @Test
    public void testMultiSetExistingAndDynamicAttributes() {
        Mock person = new Mock();
        person.set(ImmutableMap.of("name", "Test Name", "dynamicAttr",
                "Dynamic Value"));
        Assert.assertEquals("Test Name", person.name);
        Assert.assertEquals("Dynamic Value", person.get("dynamicAttr"));
    }

    @Test
    public void testMutliSetOverwriteExistingAttributes() {
        Mock person = new Mock();
        person.name = "Original Name";
        person.set(ImmutableMap.of("name", "New Name"));
        Assert.assertEquals("New Name", person.name);
    }

    @Test
    public void testMultuSetInvalidAttributes() {
        Mock person = new Mock();
        person.set(ImmutableMap.of("invalidAttr", "Invalid Value"));
        Assert.assertEquals("Invalid Value", person.get("invalidAttr"));
    }

}
