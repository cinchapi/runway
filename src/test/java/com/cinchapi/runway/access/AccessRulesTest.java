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
package com.cinchapi.runway.access;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.Array;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link AccessRules}.
 *
 * @author Jeff Nelson
 */
public class AccessRulesTest {

    @Test
    public void testFilterNoKeys() {
        Assert.assertNull(AccessRules.sanitize(
                Sets.newHashSet("foo.bar", "foo.bar.baz.bang"),
                AccessControl.NO_KEYS));
    }

    @Test
    public void testFilterAllKeys() {
        Assert.assertArrayEquals(
                Array.containing("foo.bar", "foo.bar.baz.bang"),
                AccessRules.sanitize(
                        Sets.newHashSet("foo.bar", "foo.bar.baz.bang"),
                        AccessControl.ALL_KEYS));
    }

    @Test
    public void testFilterAllowlist() {
        Assert.assertArrayEquals(Array.containing("foo.bar"),
                AccessRules.sanitize(Sets.newHashSet("foo.bar", "bar.baz"),
                        ImmutableSet.of("foo")));
    }

    @Test
    public void testFilterDenylist() {
        System.out.println(Arrays.toString(AccessRules.sanitize(Sets.newHashSet("foo.bar", "bar.baz"),
                        ImmutableSet.of("-foo"))));
        Assert.assertArrayEquals(Array.containing("bar.baz"),
                AccessRules.sanitize(Sets.newHashSet("foo.bar", "bar.baz"),
                        ImmutableSet.of("-foo")));
    }

    @Test
    public void testFilterBothLists() {
        Assert.assertArrayEquals(Array.containing("foo.baz"),
                AccessRules.sanitize(Sets.newHashSet("foo.bar", "foo.baz"),
                        Sets.newHashSet("foo", "-foo.bar")));
    }

    @Test
    public void testFilterNestedDenylist() {
        Assert.assertArrayEquals(Array.containing("foo.bar", "foo.bar.baz"),
                AccessRules.sanitize(
                        Sets.newHashSet("foo.bar", "foo.bar.baz",
                                "foo.bar.baz.bang"),
                        ImmutableSet.of("-foo.bar.baz.bang")));
    }

    @Test
    public void testValidateNoKeys() {
        Assert.assertTrue(
                AccessRules.permits(ImmutableSet.of(), AccessControl.NO_KEYS));
        Assert.assertFalse(AccessRules.permits(ImmutableSet.of("foo.bar"),
                AccessControl.NO_KEYS));
    }

    @Test
    public void testValidateAllKeys() {
        Assert.assertTrue(AccessRules.permits(ImmutableSet.of("foo.bar"),
                AccessControl.ALL_KEYS));
    }

    @Test
    public void testValidateAllowlist() {
        Assert.assertTrue(AccessRules.permits(ImmutableSet.of("foo.bar"),
                Sets.newHashSet("foo")));
        Assert.assertFalse(AccessRules.permits(ImmutableSet.of("bar.baz"),
                Sets.newHashSet("foo")));
    }

    @Test
    public void testValidateDenylist() {
        Assert.assertTrue(AccessRules.permits(ImmutableSet.of("bar.baz"),
                Sets.newHashSet("-foo")));
        Assert.assertFalse(AccessRules.permits(ImmutableSet.of("foo.bar"),
                Sets.newHashSet("-foo")));
    }

    @Test
    public void testValidateBothLists() {
        Assert.assertTrue(AccessRules.permits(ImmutableSet.of("foo.baz"),
                Sets.newHashSet("foo", "-foo.bar")));
        Assert.assertFalse(AccessRules.permits(ImmutableSet.of("foo.bar"),
                Sets.newHashSet("foo", "-foo.bar")));
    }

    @Test
    public void testValidateNestedDenylist() {
        Assert.assertTrue(
                AccessRules.permits(ImmutableSet.of("foo.bar", "foo.bar.baz"),
                        Sets.newHashSet("-foo.bar.baz.bang")));
        Assert.assertFalse(AccessRules.permits(
                ImmutableSet.of("foo.bar", "foo.bar.baz.bang"),
                Sets.newHashSet("-foo.bar.baz.bang")));
    }

    @Test
    public void testFilterReproA() {
        String[] expected = Array.containing("name", "alias");
        String[] actual = AccessRules.sanitize(ImmutableSet.of(),
                ImmutableSet.of("name", "alias"));
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testSamePath() {
        Assert.assertTrue(AccessRules.isSubpath("name", "name"));
    }

    @Test
    public void testSubpathWithOneExtraSegment() {
        Assert.assertTrue(AccessRules.isSubpath("foo.bar", "foo.bar.bang"));
    }

    @Test
    public void testSubpathWithMultipleExtraSegments() {
        Assert.assertTrue(AccessRules.isSubpath("foo", "foo.bar.bang"));
    }

    @Test
    public void testDifferentSubpath() {
        Assert.assertFalse(
                AccessRules.isSubpath("foo.bar.biz", "foo.bar.bang"));
    }

    @Test
    public void testCompletelyDifferentPaths() {
        Assert.assertFalse(AccessRules.isSubpath("foo.bar", "bar.foo"));
    }

    @Test
    public void testSubpathWithSamePrefixDifferentMeaning() {
        Assert.assertFalse(AccessRules.isSubpath("name", "nameaste"));
    }

    @Test
    public void testSubpathWithSameLength() {
        Assert.assertFalse(AccessRules.isSubpath("foo.bar", "bar.foo"));
    }

    @Test
    public void testSubpathWithShorterKey() {
        Assert.assertFalse(AccessRules.isSubpath("foo.bar.baz", "foo.bar"));
    }

    @Test
    public void testSubpathWithEmptyParentPath() {
        Assert.assertFalse(AccessRules.isSubpath("", "foo.bar"));
    }

    @Test
    public void testSubpathWithEmptyKey() {
        Assert.assertFalse(AccessRules.isSubpath("foo.bar", ""));
    }

    @Test
    public void testSubpathWithBothEmpty() {
        Assert.assertTrue(AccessRules.isSubpath("", ""));
    }

}
