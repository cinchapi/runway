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
package com.cinchapi.runway.access;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.Computed;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.cinchapi.runway.SerializationOptions;
import com.google.common.collect.ImmutableSet;

/**
 * Regression tests proving that a {@link Record}-valued key reached by
 * <em>naming</em> it (bare or {@code +}-prefixed) is framed into a nested
 * {@link Map}, exactly as the same key is when it rides in on the defaults.
 * <p>
 * The recursive descent in {@link Audience#frame frame} treats a key the caller
 * named without a navigation suffix as a terminal leaf and returns its value
 * verbatim. That is correct for scalar leaves but wrong for a {@link Record}
 * (or a collection of {@link Record Records}): a named {@code Record}-valued
 * key is returned raw instead of being recursively framed, so it escapes the
 * framing pipeline and reaches the JSON layer as an un-mapped {@code Record}.
 * The same value, when included via the defaults (the descent sees no
 * navigation entry for the key), is framed normally.
 * </p>
 * <p>
 * These tests assert the post-fix contract: a named {@code Record}-valued key
 * frames identically to a defaulted one. They fail against the current code,
 * which short-circuits the named key to its raw value.
 * </p>
 *
 * @author Jeff Nelson
 */
public class AudienceFrameNamedRecordKeyTest
        extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Establish the baseline that a linked
     * {@link Record} included via the defaults is framed into a nested
     * {@link Map}.
     * <p>
     * <strong>Start state:</strong> A {@link Container} with an
     * {@link AccessControl#ALL_KEYS} readable set whose intrinsic {@code link}
     * field points at a {@link Leaf}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(container)} with no explicit
     * keys, so {@code link} is included as a default.</li>
     * <li>Read the {@code link} entry from the result.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code link} value is a {@link Map} (the
     * framed {@link Leaf}), confirming the default path expands linked
     * {@link Record Records}.
     */
    @Test
    public void testDefaultIncludedLinkedRecordIsFramed() {
        Leaf leaf = new Leaf();
        leaf.name = "leaf";
        Container container = new Container();
        container.name = "container";
        container.link = leaf;

        Map<String, Object> result = Audience.anonymous().frame(container);

        Assert.assertNotNull(result);
        Object framed = result.get("link");
        Assert.assertTrue(
                "a linked Record included via defaults must be framed into a "
                        + "Map, was: " + framed,
                framed instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that a linked {@link Record} reached by
     * naming its key with a bare positive is framed into a nested {@link Map},
     * not returned raw.
     * <p>
     * <strong>Start state:</strong> A {@link Container} with an
     * {@link AccessControl#ALL_KEYS} readable set whose intrinsic {@code link}
     * field points at a {@link Leaf}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("link"),
     * container)}.</li>
     * <li>Read the {@code link} entry from the result.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code link} value is a {@link Map}, the
     * same shape produced by the default path. (Currently it is returned as a
     * raw {@link Record}.)
     */
    @Test
    public void testBareNamedLinkedRecordIsFramed() {
        Leaf leaf = new Leaf();
        leaf.name = "leaf";
        Container container = new Container();
        container.name = "container";
        container.link = leaf;

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("link"), container);

        Assert.assertNotNull(result);
        Object framed = result.get("link");
        Assert.assertTrue(
                "a Record reached by naming its key must be framed into a Map, "
                        + "not returned raw, was: " + framed,
                framed instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that a linked {@link Record} reached by an
     * additive {@code +} key is framed into a nested {@link Map}, not returned
     * raw. This mirrors a client that layers a {@link Record}-valued key onto
     * the defaults via {@code +}.
     * <p>
     * <strong>Start state:</strong> A {@link Container} with an
     * {@link AccessControl#ALL_KEYS} readable set whose intrinsic {@code link}
     * field points at a {@link Leaf}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of("+link"),
     * container)}.</li>
     * <li>Read the {@code link} entry from the result.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code link} value is a {@link Map}.
     * (Currently it is returned as a raw {@link Record}.)
     */
    @Test
    public void testAdditiveNamedLinkedRecordIsFramed() {
        Leaf leaf = new Leaf();
        leaf.name = "leaf";
        Container container = new Container();
        container.name = "container";
        container.link = leaf;

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+link"), container);

        Assert.assertNotNull(result);
        Object framed = result.get("link");
        Assert.assertTrue(
                "an additively named Record must be framed into a Map, not "
                        + "returned raw, was: " + framed,
                framed instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Establish the baseline that a {@link Computed}
     * collection of {@link Record Records} included via the defaults (with
     * {@link SerializationOptions#includeComputedValuesByDefault()} enabled)
     * has framed ({@link Map}) elements.
     * <p>
     * <strong>Start state:</strong> A {@link Container} whose {@code children}
     * supplier returns two {@link Leaf Leaves}; the readable set is
     * {@link AccessControl#ALL_KEYS}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build {@link SerializationOptions} with
     * {@code includeComputedValuesByDefault(true)}.</li>
     * <li>Invoke {@code Audience.anonymous().frame(options, ImmutableSet.of(),
     * container)} so {@code children} is included as a default.</li>
     * <li>Read the first element of the {@code children} collection.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code children} value is a
     * {@link Collection} whose first element is a {@link Map}, confirming the
     * default path frames the elements of a computed {@link Record} collection.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testDefaultIncludedComputedRecordCollectionIsFramed() {
        Container container = new Container();
        container.name = "container";
        Leaf a = new Leaf();
        a.name = "a";
        Leaf b = new Leaf();
        b.name = "b";
        container.addChild(a);
        container.addChild(b);

        SerializationOptions options = SerializationOptions.builder()
                .includeComputedValuesByDefault(true).build();
        Map<String, Object> result = Audience.anonymous().frame(options,
                ImmutableSet.of(), container);

        Assert.assertNotNull(result);
        Object framed = result.get("children");
        Assert.assertTrue("children should be a collection, was: " + framed,
                framed instanceof Collection);
        Object first = ((Collection<Object>) framed).iterator().next();
        Assert.assertTrue(
                "a @Computed Record collection included via defaults must have "
                        + "framed (Map) elements, was: " + first,
                first instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Computed} collection of
     * {@link Record Records} reached by an additive {@code +} key has framed
     * ({@link Map}) elements. This is the exact production scenario: a computed
     * collection of records named with {@code +} so it survives the lazy
     * default-exclusion of {@code @Computed} properties.
     * <p>
     * <strong>Start state:</strong> A {@link Container} whose {@code children}
     * supplier returns two {@link Leaf Leaves}; the readable set is
     * {@link AccessControl#ALL_KEYS}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Invoke {@code Audience.anonymous().frame(ImmutableSet.of(
     * "+children"), container)}.</li>
     * <li>Read the first element of the {@code children} collection.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code children} elements are {@link Map
     * Maps}. (Currently they are returned as raw {@link Record Records}.)
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAdditiveNamedComputedRecordCollectionIsFramed() {
        Container container = new Container();
        container.name = "container";
        Leaf a = new Leaf();
        a.name = "a";
        Leaf b = new Leaf();
        b.name = "b";
        container.addChild(a);
        container.addChild(b);

        Map<String, Object> result = Audience.anonymous()
                .frame(ImmutableSet.of("+children"), container);

        Assert.assertNotNull(result);
        Object framed = result.get("children");
        Assert.assertTrue("children should be a collection, was: " + framed,
                framed instanceof Collection);
        Object first = ((Collection<Object>) framed).iterator().next();
        Assert.assertTrue(
                "a named @Computed Record collection must have framed (Map) "
                        + "elements, not raw Records, was: " + first,
                first instanceof Map);
    }

    /**
     * Base fixture providing permissive {@link AccessControl} hooks and an
     * {@link AccessControl#ALL_KEYS} readable set for any audience, including
     * anonymous.
     */
    abstract static class Node extends Record implements AccessControl {

        /**
         * A human-readable name; an intrinsic, readable scalar field.
         */
        public String name;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return true;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return true;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return ALL_KEYS;
        }

        @Override
        public void deleteAs(Audience audience) {
            audience.delete(this);
        }
    }

    /**
     * A leaf {@link Record} that is linked to, or contained by, a
     * {@link Container}.
     */
    static class Leaf extends Node {}

    /**
     * A {@link Record} that both links a single {@link Leaf} (intrinsic field)
     * and exposes a {@link Computed} collection of {@link Leaf Leaves}.
     */
    static class Container extends Node {

        /**
         * An intrinsic, readable link to a single {@link Leaf}.
         */
        public Leaf link;

        /**
         * Backing store for the {@link #children()} computed property; kept
         * private so it is not itself a readable intrinsic field.
         */
        private final List<Leaf> kids = new ArrayList<>();

        /**
         * Add {@code child} to the {@link #children()} computed collection.
         *
         * @param child the {@link Leaf} to add
         */
        void addChild(Leaf child) {
            kids.add(child);
        }

        /**
         * Return the {@link Leaf Leaves} contained by this {@link Container} as
         * a {@link Computed} collection of {@link Record Records}.
         *
         * @return the contained {@link Leaf Leaves}
         */
        @Computed
        public List<Leaf> children() {
            return kids;
        }
    }
}
