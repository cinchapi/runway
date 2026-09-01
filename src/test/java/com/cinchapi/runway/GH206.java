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

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.access.AccessControl;
import com.cinchapi.runway.access.Audience;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/206">GH-206</a>: a frame
 * that terminates with an exception must leave no residue on the thread, so a
 * later frame on the same thread renders the same result as a frame on a fresh
 * thread.
 *
 * @author Jeff Nelson
 */
public class GH206 extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a frame which throws mid-walk does not
     * poison later frames on the same thread.
     * <p>
     * <strong>Start state:</strong> A {@link Parent} whose intrinsic
     * {@code child} field links a {@link Child} with a {@link Computed}
     * {@code bomb} property that throws only while {@link Child#EXPLODE} is
     * {@code true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Frame the {@link Parent} with the key {@code child.bomb} and confirm
     * the {@code child} entry is a nested {@link Map} (sanity check).</li>
     * <li>Arm {@link Child#EXPLODE} and frame again, expecting the computed
     * failure to propagate out of the frame.</li>
     * <li>Disarm {@link Child#EXPLODE} and frame a third time on the same
     * thread.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The third frame nests the {@code child} as a
     * {@link Map} again. A {@code "(recursive link)"} placeholder
     * {@link String} instead means the failed frame leaked its recursion
     * bookkeeping.
     */
    @Test
    public void testFrameExpandsLinkedRecordAfterPriorFrameThrows() {
        Child child = new Child();
        child.name = "child";
        Parent parent = new Parent();
        parent.name = "parent";
        parent.child = child;
        Set<String> keys = ImmutableSet.of("child.bomb");
        Audience audience = Audience.anonymous();

        Map<String, Object> baseline = audience.frame(keys, parent);
        Object framed = baseline.get("child");
        Assert.assertTrue(
                "sanity: a healthy frame must nest the child, was: " + framed,
                framed instanceof Map);

        Child.EXPLODE = true;
        try {
            audience.frame(keys, parent);
            Assert.fail("The mid-frame failure should propagate");
        }
        catch (Exception expected) {
            // The crash under test; the computed bomb detonated mid-frame.
        }
        finally {
            Child.EXPLODE = false;
        }

        Map<String, Object> result = audience.frame(keys, parent);
        framed = result.get("child");
        Assert.assertTrue(
                "a frame on the same thread after a mid-frame exception must "
                        + "nest the child again; a \"(recursive link)\" "
                        + "placeholder means the failed frame leaked its "
                        + "recursion bookkeeping, was: " + framed,
                framed instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that a frame which throws while walking a
     * collection-valued link does not poison later frames on the same thread.
     * <p>
     * <strong>Start state:</strong> A {@link Parent} whose intrinsic
     * {@code children} field links a single {@link Child} with a
     * {@link Computed} {@code bomb} property that throws only while
     * {@link Child#EXPLODE} is {@code true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Frame the {@link Parent} with the key {@code children.bomb} and
     * confirm the sole element is a nested {@link Map} (sanity check).</li>
     * <li>Arm {@link Child#EXPLODE} and frame again, expecting the computed
     * failure to propagate out of the frame.</li>
     * <li>Disarm {@link Child#EXPLODE} and frame a third time on the same
     * thread.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The third frame nests the sole element as a
     * {@link Map} again. A {@code "(recursive link)"} placeholder
     * {@link String} instead means the failed frame leaked the bookkeeping it
     * records for an element of a collection, which is a distinct code path
     * from the one a singular link takes.
     */
    @Test
    public void testFrameExpandsLinkedRecordInCollectionAfterPriorFrameThrows() {
        Child child = new Child();
        child.name = "child";
        Parent parent = new Parent();
        parent.name = "parent";
        parent.children.add(child);
        Set<String> keys = ImmutableSet.of("children.bomb");
        Audience audience = Audience.anonymous();

        Object framed = Iterables.getOnlyElement(
                (Iterable<?>) audience.frame(keys, parent).get("children"));
        Assert.assertTrue(
                "sanity: a healthy frame must nest the element, was: " + framed,
                framed instanceof Map);

        Child.EXPLODE = true;
        try {
            audience.frame(keys, parent);
            Assert.fail("The mid-frame failure should propagate");
        }
        catch (Exception expected) {
            // The crash under test; the computed bomb detonated mid-frame.
        }
        finally {
            Child.EXPLODE = false;
        }

        framed = Iterables.getOnlyElement(
                (Iterable<?>) audience.frame(keys, parent).get("children"));
        Assert.assertTrue(
                "a frame on the same thread after a mid-frame exception must "
                        + "nest the collection element again; a "
                        + "\"(recursive link)\" placeholder means the failed "
                        + "frame leaked its recursion bookkeeping, was: "
                        + framed,
                framed instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that a frame which throws while walking a
     * link to a record that is not {@link AccessControl access controlled} does
     * not poison later frames on the same thread.
     * <p>
     * <strong>Start state:</strong> A {@link Parent} whose intrinsic
     * {@code plain} field links a {@link Plain} record with a {@link Computed}
     * {@code bomb} property that throws only while {@link Plain#EXPLODE} is
     * {@code true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Frame the {@link Parent} with the key {@code plain.bomb} and confirm
     * the {@code plain} entry is a nested {@link Map} (sanity check).</li>
     * <li>Arm {@link Plain#EXPLODE} and frame again, expecting the computed
     * failure to propagate out of the frame.</li>
     * <li>Disarm {@link Plain#EXPLODE} and frame a third time on the same
     * thread.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The third frame nests the {@code plain} entry
     * as a {@link Map} again. A record without {@link AccessControl} renders
     * through {@link Record#map(SerializationOptions, String...)} instead of a
     * nested frame, which is a distinct code path from the one an
     * {@link AccessControl} link takes.
     */
    @Test
    public void testFrameExpandsUncontrolledLinkedRecordAfterPriorFrameThrows() {
        Plain plain = new Plain();
        Parent parent = new Parent();
        parent.name = "parent";
        parent.plain = plain;
        Set<String> keys = ImmutableSet.of("plain.bomb");
        Audience audience = Audience.anonymous();

        Object framed = audience.frame(keys, parent).get("plain");
        Assert.assertTrue(
                "sanity: a healthy frame must nest the record, was: " + framed,
                framed instanceof Map);

        Plain.EXPLODE = true;
        try {
            audience.frame(keys, parent);
            Assert.fail("The mid-frame failure should propagate");
        }
        catch (Exception expected) {
            // The crash under test; the computed bomb detonated mid-frame.
        }
        finally {
            Plain.EXPLODE = false;
        }

        framed = audience.frame(keys, parent).get("plain");
        Assert.assertTrue(
                "a frame on the same thread after a mid-frame exception must "
                        + "nest the record again; a \"(recursive link)\" "
                        + "placeholder means the failed frame leaked its "
                        + "recursion bookkeeping, was: " + framed,
                framed instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that a frame which throws while walking a
     * collection of records that are not {@link AccessControl access
     * controlled} does not poison later frames on the same thread.
     * <p>
     * <strong>Start state:</strong> A {@link Parent} whose intrinsic
     * {@code plains} field links a single {@link Plain} record with a
     * {@link Computed} {@code bomb} property that throws only while
     * {@link Plain#EXPLODE} is {@code true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Frame the {@link Parent} with the key {@code plains.bomb} and confirm
     * the sole element is a nested {@link Map} (sanity check).</li>
     * <li>Arm {@link Plain#EXPLODE} and frame again, expecting the computed
     * failure to propagate out of the frame.</li>
     * <li>Disarm {@link Plain#EXPLODE} and frame a third time on the same
     * thread.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The third frame nests the sole element as a
     * {@link Map} again. This exercises the collection element path for a
     * record that renders through
     * {@link Record#map(SerializationOptions, String...)}, which no other
     * assertion covers.
     */
    @Test
    public void testFrameExpandsUncontrolledLinkedRecordInCollectionAfterPriorFrameThrows() {
        Plain plain = new Plain();
        Parent parent = new Parent();
        parent.name = "parent";
        parent.plains.add(plain);
        Set<String> keys = ImmutableSet.of("plains.bomb");
        Audience audience = Audience.anonymous();

        Object framed = Iterables.getOnlyElement(
                (Iterable<?>) audience.frame(keys, parent).get("plains"));
        Assert.assertTrue(
                "sanity: a healthy frame must nest the element, was: " + framed,
                framed instanceof Map);

        Plain.EXPLODE = true;
        try {
            audience.frame(keys, parent);
            Assert.fail("The mid-frame failure should propagate");
        }
        catch (Exception expected) {
            // The crash under test; the computed bomb detonated mid-frame.
        }
        finally {
            Plain.EXPLODE = false;
        }

        framed = Iterables.getOnlyElement(
                (Iterable<?>) audience.frame(keys, parent).get("plains"));
        Assert.assertTrue(
                "a frame on the same thread after a mid-frame exception must "
                        + "nest the collection element again; a "
                        + "\"(recursive link)\" placeholder means the failed "
                        + "frame leaked its recursion bookkeeping, was: "
                        + framed,
                framed instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code "(recursive link)"}
     * placeholder still renders for a genuine cycle inside a single frame.
     * <p>
     * <strong>Start state:</strong> A {@link Parent} and a {@link Child} that
     * link to each other, so walking from the {@link Parent} through the
     * {@link Child} and back closes a cycle. No frame has thrown.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Frame the {@link Parent} with the key {@code child.parent}.</li>
     * <li>Read the {@code parent} entry of the nested {@code child}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code parent} entry is the placeholder
     * {@link String} that ends with {@code "(recursive link)"}. Cycle detection
     * is the reason the in-flight bookkeeping exists, so it must survive the
     * release of that bookkeeping on the failure path.
     */
    @Test
    public void testFrameRendersPlaceholderForCycleWithinSingleFrame() {
        Child child = new Child();
        child.name = "child";
        Parent parent = new Parent();
        parent.name = "parent";
        parent.child = child;
        child.parent = parent;

        Object framed = ((Map<?, ?>) Audience.anonymous()
                .frame(ImmutableSet.of("child.parent"), parent).get("child"))
                        .get("parent");
        Assert.assertTrue(
                "a cycle within a single frame must render as the placeholder, "
                        + "was: " + framed,
                framed instanceof String
                        && ((String) framed).endsWith("(recursive link)"));
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

    }

    /**
     * A {@link Record} whose {@link Computed} {@code bomb} property throws on
     * demand, to simulate a data-dependent failure during a frame.
     */
    static class Child extends Node {

        /**
         * When {@code true}, {@link #bomb()} throws instead of returning.
         */
        static boolean EXPLODE = false;

        /**
         * An intrinsic, readable link back to the holding {@link Parent}, which
         * closes a cycle when it is set.
         */
        public Parent parent;

        /**
         * Return a value, or throw if {@link #EXPLODE} is armed.
         *
         * @return a constant value when disarmed
         */
        @Computed
        public String bomb() {
            if(EXPLODE) {
                throw new IllegalStateException("boom");
            }
            else {
                return "ok";
            }
        }
    }

    /**
     * A {@link Record} that is not {@link AccessControl access controlled}, so
     * a frame renders it through
     * {@link Record#map(SerializationOptions, String...)} rather than through a
     * nested frame. Its {@link Computed} {@code bomb} property throws on
     * demand.
     */
    static class Plain extends Record {

        /**
         * When {@code true}, {@link #bomb()} throws instead of returning.
         */
        static boolean EXPLODE = false;

        /**
         * Return a value, or throw if {@link #EXPLODE} is armed.
         *
         * @return a constant value when disarmed
         */
        @Computed
        public String bomb() {
            if(EXPLODE) {
                throw new IllegalStateException("boom");
            }
            else {
                return "ok";
            }
        }
    }

    /**
     * A {@link Record} that links a single {@link Child} through an intrinsic
     * field.
     */
    static class Parent extends Node {

        /**
         * An intrinsic, readable link to the {@link Child}.
         */
        public Child child;

        /**
         * An intrinsic, readable link to many {@link Child Children}.
         */
        public Set<Child> children = Sets.newLinkedHashSet();

        /**
         * An intrinsic, readable link to a {@link Plain} record.
         */
        public Plain plain;

        /**
         * An intrinsic, readable link to many {@link Plain} records.
         */
        public Set<Plain> plains = Sets.newLinkedHashSet();
    }
}
