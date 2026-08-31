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

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/206">GH-206</a>: a frame
 * that terminates with an exception must leave no residue on the thread, so a
 * later frame on the same thread renders the same result as a frame on a
 * fresh thread.
 *
 * @author Jeff Nelson
 */
public class GH206 extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a frame which throws mid-walk does
     * not poison later frames on the same thread.
     * <p>
     * <strong>Start state:</strong> A {@link Parent} whose intrinsic
     * {@code child} field links a {@link Child} with a {@link Computed}
     * {@code bomb} property that throws only while {@link Child#EXPLODE} is
     * {@code true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Frame the {@link Parent} with the key {@code child.bomb} and
     * confirm the {@code child} entry is a nested {@link Map} (sanity
     * check).</li>
     * <li>Arm {@link Child#EXPLODE} and frame again, expecting the computed
     * failure to propagate out of the frame.</li>
     * <li>Disarm {@link Child#EXPLODE} and frame a third time on the same
     * thread.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The third frame nests the {@code child} as a
     * {@link Map} again. A {@code "(recursive link)"} placeholder {@link String}
     * instead means the failed frame leaked its recursion bookkeeping.
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
     * A {@link Record} whose {@link Computed} {@code bomb} property throws on
     * demand, to simulate a data-dependent failure during a frame.
     */
    static class Child extends Node {

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
    }
}
