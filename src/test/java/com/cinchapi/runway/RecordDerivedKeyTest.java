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

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Throwables;

/**
 * Tests for how a {@link Record} reads a single key that a {@link Derived}
 * method supplies, without running the {@link Derived} methods of other keys.
 *
 * @author Jeff Nelson
 */
public class RecordDerivedKeyTest {

    /**
     * <strong>Goal:</strong> Verify that reading a derived key with {@code get}
     * runs only the {@link Derived} method that supplies it.
     * <p>
     * <strong>Start state:</strong> A new {@link Counted} with the
     * {@link Derived} keys {@code a} and {@code b}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code a} with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is {@code "a"}; {@code a} ran once
     * and {@code b} never ran.
     */
    @Test
    public void testGetRunsOnlyTheDerivedMethodForTheKey() {
        Counted record = new Counted();
        Assert.assertEquals("a", record.get("a"));
        Assert.assertEquals(1, record.aRuns);
        Assert.assertEquals(0, record.bRuns);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code map} with named keys runs only
     * the {@link Derived} methods of those keys.
     * <p>
     * <strong>Start state:</strong> A new {@link Counted} with the
     * {@link Derived} keys {@code a} and {@code b}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code a} with {@code map}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result maps {@code a} to {@code "a"}, and
     * {@code b} never ran.
     */
    @Test
    public void testMapWithKeysRunsOnlyTheNamedDerivedMethods() {
        Counted record = new Counted();
        Assert.assertEquals("a", record.map("a").get("a"));
        Assert.assertEquals(0, record.bRuns);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code map} that excludes a derived
     * key never runs the {@link Derived} method of that key.
     * <p>
     * <strong>Start state:</strong> A new {@link Counted} with the
     * {@link Derived} keys {@code a} and {@code b}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read every key except {@code b} with {@code map("-b")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result maps {@code a} to {@code "a"} and
     * has no {@code b}, and {@code b} never ran.
     */
    @Test
    public void testMapExcludingDerivedKeyNeverRunsItsMethod() {
        Counted record = new Counted();
        Map<String, Object> data = record.map("-b");
        Assert.assertEquals("a", data.get("a"));
        Assert.assertFalse(data.containsKey("b"));
        Assert.assertEquals(0, record.bRuns);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Derived} method runs once per
     * {@link Record} when {@code get} reads its key before {@code map} reads
     * every key.
     * <p>
     * <strong>Start state:</strong> A new {@link Counted}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code a} with {@code get}.</li>
     * <li>Read every key with {@code map}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both reads return {@code "a"} for {@code a},
     * which ran once.
     */
    @Test
    public void testDerivedMethodRunsOnceAcrossGetAndMap() {
        Counted record = new Counted();
        Assert.assertEquals("a", record.get("a"));
        Assert.assertEquals("a", record.map().get("a"));
        Assert.assertEquals(1, record.aRuns);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code get} returns the
     * {@link Computed} value for a key that both a {@link Computed} and a
     * {@link Derived} method supply, as {@code map} does.
     * <p>
     * <strong>Start state:</strong> A new {@link BothKinds}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code value} with {@code get} and with {@code map}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both reads return {@code "computed"}.
     */
    @Test
    public void testGetPrefersComputedValueOverDerivedValue() {
        BothKinds record = new BothKinds();
        Assert.assertEquals("computed", record.get("value"));
        Assert.assertEquals("computed", record.map("value").get("value"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Derived} method that reads
     * its own key sees no value instead of running again.
     * <p>
     * <strong>Start state:</strong> A new {@link SelfReading}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code self} with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is {@code "self:null"}.
     */
    @Test
    public void testDerivedMethodThatReadsItsOwnKeySeesNull() {
        Assert.assertEquals("self:null", new SelfReading().get("self"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Derived} method that throws
     * fails only the reads that need its key and leaves the other derived
     * values in place.
     * <p>
     * <strong>Start state:</strong> A new {@link PartlyFailing}, whose
     * {@code failing} method always throws.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code ok} with {@code get}.</li>
     * <li>Read every key with {@code map} and expect a failure caused by an
     * {@link IllegalStateException}.</li>
     * <li>Read {@code ok} with {@code get} again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both reads of {@code ok} return {@code "ok"},
     * and the {@code ok} method ran once.
     */
    @Test
    public void testFailingDerivedMethodLeavesOtherDerivedValues() {
        PartlyFailing record = new PartlyFailing();
        Assert.assertEquals("ok", record.get("ok"));
        try {
            record.map();
            Assert.fail("Expected IllegalStateException");
        }
        catch (RuntimeException e) {
            Assert.assertTrue(e.toString(), Throwables
                    .getRootCause(e) instanceof IllegalStateException);
        }
        Assert.assertEquals("ok", record.get("ok"));
        Assert.assertEquals(1, record.okRuns);
    }

    /**
     * A {@link Record} with two {@link Derived} methods that count their runs.
     */
    static class Counted extends Record {

        /**
         * A field, because a {@link Record} without fields cannot read its own
         * data. See
         * <a href="https://github.com/cinchapi/runway/issues/225">GH-225</a>.
         */
        String name = "counted";

        /**
         * The number of times {@link #a()} ran.
         */
        transient int aRuns;

        /**
         * The number of times {@link #b()} ran.
         */
        transient int bRuns;

        /**
         * Return {@code "a"} and count the run.
         *
         * @return {@code "a"}
         */
        @Derived
        public String a() {
            aRuns++;
            return "a";
        }

        /**
         * Return {@code "b"} and count the run.
         *
         * @return {@code "b"}
         */
        @Derived
        public String b() {
            bRuns++;
            return "b";
        }
    }

    /**
     * A {@link Record} with a {@link Computed} and a {@link Derived} method
     * that supply the same key.
     */
    static class BothKinds extends Record {

        /**
         * A field, because a {@link Record} without fields cannot read its own
         * data. See
         * <a href="https://github.com/cinchapi/runway/issues/225">GH-225</a>.
         */
        String name = "both";

        /**
         * Return the computed value.
         *
         * @return {@code "computed"}
         */
        @Computed("value")
        public String computedValue() {
            return "computed";
        }

        /**
         * Return the derived value.
         *
         * @return {@code "derived"}
         */
        @Derived("value")
        public String derivedValue() {
            return "derived";
        }
    }

    /**
     * A {@link Record} with a {@link Derived} method that reads its own key.
     */
    static class SelfReading extends Record {

        /**
         * A field, because a {@link Record} without fields cannot read its own
         * data. See
         * <a href="https://github.com/cinchapi/runway/issues/225">GH-225</a>.
         */
        String name = "self";

        /**
         * Return {@code "self:"} followed by this method's own value as
         * {@code get} reads it.
         *
         * @return {@code "self:"} followed by the value of {@code self}
         */
        @Derived
        public String self() {
            return "self:" + get("self");
        }
    }

    /**
     * A {@link Record} with a {@link Derived} method that counts its runs and
     * one that always throws.
     */
    static class PartlyFailing extends Record {

        /**
         * A field, because a {@link Record} without fields cannot read its own
         * data. See
         * <a href="https://github.com/cinchapi/runway/issues/225">GH-225</a>.
         */
        String name = "partly";

        /**
         * The number of times {@link #ok()} ran.
         */
        transient int okRuns;

        /**
         * Return {@code "ok"} and count the run.
         *
         * @return {@code "ok"}
         */
        @Derived
        public String ok() {
            okRuns++;
            return "ok";
        }

        /**
         * Throw an {@link IllegalStateException}.
         *
         * @return never
         */
        @Derived
        public String failing() {
            throw new IllegalStateException();
        }
    }

}
