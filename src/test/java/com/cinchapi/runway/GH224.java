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

import com.google.common.base.Throwables;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/224">GH-224</a>: a read of
 * a {@link Record Record's} derived or computed properties that fails must
 * leave no partial properties behind, so every later read fails the same way.
 *
 * @author Jeff Nelson
 */
public class GH224 {

    /**
     * <strong>Goal:</strong> Verify that every read of a {@link Record} with a
     * {@link Derived} method that requires parameters throws, not only the
     * first.
     * <p>
     * <strong>Start state:</strong> A new {@link DerivedWithParameter}, which
     * also declares a valid {@link Derived} method.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read the {@code valid} key with {@code get} and expect an
     * {@link IllegalArgumentException}.</li>
     * <li>Read the {@code valid} key again on the same instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second read also throws an
     * {@link IllegalArgumentException}.
     */
    @Test
    public void testGetThrowsOnEveryReadWhenDerivedMethodRequiresParameters() {
        DerivedWithParameter record = new DerivedWithParameter();
        assertGetThrows(record, "valid", IllegalArgumentException.class);
        assertGetThrows(record, "valid", IllegalArgumentException.class);
    }

    /**
     * <strong>Goal:</strong> Verify that every read of a {@link Record} with a
     * {@link Computed} method that requires parameters throws, not only the
     * first.
     * <p>
     * <strong>Start state:</strong> A new {@link ComputedWithParameter}, which
     * also declares a valid {@link Computed} method.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read the {@code valid} key with {@code get} and expect an
     * {@link IllegalArgumentException}.</li>
     * <li>Read the {@code valid} key again on the same instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second read also throws an
     * {@link IllegalArgumentException}.
     */
    @Test
    public void testGetThrowsOnEveryReadWhenComputedMethodRequiresParameters() {
        ComputedWithParameter record = new ComputedWithParameter();
        assertGetThrows(record, "valid", IllegalArgumentException.class);
        assertGetThrows(record, "valid", IllegalArgumentException.class);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Derived} method that throws
     * leaves no partial derived properties, so a later read runs the methods
     * again.
     * <p>
     * <strong>Start state:</strong> A new {@link FailingDerived} whose
     * {@code failing} method throws while {@link FailingDerived#fail} is
     * {@code true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read the {@code failing} key with {@code get} and expect a failure
     * caused by an {@link IllegalStateException}.</li>
     * <li>Clear {@link FailingDerived#fail} and read the {@code failing} key
     * again on the same instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second read returns {@code "recovered"}.
     */
    @Test
    public void testGetRunsDerivedMethodsAgainAfterDerivedMethodThrows() {
        FailingDerived record = new FailingDerived();
        assertGetThrows(record, "failing", IllegalStateException.class);
        record.fail = false;
        Assert.assertEquals("recovered", record.get("failing"));
    }

    /**
     * Assert that reading {@code key} from {@code record} with
     * {@link Record#get(String)} throws an exception whose root cause is of
     * {@code type}.
     *
     * @param record the {@link Record} to read
     * @param key the key to read
     * @param type the expected type of the root cause
     */
    private static void assertGetThrows(Record record, String key,
            Class<? extends Throwable> type) {
        try {
            record.get(key);
            Assert.fail("Expected " + type.getSimpleName());
        }
        catch (RuntimeException e) {
            Assert.assertTrue(e.toString(),
                    type.isInstance(Throwables.getRootCause(e)));
        }
    }

    /**
     * A {@link Record} with a valid {@link Derived} method and one that
     * requires a parameter.
     */
    static class DerivedWithParameter extends Record {

        /**
         * Return a fixed value.
         *
         * @return {@code "valid"}
         */
        @Derived
        public String valid() {
            return "valid";
        }

        /**
         * Return a greeting for {@code name}.
         *
         * @param name the name to greet
         * @return the greeting
         */
        @Derived
        public String greeting(String name) {
            return "hello " + name;
        }
    }

    /**
     * A {@link Record} with a valid {@link Computed} method and one that
     * requires a parameter.
     */
    static class ComputedWithParameter extends Record {

        /**
         * Return a fixed value.
         *
         * @return {@code "valid"}
         */
        @Computed
        public String valid() {
            return "valid";
        }

        /**
         * Return a greeting for {@code name}.
         *
         * @param name the name to greet
         * @return the greeting
         */
        @Computed
        public String greeting(String name) {
            return "hello " + name;
        }
    }

    /**
     * A {@link Record} with a {@link Derived} method that throws on demand.
     */
    static class FailingDerived extends Record {

        /**
         * Whether {@link #failing()} throws.
         */
        transient boolean fail = true;

        /**
         * Return a fixed value, or throw while {@link #fail} is {@code true}.
         *
         * @return {@code "recovered"}
         * @throws IllegalStateException if {@link #fail} is {@code true}
         */
        @Derived
        public String failing() {
            if(fail) {
                throw new IllegalStateException();
            }
            else {
                return "recovered";
            }
        }
    }

}
