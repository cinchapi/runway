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

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;

/**
 * Unit tests for {@link DynamicWritePolicy}.
 *
 * @author Jeff Nelson
 */
public class DynamicWritePolicyTest {

    /**
     * Return the {@link Field} named {@code name} from the {@link Fixture}
     * class.
     *
     * @param name the field name
     * @return the {@link Field}
     */
    private static Field field(String name) {
        try {
            return Fixture.class.getDeclaredField(name);
        }
        catch (NoSuchFieldException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link DynamicWritePolicy#permissive()
     * permissive} policy considers every field writable, regardless of
     * visibility or finality.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get the {@link DynamicWritePolicy#permissive() permissive}
     * policy.</li>
     * <li>Check {@code isWritable} for a public final field, a private field, a
     * protected field, a package-private field and a public field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isWritable} returns {@code true} for
     * every field.
     */
    @Test
    public void testPermissivePolicyAllowsEveryField() {
        DynamicWritePolicy policy = DynamicWritePolicy.permissive();
        Assert.assertTrue(policy.isWritable(field("publicFinalField")));
        Assert.assertTrue(policy.isWritable(field("privateField")));
        Assert.assertTrue(policy.isWritable(field("protectedField")));
        Assert.assertTrue(policy.isWritable(field("packagePrivateField")));
        Assert.assertTrue(policy.isWritable(field("publicField")));
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy refuses a
     * final field even when the field is public.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get the {@link DynamicWritePolicy#javaDefaults() javaDefaults}
     * policy.</li>
     * <li>Check {@code isWritable} for a public final field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isWritable} returns {@code false}.
     */
    @Test
    public void testJavaDefaultsRefusesFinalField() {
        DynamicWritePolicy policy = DynamicWritePolicy.javaDefaults();
        Assert.assertFalse(policy.isWritable(field("publicFinalField")));
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy refuses a
     * private field.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get the {@link DynamicWritePolicy#javaDefaults() javaDefaults}
     * policy.</li>
     * <li>Check {@code isWritable} for a private field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isWritable} returns {@code false}.
     */
    @Test
    public void testJavaDefaultsRefusesPrivateField() {
        DynamicWritePolicy policy = DynamicWritePolicy.javaDefaults();
        Assert.assertFalse(policy.isWritable(field("privateField")));
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy refuses a
     * protected field.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get the {@link DynamicWritePolicy#javaDefaults() javaDefaults}
     * policy.</li>
     * <li>Check {@code isWritable} for a protected field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isWritable} returns {@code false}.
     */
    @Test
    public void testJavaDefaultsRefusesProtectedField() {
        DynamicWritePolicy policy = DynamicWritePolicy.javaDefaults();
        Assert.assertFalse(policy.isWritable(field("protectedField")));
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy refuses a
     * package-private field.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get the {@link DynamicWritePolicy#javaDefaults() javaDefaults}
     * policy.</li>
     * <li>Check {@code isWritable} for a package-private field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isWritable} returns {@code false}.
     */
    @Test
    public void testJavaDefaultsRefusesPackagePrivateField() {
        DynamicWritePolicy policy = DynamicWritePolicy.javaDefaults();
        Assert.assertFalse(policy.isWritable(field("packagePrivateField")));
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy considers a
     * public non-final field writable.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get the {@link DynamicWritePolicy#javaDefaults() javaDefaults}
     * policy.</li>
     * <li>Check {@code isWritable} for a public non-final field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isWritable} returns {@code true}.
     */
    @Test
    public void testJavaDefaultsAllowsPublicField() {
        DynamicWritePolicy policy = DynamicWritePolicy.javaDefaults();
        Assert.assertTrue(policy.isWritable(field("publicField")));
    }

    /**
     * <strong>Goal:</strong> Verify that a field annotated with
     * {@link Writable} is writable under any policy, even when the field is
     * both private and final.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get the {@link DynamicWritePolicy#javaDefaults() javaDefaults}
     * policy.</li>
     * <li>Check {@code isWritable} for a private final field annotated with
     * {@link Writable}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code isWritable} returns {@code true}.
     */
    @Test
    public void testWritableAnnotationOverridesPolicy() {
        DynamicWritePolicy policy = DynamicWritePolicy.javaDefaults();
        Assert.assertTrue(policy.isWritable(field("writableField")));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link DynamicWritePolicy#builder()
     * builder} with no allowances produces the same rules as the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a policy with {@code builder().build()}.</li>
     * <li>Check {@code isWritable} for a public non-final field and for a
     * final, private, protected and package-private field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only the public non-final field is writable.
     */
    @Test
    public void testBuilderDefaultsMatchJavaDefaults() {
        DynamicWritePolicy policy = DynamicWritePolicy.builder().build();
        Assert.assertTrue(policy.isWritable(field("publicField")));
        Assert.assertFalse(policy.isWritable(field("publicFinalField")));
        Assert.assertFalse(policy.isWritable(field("privateField")));
        Assert.assertFalse(policy.isWritable(field("protectedField")));
        Assert.assertFalse(policy.isWritable(field("packagePrivateField")));
    }

    /**
     * <strong>Goal:</strong> Verify that a policy that only allows final fields
     * permits a public final field but still refuses a private field.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a policy with {@code builder().allowFinalFields().build()}.
     * </li>
     * <li>Check {@code isWritable} for a public final field and a private
     * field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The public final field is writable and the
     * private field is not.
     */
    @Test
    public void testAllowFinalFieldsDoesNotAllowPrivateFields() {
        DynamicWritePolicy policy = DynamicWritePolicy.builder()
                .allowFinalFields().build();
        Assert.assertTrue(policy.isWritable(field("publicFinalField")));
        Assert.assertFalse(policy.isWritable(field("privateField")));
    }

    /**
     * <strong>Goal:</strong> Verify that a policy that only allows private
     * fields permits a private non-final field but still refuses a private
     * final field, because finality is a separate policy axis.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a policy with {@code builder().allowPrivateFields().build()}.
     * </li>
     * <li>Check {@code isWritable} for a private non-final field and a private
     * final field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The private non-final field is writable and
     * the private final field is not.
     */
    @Test
    public void testAllowPrivateFieldsDoesNotAllowPrivateFinalFields() {
        DynamicWritePolicy policy = DynamicWritePolicy.builder()
                .allowPrivateFields().build();
        Assert.assertTrue(policy.isWritable(field("privateField")));
        Assert.assertFalse(policy.isWritable(field("privateFinalField")));
    }

    /**
     * <strong>Goal:</strong> Verify that each visibility allowance only permits
     * fields with the corresponding visibility.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a policy that allows protected fields.</li>
     * <li>Build a policy that allows package-private fields.</li>
     * <li>Check {@code isWritable} for a protected field and a package-private
     * field against both policies.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each policy permits only the field whose
     * visibility it allows.
     */
    @Test
    public void testVisibilityAllowancesAreIndependent() {
        DynamicWritePolicy allowProtected = DynamicWritePolicy.builder()
                .allowProtectedFields().build();
        DynamicWritePolicy allowPackagePrivate = DynamicWritePolicy.builder()
                .allowPackagePrivateFields().build();
        Assert.assertTrue(allowProtected.isWritable(field("protectedField")));
        Assert.assertFalse(
                allowProtected.isWritable(field("packagePrivateField")));
        Assert.assertTrue(
                allowPackagePrivate.isWritable(field("packagePrivateField")));
        Assert.assertFalse(
                allowPackagePrivate.isWritable(field("protectedField")));
    }

    /**
     * A fixture that declares one field for each modifier shape that a
     * {@link DynamicWritePolicy} evaluates.
     */
    @SuppressWarnings("unused")
    private static class Fixture {

        /**
         * A public final field.
         */
        public final String publicFinalField;

        /**
         * A public non-final field.
         */
        public String publicField;

        /**
         * A private non-final field.
         */
        private String privateField;

        /**
         * A private final field.
         */
        private final String privateFinalField;

        /**
         * A protected field.
         */
        protected String protectedField;

        /**
         * A package-private field.
         */
        String packagePrivateField;

        /**
         * A private final field that is annotated as {@link Writable}.
         */
        @Writable
        private final String writableField;

        /**
         * Construct a new instance.
         */
        private Fixture() {
            this.publicFinalField = "value";
            this.privateFinalField = "value";
            this.writableField = "value";
        }
    }

}
