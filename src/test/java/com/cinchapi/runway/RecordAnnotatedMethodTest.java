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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.Record.StaticAnalysis;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

/**
 * Tests for how a {@link Record} finds its {@link Computed} and {@link Derived}
 * methods through {@link StaticAnalysis}, and how {@link Record#get(String)}
 * reads the properties those methods supply.
 *
 * @author Jeff Nelson
 */
public class RecordAnnotatedMethodTest {

    /**
     * Return a copy of {@code clazz} that a separate {@link ClassLoader}
     * defines, so the copy is a distinct {@link Class} that the
     * {@link StaticAnalysis} startup scan never found.
     * <p>
     * The copy resolves every other class, including {@link Record}, through
     * the {@link ClassLoader} of {@code clazz}.
     * </p>
     *
     * @param clazz a class whose bytecode is on the test classpath
     * @return the isolated copy of {@code clazz}
     * @throws IOException if the bytecode of {@code clazz} cannot be read
     */
    private static Class<?> loadIsolatedCopy(Class<?> clazz)
            throws IOException {
        String resource = clazz.getName().replace('.', '/') + ".class";
        byte[] bytes;
        try (InputStream in = clazz.getClassLoader()
                .getResourceAsStream(resource)) {
            bytes = ByteStreams.toByteArray(in);
        }
        return new IsolatedClassLoader(clazz.getClassLoader())
                .define(clazz.getName(), bytes);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getDerivedMethods} returns a
     * class's {@link Derived} methods with non-overridden default interface
     * methods first, and returns the same list on every call.
     * <p>
     * <strong>Start state:</strong> The {@link StaticAnalysis} singleton has
     * scanned the test classpath, which holds {@link LabeledRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code getDerivedMethods} for {@link LabeledRecord}.</li>
     * <li>Call it a second time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first result holds
     * {@link Labeled#defaultLabel()} and then
     * {@link LabeledRecord#declaredLabel()}, and the second result is the same
     * list instance.
     */
    @Test
    public void testGetDerivedMethodsReturnsDefaultsThenDeclaredMethods()
            throws NoSuchMethodException {
        List<Method> methods = StaticAnalysis.instance()
                .getDerivedMethods(LabeledRecord.class);
        Assert.assertEquals(
                ImmutableList.of(Labeled.class.getMethod("defaultLabel"),
                        LabeledRecord.class.getDeclaredMethod("declaredLabel")),
                methods);
        Assert.assertSame(methods, StaticAnalysis.instance()
                .getDerivedMethods(LabeledRecord.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getComputedMethods} includes a
     * {@link Computed} method that requires parameters, and returns the same
     * list on every call.
     * <p>
     * <strong>Start state:</strong> The {@link StaticAnalysis} singleton has
     * scanned the test classpath, which holds {@link ComputedWithParameter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code getComputedMethods} for
     * {@link ComputedWithParameter}.</li>
     * <li>Call it a second time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first result holds only
     * {@link ComputedWithParameter#greeting(String)}, and the second result is
     * the same list instance.
     */
    @Test
    public void testGetComputedMethodsReturnsSameListOnEveryCall()
            throws NoSuchMethodException {
        List<Method> methods = StaticAnalysis.instance()
                .getComputedMethods(ComputedWithParameter.class);
        Assert.assertEquals(ImmutableList.of(ComputedWithParameter.class
                .getDeclaredMethod("greeting", String.class)), methods);
        Assert.assertSame(methods, StaticAnalysis.instance()
                .getComputedMethods(ComputedWithParameter.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getDerivedMethods} finds and
     * caches the {@link Derived} methods of a {@link Record} class that the
     * startup scan did not find.
     * <p>
     * <strong>Start state:</strong> An isolated copy of {@link LabeledRecord}
     * that a separate {@link ClassLoader} defines.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the isolated copy and check that {@code types()} does not
     * contain it.</li>
     * <li>Call {@code getDerivedMethods} for the copy twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first result holds
     * {@link Labeled#defaultLabel()} and then the copy's
     * {@code declaredLabel()}, and the second result is the same list instance.
     */
    @Test
    public void testGetDerivedMethodsFindsMethodsOfUnscannedClass()
            throws IOException, NoSuchMethodException {
        Class<? extends Record> isolated = loadIsolatedCopy(LabeledRecord.class)
                .asSubclass(Record.class);
        Assert.assertNotSame(LabeledRecord.class, isolated);
        Assert.assertFalse(
                StaticAnalysis.instance().types().contains(isolated));
        List<Method> methods = StaticAnalysis.instance()
                .getDerivedMethods(isolated);
        Assert.assertEquals(
                ImmutableList.of(Labeled.class.getMethod("defaultLabel"),
                        isolated.getDeclaredMethod("declaredLabel")),
                methods);
        Assert.assertSame(methods,
                StaticAnalysis.instance().getDerivedMethods(isolated));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} whose class the
     * startup scan did not find still reads its {@link Derived} properties.
     * <p>
     * <strong>Start state:</strong> An isolated copy of {@link LabeledRecord}
     * that a separate {@link ClassLoader} defines.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct an instance of the isolated copy.</li>
     * <li>Read the {@code label} key with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is {@code "declared"}, the value of
     * the class's declared {@link Derived} method.
     */
    @Test
    public void testGetReadsDerivedValueOfUnscannedClass() throws Exception {
        Record record = (Record) loadIsolatedCopy(LabeledRecord.class)
                .getDeclaredConstructor().newInstance();
        Assert.assertEquals("declared", record.get("label"));
    }

    /**
     * <strong>Goal:</strong> Verify that a declared {@link Derived} method
     * replaces a default interface method that supplies the same key.
     * <p>
     * <strong>Start state:</strong> A {@link LabeledRecord}, which inherits
     * {@link Labeled#defaultLabel()} and declares
     * {@link LabeledRecord#declaredLabel()}, both for the key {@code label}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read the {@code label} key with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is {@code "declared"}.
     */
    @Test
    public void testGetReturnsDeclaredDerivedValueWhenDefaultMethodSharesKey() {
        Assert.assertEquals("declared", new LabeledRecord().get("label"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Derived} method that requires
     * parameters fails the read of the {@link Record Record's} derived
     * properties.
     * <p>
     * <strong>Start state:</strong> A {@link DerivedWithParameter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read the {@code greeting} key with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} whose
     * message names {@link Derived}.
     */
    @Test
    public void testGetThrowsWhenDerivedMethodRequiresParameters() {
        try {
            new DerivedWithParameter().get("greeting");
            Assert.fail();
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains("annotated with Derived"));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Computed} method that
     * requires parameters fails the read of the {@link Record Record's}
     * computed properties.
     * <p>
     * <strong>Start state:</strong> A {@link ComputedWithParameter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read the {@code greeting} key with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} whose
     * message names {@link Computed}.
     */
    @Test
    public void testGetThrowsWhenComputedMethodRequiresParameters() {
        try {
            new ComputedWithParameter().get("greeting");
            Assert.fail();
        }
        catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains("annotated with Computed"));
        }
    }

    /**
     * A type that supplies a default {@link Derived} value for the key
     * {@code label}.
     */
    public interface Labeled {

        /**
         * Return the default label.
         *
         * @return {@code "default"}
         */
        @Derived("label")
        default String defaultLabel() {
            return "default";
        }
    }

    /**
     * A {@link Record} that declares its own {@link Derived} value for the key
     * that {@link Labeled} also supplies.
     * <p>
     * The class is public so that an isolated copy from another
     * {@link ClassLoader} can be constructed.
     * </p>
     */
    public static class LabeledRecord extends Record implements Labeled {

        /**
         * Return the declared label.
         *
         * @return {@code "declared"}
         */
        @Derived("label")
        public String declaredLabel() {
            return "declared";
        }
    }

    /**
     * A {@link Record} with a {@link Derived} method that requires a parameter.
     */
    static class DerivedWithParameter extends Record {

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
     * A {@link Record} with a {@link Computed} method that requires a
     * parameter.
     */
    static class ComputedWithParameter extends Record {

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
     * A {@link ClassLoader} that defines a class from supplied bytecode and
     * delegates every other lookup to its parent.
     */
    private static class IsolatedClassLoader extends ClassLoader {

        /**
         * Construct a new instance.
         *
         * @param parent the {@link ClassLoader} that resolves every class this
         *            loader does not define
         */
        IsolatedClassLoader(ClassLoader parent) {
            super(parent);
        }

        /**
         * Define the class called {@code name} from {@code bytes}.
         *
         * @param name the fully qualified class name
         * @param bytes the class's bytecode
         * @return the defined {@link Class}
         */
        Class<?> define(String name, byte[] bytes) {
            return defineClass(name, bytes, 0, bytes.length);
        }
    }

}
