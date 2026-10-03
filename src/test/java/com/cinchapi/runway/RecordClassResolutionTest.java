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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Random;
import com.cinchapi.runway.Record.StaticAnalysis;

/**
 * Tests for resolving a {@link Record} class from its stored name through
 * {@link StaticAnalysis#getRecordClass(String)}.
 *
 * @author Jeff Nelson
 */
public class RecordClassResolutionTest {

    /**
     * <strong>Goal:</strong> Verify that every {@link Record} class the startup
     * scan found resolves from its name to the scanned {@link Class} object.
     * <p>
     * <strong>Start state:</strong> The {@link StaticAnalysis} singleton has
     * scanned the test classpath.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read every scanned type from {@code types()}.</li>
     * <li>Check that the scan found {@code PointGuard}, so the loop below
     * runs.</li>
     * <li>Resolve each type by its name with {@code getRecordClass}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each name resolves to the same {@link Class}
     * object that the scan found.
     */
    @Test
    public void testGetRecordClassReturnsScannedClassForEveryType() {
        StaticAnalysis analysis = StaticAnalysis.instance();
        Set<Class<? extends Record>> types = analysis.types();
        Assert.assertTrue(
                types.contains(RunwayBaseClientServerTest.PointGuard.class));
        for (Class<? extends Record> type : types) {
            Assert.assertSame(type, analysis.getRecordClass(type.getName()));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a name the startup scan did not find
     * still resolves through {@link Class#forName(String)}.
     * <p>
     * <strong>Start state:</strong> The {@link StaticAnalysis} singleton has
     * scanned the test classpath, which holds no {@link Record} class that the
     * scan misses, so a class outside the {@link Record} hierarchy stands in
     * for one.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Resolve the name of {@link String} with {@code getRecordClass}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code String.class}.
     */
    @Test
    public void testGetRecordClassFallsBackToClassForNameWhenNotScanned() {
        Class<?> clazz = StaticAnalysis.instance()
                .getRecordClass(String.class.getName());
        Assert.assertSame(String.class, clazz);
    }

    /**
     * <strong>Goal:</strong> Verify that a name no class has fails the same way
     * as a direct {@link Class#forName(String)} lookup.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a random class name under the {@code com.cinchapi.runway}
     * package.</li>
     * <li>Resolve it with {@code getRecordClass}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RuntimeException} whose cause is a
     * {@link ClassNotFoundException}.
     */
    @Test
    public void testGetRecordClassThrowsWhenNoClassExists() {
        String name = "com.cinchapi.runway." + Random.getSimpleString();
        try {
            StaticAnalysis.instance().getRecordClass(name);
            Assert.fail();
        }
        catch (RuntimeException e) {
            Assert.assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
    }

}
