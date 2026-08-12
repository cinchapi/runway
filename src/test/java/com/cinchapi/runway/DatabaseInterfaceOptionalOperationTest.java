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

import org.junit.Test;

import com.cinchapi.concourse.lang.sort.Order;

/**
 * Tests that the optional persistence operations on {@link DatabaseInterface}
 * are refused by an implementation that only supports reads.
 *
 * @author Jeff Nelson
 */
public class DatabaseInterfaceOptionalOperationTest {

    /**
     * A {@link DatabaseInterface} that supplies only the required read
     * primitive.
     */
    private final DatabaseInterface db = selections -> null;

    /**
     * <strong>Goal:</strong> Verify that the {@code intern} default is an
     * optional operation.
     * <p>
     * <strong>Start state:</strong> A {@link DatabaseInterface} that implements
     * only {@code select}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testInternDefaultIsUnsupported() {
        db.intern((Record) null);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code findUniqueAndUpdate}
     * default is an optional operation.
     * <p>
     * <strong>Start state:</strong> A {@link DatabaseInterface} that implements
     * only {@code select}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueAndUpdate}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testFindUniqueAndUpdateDefaultIsUnsupported() {
        db.findUniqueAndUpdate(null, null, "key", value -> value);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code findAnyUniqueAndUpdate}
     * default is an optional operation.
     * <p>
     * <strong>Start state:</strong> A {@link DatabaseInterface} that implements
     * only {@code select}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findAnyUniqueAndUpdate}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testFindAnyUniqueAndUpdateDefaultIsUnsupported() {
        db.findAnyUniqueAndUpdate(null, null, "key", value -> value);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code findFirstAndUpdate} default
     * is an optional operation.
     * <p>
     * <strong>Start state:</strong> A {@link DatabaseInterface} that implements
     * only {@code select}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findFirstAndUpdate}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testFindFirstAndUpdateDefaultIsUnsupported() {
        db.findFirstAndUpdate(null, null, Order.by("key").ascending(), "key",
                value -> value);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code findAnyFirstAndUpdate}
     * default is an optional operation.
     * <p>
     * <strong>Start state:</strong> A {@link DatabaseInterface} that implements
     * only {@code select}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findAnyFirstAndUpdate}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testFindAnyFirstAndUpdateDefaultIsUnsupported() {
        db.findAnyFirstAndUpdate(null, null, Order.by("key").ascending(), "key",
                value -> value);
    }

}
