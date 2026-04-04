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

import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Selection#withInjectedFilter(Selection, Predicate)}.
 *
 * @author Jeff Nelson
 */
public class SelectionWithInjectedFilterTest {

    /**
     * <strong>Goal:</strong> Verify that injecting a {@code null} filter into a
     * {@link Selection} that already has a client-side filter preserves the
     * original filter.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with a non-trivial client-side filter.</li>
     * <li>Call {@link Selection#withInjectedFilter} with {@code null}.</li>
     * <li>Resolve the result to a {@link DatabaseSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resulting {@link DatabaseSelection
     * DatabaseSelection's} filter is non-null and is not the {@code NO_FILTER}
     * sentinel.
     */
    @Test
    public void testInjectedNullFilterPreservesOriginalFilter() {
        Predicate<TestRecord> original = r -> true;
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .filter(original).build();
        Selection<TestRecord> result = Selection.withInjectedFilter(sel, null);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertNotNull(db.filter);
        Assert.assertFalse(DatabaseSelection.isNoFilter(db.filter));
    }

    /**
     * <strong>Goal:</strong> Verify that injecting the {@code NO_FILTER}
     * sentinel into a {@link Selection} that already has a client-side filter
     * preserves the original filter.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with a non-trivial client-side filter.</li>
     * <li>Call {@link Selection#withInjectedFilter} with the {@code NO_FILTER}
     * sentinel.</li>
     * <li>Resolve the result to a {@link DatabaseSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resulting {@link DatabaseSelection
     * DatabaseSelection's} filter is non-null and is not the {@code NO_FILTER}
     * sentinel.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInjectedNoFilterPreservesOriginalFilter() {
        Predicate<TestRecord> original = r -> true;
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .filter(original).build();
        Selection<TestRecord> result = Selection.withInjectedFilter(sel,
                (Predicate<? super TestRecord>) DatabaseSelection.NO_FILTER);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertNotNull(db.filter);
        Assert.assertFalse(DatabaseSelection.isNoFilter(db.filter));
    }

    /**
     * <strong>Goal:</strong> Verify that injecting a real filter into a
     * {@link Selection} that already has a client-side filter composes both
     * filters via {@code and()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with a non-trivial client-side filter.</li>
     * <li>Call {@link Selection#withInjectedFilter} with a second non-trivial
     * filter.</li>
     * <li>Resolve the result to a {@link DatabaseSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resulting filter is neither the original
     * nor the injected filter (it is a composite), and it is not the
     * {@code NO_FILTER} sentinel.
     */
    @Test
    public void testInjectedRealFilterComposesWithOriginal() {
        Predicate<TestRecord> original = r -> true;
        Predicate<TestRecord> injected = r -> true;
        Selection<TestRecord> sel = Selection.of(TestRecord.class)
                .filter(original).build();
        Selection<TestRecord> result = Selection.withInjectedFilter(sel,
                injected);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertNotNull(db.filter);
        Assert.assertNotSame(original, db.filter);
        Assert.assertNotSame(injected, db.filter);
        Assert.assertFalse(DatabaseSelection.isNoFilter(db.filter));
    }

    /**
     * <strong>Goal:</strong> Verify that injecting a {@code null} filter into a
     * {@link Selection} with no existing filter (the default {@code NO_FILTER})
     * preserves the default filter.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Selection} with no explicit filter.</li>
     * <li>Call {@link Selection#withInjectedFilter} with {@code null}.</li>
     * <li>Resolve the result to a {@link DatabaseSelection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resulting filter is the {@code NO_FILTER}
     * sentinel (unchanged).
     */
    @Test
    public void testInjectedNullFilterPreservesNoFilter() {
        Selection<TestRecord> sel = Selection.of(TestRecord.class).build();
        Selection<TestRecord> result = Selection.withInjectedFilter(sel, null);
        DatabaseSelection<TestRecord> db = (DatabaseSelection<TestRecord>) result;
        Assert.assertTrue(DatabaseSelection.isNoFilter(db.filter));
    }

    /**
     * A simple {@link Record} subclass for testing.
     */
    static class TestRecord extends Record {}

}
