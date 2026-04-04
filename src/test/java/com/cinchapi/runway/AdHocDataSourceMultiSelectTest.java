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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * Tests that verify {@link AdHocDataSource} records are correctly resolved when
 * {@link Runway#select(Selection...)} executes multiple {@link Selection
 * Selections} simultaneously.
 * <p>
 * These tests reproduce a bug where the multi-selection path in
 * {@link Runway#select} dispatches isolated selections to worker threads via a
 * {@code JoinableExecutorService}. Because attached {@link AdHocDataSource
 * AdHocDataSources} are stored in a {@link ThreadLocal}, worker threads cannot
 * see them, causing count and/or data selections to return empty results.
 * </p>
 *
 * @author Jeff Nelson
 */
public class AdHocDataSourceMultiSelectTest extends RunwayBaseClientServerTest {

    /**
     * Verify that a count {@link Selection} returns the correct total when
     * executed alongside a data {@link Selection} through
     * {@link Runway#select}.
     * <p>
     * This is the primary reproduction case: the count selection is dispatched
     * to a worker thread that lacks the {@link ThreadLocal} attachment, causing
     * it to query the database (which has no ad-hoc records) and return 0
     * instead of the expected count.
     * </p>
     */
    @Test
    public void testMultiSelectCountWithAttachedAdHocDataSource() {
        Collection<MockModel> data = Arrays.asList(new MockModel("alpha"),
                new MockModel("beta"), new MockModel("gamma"));
        AdHocDataSource<MockModel> source = new AdHocDataSource<MockModel>(
                MockModel.class, () -> data) {};

        runway.attach(source);
        try {
            Selection<MockModel> countSel = Selection.of(MockModel.class)
                    .count().build();
            Selection<MockModel> dataSel = Selection.of(MockModel.class)
                    .build();

            Selections results = runway.select(countSel, dataSel);
            int count = results.next();
            Set<MockModel> models = results.next();

            Assert.assertEquals(
                    "Count selection should reflect all ad-hoc records", 3,
                    count);
            Assert.assertEquals(
                    "Data selection should return all ad-hoc records", 3,
                    models.size());
        }
        finally {
            runway.detach(MockModel.class);
        }
    }

    /**
     * Verify that a count {@link Selection} returns the correct total when the
     * data {@link Selection} includes pagination, which forces both selections
     * to be non-combinable and dispatched to the thread pool.
     */
    @Test
    public void testMultiSelectCountWithPaginatedDataAndAttachedAdHocDataSource() {
        Collection<MockModel> data = Arrays.asList(new MockModel("alpha"),
                new MockModel("beta"), new MockModel("gamma"));
        AdHocDataSource<MockModel> source = new AdHocDataSource<MockModel>(
                MockModel.class, () -> data) {};

        runway.attach(source);
        try {
            Selection<MockModel> countSel = Selection.of(MockModel.class)
                    .count().build();
            Selection<MockModel> dataSel = Selection.of(MockModel.class)
                    .page(Page.sized(1)).build();

            Selections results = runway.select(countSel, dataSel);
            int count = results.next();
            Set<MockModel> models = results.next();

            Assert.assertEquals(
                    "Count should reflect total ad-hoc records, not just the page",
                    3, count);
            Assert.assertEquals(
                    "Data selection should return exactly 1 model for page size 1",
                    1, models.size());
        }
        finally {
            runway.detach(MockModel.class);
        }
    }

    /**
     * Verify that a count {@link Selection} with a filter returns the correct
     * total alongside a filtered data {@link Selection} with pagination and
     * ordering.
     * <p>
     * The filter and pagination make both selections non-combinable, which
     * forces them into the isolated (thread pool) path.
     * </p>
     */
    @Test
    public void testMultiSelectCountWithFilteredPaginatedDataAndAttachedAdHocDataSource() {
        Collection<MockModel> data = Arrays.asList(new MockModel("alpha"),
                new MockModel("beta"), new MockModel("gamma"));
        AdHocDataSource<MockModel> source = new AdHocDataSource<MockModel>(
                MockModel.class, () -> data) {};

        runway.attach(source);
        try {
            // Use a filter that accepts all records — the presence of the
            // filter is what makes the selection non-combinable
            Selection<MockModel> countSel = Selection.of(MockModel.class)
                    .count().filter(record -> true).build();
            Selection<MockModel> dataSel = Selection.of(MockModel.class)
                    .order(Order.by("name").ascending()).page(Page.sized(2))
                    .filter(record -> true).build();

            Selections results = runway.select(countSel, dataSel);
            int count = results.next();
            Set<MockModel> models = results.next();

            Assert.assertEquals(
                    "Count should reflect total ad-hoc records even with filter",
                    3, count);
            Assert.assertEquals(
                    "Data should return page-sized subset of ad-hoc records", 2,
                    models.size());
        }
        finally {
            runway.detach(MockModel.class);
        }
    }

    /**
     * Verify that a single count {@link Selection} (not multi-select) works
     * correctly. This confirms the single-selection fast path is not affected.
     */
    @Test
    public void testSingleCountSelectionWithAttachedAdHocDataSource() {
        Collection<MockModel> data = Arrays.asList(new MockModel("alpha"),
                new MockModel("beta"), new MockModel("gamma"));
        AdHocDataSource<MockModel> source = new AdHocDataSource<MockModel>(
                MockModel.class, () -> data) {};

        runway.attach(source);
        try {
            Selection<MockModel> countSel = Selection.of(MockModel.class)
                    .count().build();

            Selections results = runway.select(countSel);
            int count = results.next();

            Assert.assertEquals(
                    "Single count selection should find attached ad-hoc records",
                    3, count);
        }
        finally {
            runway.detach(MockModel.class);
        }
    }

    /**
     * A mock {@link AdHocRecord} that represents a named model.
     */
    static class MockModel extends AdHocRecord {

        String name;

        MockModel(String name) {
            this.name = name;
        }
    }

}
