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
package com.cinchapi.runway.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.paginate.Page;

/**
 * Unit tests for {@link Pagination}.
 *
 * @author Jeff Nelson
 */
public class PaginationTest {

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Pagination#applyFilterAndPage(Function, Predicate, Page)} returns
     * the same items as filtering the whole source and then paging the filtered
     * stream.
     * <p>
     * <strong>Start state:</strong> A source list of the numbers 1 through 100.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Define a page function that faithfully pages the source from
     * {@code skip()}.</li>
     * <li>Apply an even-number filter with successive {@link Page Pages} until
     * the result is empty.</li>
     * <li>Compare each result to the filtered source stream paged with the same
     * {@link Page}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Every page matches the oracle.
     */
    @Test
    public void testApplyFilterAndPage() {
        List<Long> source = new ArrayList<>();
        for (long i = 1; i <= 100; ++i) {
            source.add(i);
        }
        Function<Page, Set<Long>> function = $page -> {
            Set<Long> items = new LinkedHashSet<>();
            for (int i = $page.skip(); i < $page.skip() + $page.limit()
                    && i < source.size(); ++i) {
                items.add(source.get(i));
            }
            return items;
        };
        Predicate<Long> filter = item -> item % 2 == 0;
        Page page = Page.skipLimit(6, 20);
        Set<Long> actual;
        do {
            actual = Pagination.applyFilterAndPage(function, filter, page);
            Set<Long> expected = source.stream().filter(filter)
                    .skip(page.skip()).limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            Assert.assertEquals(expected, actual);
            page = page.next();
        }
        while (!actual.isEmpty());
    }

}
