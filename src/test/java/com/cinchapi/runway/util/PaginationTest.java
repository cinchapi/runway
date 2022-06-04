/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    @Test
    public void testApplyFilterAndPage() {
        List<Long> source = new ArrayList<>();
        for (long i = 1; i <= 100; ++i) {
            source.add(i);
        }
        Function<Page, Set<Long>> function = $page -> {
            Set<Long> items = new LinkedHashSet<>();
            int count = $page.skip() + 1;
            for (long i = count; (i <= $page.skip() + $page.limit())
                    && i < source.size(); ++i) {
                items.add(source.get((int) i));
            }
            return items;
        };
        Predicate<Long> filter = item -> item % 2 == 0;
        Page page = Page.of(6, 20);
        Set<Long> actual;
        do {
            actual = Pagination.applyFilterAndPage(function, filter, page);
            Set<Long> expected = source.stream().filter(filter).skip(page.skip())
                    .limit(page.limit()).collect(Collectors.toCollection(LinkedHashSet::new));
            System.out.println("actual = " + actual);
            System.out.println("expected = " + expected);
            Assert.assertEquals(expected, actual);
            page = page.next();
        }
        while(!actual.isEmpty());
    }

}
