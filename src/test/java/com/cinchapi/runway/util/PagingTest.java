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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.paginate.Page;

/**
 * Unit tests for {@link Paging}.
 *
 * @author Jeff Nelson
 */
public class PagingTest {
    
    @Test
    public void testFilterAndPaginate() {
        Function<Page, Set<Long>> function = $page -> {
            Set<Long> items = new LinkedHashSet<>();
            int count = $page.skip() + 1;
            for(long i = count;  i <= $page.skip() + $page.limit(); ++i) {
                items.add(i);
            }
            return items;
        };
        Page page = Page.of(6, 20);
        Set<Long> items = Paging.filterAndPaginate(function, item -> item % 2 == 0, page);
        Set<Long> expected = new LinkedHashSet<>();
        long count = 8;
        while(expected.size() < 20) {
            expected.add(count);
            count += 2;
        }
        Assert.assertEquals(expected, items);
    }

}
