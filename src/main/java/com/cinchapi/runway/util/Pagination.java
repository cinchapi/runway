/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
import java.util.function.Predicate;
import com.cinchapi.concourse.lang.paginate.Page;

/**
 * A utility for applying {@link Page Pages}.
 *
 * @author Jeff Nelson
 */
public final class Pagination {

    /**
     * Apply the {@code filter} to the items returned from {@code function} and
     * return a {@link Set} containing those that would appear on the
     * {@code page}.
     * <p>
     * The goal is to simulate pagination being applied to the items after they
     * are {@code filter}ed. Therefore, items that might appear after those on
     * {@code page} in an unfiltered stream returned from {@code function} might
     * instead be included in the returned {@link Set} because the universe of
     * possible items over which pagination could occur only includes those that
     * pass the {@code filter}.
     * </p>
     * 
     * @param function a {@link Function} that returns all the items on a
     *            {@link Page} as a {@link Set}
     * @param filter
     * @param page
     * @return a {@link Set} containing the items on {@link Page} after applying
     *         the {@code filter} to the items as they are returned from the
     *         {@code function}
     */
    public static <T> Set<T> applyFilterAndPage(Function<Page, Set<T>> function,
            Predicate<T> filter, Page page) {
        Set<T> records = new LinkedHashSet<>();
        int offset = page.offset();
        int limit = page.limit();
        int count = 0;
        int skipped = 0;
        page = Page.of(0, limit);
        int surplusFactor = 1;
        outer: while (count < limit) {
            int prevCount = count;
            Set<T> unfiltered = function.apply(page);
            if(unfiltered.isEmpty()) {
                break;
            }
            else {
                for (T record : unfiltered) {
                    if(filter.test(record)) {
                        if(skipped < offset) {
                            ++skipped;
                        }
                        else {
                            records.add(record);
                            ++count;
                            if(count == limit) {
                                break outer;
                            }
                        }
                    }
                }
                page = page.next();
                if(prevCount == count) {
                    // The last page from the database did not contain any
                    // filter matches, so try to increase the page size in hopes
                    // of casting a wider net
                    ++surplusFactor;
                }
                else {
                    surplusFactor = Math.max(1, --surplusFactor);
                }
                page = page.size(limit * surplusFactor);
            }
        }
        return records;
    }

}
