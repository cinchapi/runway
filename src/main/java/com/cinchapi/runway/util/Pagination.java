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
    public static <T> Set<T> filterAndPage(Function<Page, Set<T>> function,
            Predicate<T> filter, Page page) {
        Set<T> records = new LinkedHashSet<>();
        int skipped = 0;
        int skip = page.skip();
        page = Page.of(0, page.limit());
        outer: for (;;) {
            Set<T> unfiltered = function.apply(page);
            if(unfiltered.isEmpty()) {
                break;
            }
            else {
                for (T record : unfiltered) {
                    if(records.size() >= page.limit()) {
                        break outer;
                    }
                    else if(filter.test(record)) {
                        if(skipped < skip) {
                            ++skipped;
                        }
                        else {
                            records.add(record);
                        }
                    }
                }
                page = page.next();
            }
        }
        return records;
    }

}
