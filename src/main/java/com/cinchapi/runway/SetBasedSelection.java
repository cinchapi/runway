/*
 * Copyright (c) 2013-2026 Cinchapi Inc.
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
package com.cinchapi.runway;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 *
 *
 * @author jeff
 */
abstract class SetBasedSelection<T extends Record>
        extends DatabaseSelection<T> {

    /**
     * The sort order, or {@code null} for no sorting.
     */
    @Nullable
    final Order order;

    /**
     * The pagination, or {@code null} for no pagination.
     */
    @Nullable
    final Page page;

    /**
     * Construct a new instance.
     * 
     * @param clazz
     * @param any
     * @param realms
     */
    SetBasedSelection(Class<T> clazz, boolean any, Realms realms, Order order, Page page) {
        super(clazz, any, realms);
        this.order = order;
        this.page = page;
    }

}
