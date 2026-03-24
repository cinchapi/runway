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

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Selection} that loads all {@link Record Records} of a given class.
 * <p>
 * Results can optionally be sorted and paginated. The result is a
 * {@link java.util.Set Set} of {@link Record Records}.
 * </p>
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
public final class LoadClassSelection<T extends Record> extends Selection<T> {

    /**
     * The sort order, or {@code null} for no sorting.
     */
    @Nullable
    Order order;

    /**
     * The pagination, or {@code null} for no pagination.
     */
    @Nullable
    Page page;

    /**
     * Construct a new {@link LoadClassSelection}.
     *
     * @param clazz the target class
     * @param any whether to include descendants
     */
    LoadClassSelection(Class<T> clazz, boolean any) {
        super(clazz, any);
    }

    /**
     * Sort the results by the given {@code order}.
     *
     * @param order the sort order
     * @return this {@link LoadClassSelection} for chaining
     * @throws IllegalStateException if this {@link LoadClassSelection} is not
     *             {@link State#PENDING}
     */
    public LoadClassSelection<T> order(Order order) {
        ensurePending();
        this.order = order;
        return this;
    }

    /**
     * Paginate the results by the given {@code page}.
     *
     * @param page the pagination
     * @return this {@link LoadClassSelection} for chaining
     * @throws IllegalStateException if this {@link LoadClassSelection} is not
     *             {@link State#PENDING}
     */
    public LoadClassSelection<T> page(Page page) {
        ensurePending();
        this.page = page;
        return this;
    }

    /**
     * Constrain this {@link LoadClassSelection} to the given {@code realms}.
     *
     * @param realms the {@link Realms} filter
     * @return this {@link LoadClassSelection} for chaining
     * @throws IllegalStateException if this {@link LoadClassSelection} is not
     *             {@link State#PENDING}
     */
    public LoadClassSelection<T> realms(Realms realms) {
        ensurePending();
        this.realms = realms;
        return this;
    }

    @Override
    boolean isCombinable() {
        return order == null && page == null;
    }

}
