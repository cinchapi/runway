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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Selection} that finds {@link Record Records} matching a
 * {@link Criteria}.
 * <p>
 * Results can optionally be sorted and paginated. The result is a
 * {@link java.util.Set Set} of matching {@link Record Records}.
 * </p>
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
public final class FindSelection<T extends Record> extends Selection<T> {

    /**
     * The query criteria.
     */
    final Criteria criteria;

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
     * Construct a new {@link FindSelection}.
     *
     * @param clazz the target class
     * @param criteria the query criteria
     * @param any whether to include descendants
     */
    FindSelection(Class<T> clazz, Criteria criteria, boolean any) {
        super(clazz, any);
        this.criteria = criteria;
    }

    /**
     * Sort the results by the given {@code order}.
     *
     * @param order the sort order
     * @return this {@link FindSelection} for chaining
     * @throws IllegalStateException if this {@link FindSelection} is not
     *             {@link State#PENDING}
     */
    public FindSelection<T> order(Order order) {
        ensurePending();
        this.order = order;
        return this;
    }

    /**
     * Paginate the results by the given {@code page}.
     *
     * @param page the pagination
     * @return this {@link FindSelection} for chaining
     * @throws IllegalStateException if this {@link FindSelection} is not
     *             {@link State#PENDING}
     */
    public FindSelection<T> page(Page page) {
        ensurePending();
        this.page = page;
        return this;
    }

    /**
     * Constrain this {@link FindSelection} to the given {@code realms}.
     *
     * @param realms the {@link Realms} filter
     * @return this {@link FindSelection} for chaining
     * @throws IllegalStateException if this {@link FindSelection} is not
     *             {@link State#PENDING}
     */
    public FindSelection<T> realms(Realms realms) {
        ensurePending();
        this.realms = realms;
        return this;
    }

    @Override
    boolean isCombinable() {
        return order == null && page == null;
    }

}
