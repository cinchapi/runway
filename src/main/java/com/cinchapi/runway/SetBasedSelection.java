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
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

/**
 * A {@link DatabaseSelection} whose result is a {@link java.util.Set Set} of
 * {@link Record Records}. This is the common base for {@link FindSelection} and
 * {@link LoadClassSelection}, which both support optional sorting and
 * pagination.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
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
     * Construct a new {@link SetBasedSelection}.
     *
     * @param clazz the target {@link Record} class
     * @param any whether to include descendants of {@code clazz}
     * @param realms the {@link Realms} filter
     * @param order the sort order, or {@code null} for no sorting
     * @param page the pagination, or {@code null} for no pagination
     */
    SetBasedSelection(Class<T> clazz, boolean any, Realms realms, Order order,
            Page page) {
        super(clazz, any, realms);
        this.order = order;
        this.page = page;
    }

    @Override
    protected final void describeSpec(ToStringHelper helper) {
        describeSetSpec(helper);
        if(order != null) {
            helper.add("order", order);
        }
        if(page != null) {
            helper.add("page", page);
        }
    }

    /**
     * Add selection-type-specific fields to the {@link ToStringHelper} used by
     * {@link #toString()}.
     * <p>
     * Subclasses append their distinguishing properties (e.g., criteria) to
     * {@code helper}. Common set-based fields ({@code order}, {@code page}) are
     * added by the caller and must not be duplicated here.
     *
     * @param helper the {@link MoreObjects.ToStringHelper} to populate
     */
    protected abstract void describeSetSpec(ToStringHelper helper);

}
