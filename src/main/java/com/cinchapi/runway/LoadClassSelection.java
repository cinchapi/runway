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
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.google.common.base.MoreObjects;

/**
 * A {@link Selection} that loads all {@link Record Records} of a given class.
 * <p>
 * Results can optionally be sorted and paginated. The result is a
 * {@link java.util.Set Set} of {@link Record Records}.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
@Immutable
final class LoadClassSelection<T extends Record> extends SetBasedSelection<T> {

    /**
     * Return a {@link Reservation} for a load-all query with the given
     * parameters.
     *
     * @param clazz the target class
     * @param order the sort order
     * @param page the pagination
     * @param realms the realms filter
     * @param any whether to include descendants
     * @return the {@link Reservation}
     */
    static Reservation reservationFor(Class<?> clazz, @Nullable Order order,
            @Nullable Page page, Realms realms, boolean any) {
        return Reservation.builder(clazz).realms(realms).any(any).order(order)
                .page(page).build();
    }

    /**
     * Construct a new {@link LoadClassSelection}.
     *
     * @param state the builder state
     */
    LoadClassSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms, state.order, state.page);
        this.filter = state.filter;
    }

    @Override
    protected void describeSetSpec(MoreObjects.ToStringHelper helper) {
        // No additional fields beyond what SetBasedSelection
        // provides.
    }

    @Override
    DatabaseSelection<T> duplicate() {
        BuilderState<T> state = new BuilderState<>(clazz, any);
        state.order = order;
        state.page = page;
        state.filter = filter;
        state.realms = realms;
        return new LoadClassSelection<>(state);
    }

    @Override
    boolean isCombinable() {
        return order == null && page == null
                && DatabaseSelection.isNoFilter(filter);
    }

    @Override
    Reservation reservation() {
        return reservationFor(clazz, order, page, realms, any);
    }

}
