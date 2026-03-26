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

import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

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
public final class LoadClassSelection<T extends Record>
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
     * The client-side filter, or {@code null} for no filtering.
     */
    @Nullable
    final Predicate<T> filter;

    /**
     * Construct a new {@link LoadClassSelection}.
     *
     * @param state the builder state
     */
    LoadClassSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms);
        this.order = state.order;
        this.page = state.page;
        this.filter = state.filter;
    }

    @Override
    boolean isCombinable() {
        return order == null && page == null
                && DatabaseSelection.isNoFilter(filter);
    }

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

    @Override
    Reservation reservation() {
        return reservationFor(clazz, order, page, realms, any);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LoadClassSelection{clazz=");
        sb.append(clazz.getSimpleName());
        if(order != null) {
            sb.append(", order=").append(order);
        }
        if(page != null) {
            sb.append(", page=").append(page);
        }
        sb.append(", realms=").append(realms);
        if(any) {
            sb.append(", any=true");
        }
        return sb.append('}').toString();
    }

}
