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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Selection} that finds {@link Record Records} matching a
 * {@link Criteria}.
 * <p>
 * Results can optionally be sorted and paginated. The result is a
 * {@link java.util.Set Set} of matching {@link Record Records}.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
@Immutable
final class FindSelection<T extends Record> extends SetBasedSelection<T> {

    /**
     * The query criteria.
     */
    final Criteria criteria;

    /**
     * The client-side filter, or {@code null} for no filtering.
     */
    @Nullable
    final Predicate<T> filter;

    /**
     * Construct a new {@link FindSelection}.
     *
     * @param state the builder state
     */
    FindSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms, state.order, state.page);
        this.criteria = state.criteria;
        this.filter = state.filter;
    }

    @Override
    DatabaseSelection<T> duplicate() {
        BuilderState<T> state = new BuilderState<>(clazz, any);
        state.criteria = criteria;
        state.order = order;
        state.page = page;
        state.filter = filter;
        state.realms = realms;
        return new FindSelection<>(state);
    }

    @Override
    boolean isCombinable() {
        return order == null && page == null
                && filter == DatabaseSelection.NO_FILTER;
    }

    /**
     * Return a {@link Reservation} for a find query with the given parameters.
     *
     * @param clazz the target class
     * @param criteria the query criteria
     * @param order the sort order
     * @param page the pagination
     * @param realms the realms filter
     * @param any whether to include descendants
     * @return the {@link Reservation}
     */
    static Reservation reservationFor(Class<?> clazz, Criteria criteria,
            @Nullable Order order, @Nullable Page page, Realms realms,
            boolean any) {
        return Reservation.builder(clazz).realms(realms).any(any)
                .criteria(criteria).order(order).page(page).build();
    }

    @Override
    Reservation reservation() {
        return reservationFor(clazz, criteria, order, page, realms, any);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("FindSelection{clazz=");
        sb.append(clazz.getSimpleName());
        sb.append(", criteria=").append(criteria);
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
