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
public final class FindSelection<T extends Record>
        extends DatabaseSelection<T> {

    /**
     * The query criteria.
     */
    final Criteria criteria;

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
     * Construct a new {@link FindSelection}.
     *
     * @param state the builder state
     */
    FindSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms);
        this.criteria = state.criteria;
        this.order = state.order;
        this.page = state.page;
        this.filter = state.filter;
    }

    @Override
    boolean isCombinable() {
        return order == null && page == null && filter == null;
    }

}
