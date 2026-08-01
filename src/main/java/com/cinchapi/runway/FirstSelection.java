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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.google.common.base.MoreObjects.ToStringHelper;

/**
 * A {@link Selection} that finds the first {@link Record} matching optional
 * {@link Criteria} under a required {@link Order}.
 * <p>
 * The result is a single {@link Record} of type {@code T}, or {@code null} if
 * no match exists. "First" is defined entirely by the {@link Order}. Unlike
 * {@link UniqueSelection}, no duplicate detection is performed when more than
 * one {@link Record} matches.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
@Immutable
final class FirstSelection<T extends Record> extends DatabaseSelection<T> {

    /**
     * The query criteria, or {@code null} for finding the first {@link Record}
     * of the target class without additional constraints.
     */
    @Nullable
    final Criteria criteria;

    /**
     * The sort order that defines which matching {@link Record} is first.
     */
    final Order order;

    /**
     * Construct a new {@link FirstSelection}.
     *
     * @param state the builder state
     */
    FirstSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms, state.filter);
        this.criteria = state.criteria;
        this.order = state.order;
    }

    @Override
    protected void describeSpec(ToStringHelper helper) {
        if(criteria != null) {
            helper.add("criteria", criteria);
        }
        helper.add("order", order);
    }

    @Override
    DatabaseSelection<T> duplicate() {
        BuilderState<T> state = new BuilderState<>(clazz, any);
        state.criteria = criteria;
        state.order = order;
        state.filter = filter;
        state.first = true;
        state.realms = realms;
        return new FirstSelection<>(state);
    }

    @Override
    boolean isCombinable() {
        return false;
    }

    @Override
    Reservation reservation() {
        return Reservation.builder(clazz).realms(realms).any(any).first(true)
                .criteria(criteria).order(order).build();
    }

}
