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

/**
 * A {@link Selection} that counts {@link Record Records} matching optional
 * {@link Criteria}.
 * <p>
 * The result is an {@link Integer} representing the count of matching
 * {@link Record Records}.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
@Immutable
final class CountSelection<T extends Record> extends DatabaseSelection<T> {

    /**
     * The query criteria, or {@code null} for counting all {@link Record
     * Records} of the target class.
     */
    @Nullable
    final Criteria criteria;

    /**
     * The client-side filter, or {@code null} for no filtering.
     */
    @Nullable
    final Predicate<T> filter;

    /**
     * Construct a new {@link CountSelection}.
     *
     * @param state the builder state
     */
    CountSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms);
        this.criteria = state.criteria;
        this.filter = state.filter;
    }

    @Override
    DatabaseSelection<T> duplicate() {
        BuilderState<T> state = new BuilderState<>(clazz, any);
        state.criteria = criteria;
        state.filter = filter;
        state.counting = true;
        state.realms = realms;
        return new CountSelection<>(state);
    }

    @Override
    boolean isCombinable() {
        return false;
    }

    @Override
    boolean isCounting() {
        return true;
    }

    /**
     * Return a {@link Reservation} for a count query with the given parameters.
     *
     * @param clazz the target class
     * @param criteria the query criteria
     * @param realms the realms filter
     * @param any whether to include descendants
     * @return the {@link Reservation}
     */
    static Reservation reservationFor(Class<?> clazz,
            @Nullable Criteria criteria, Realms realms, boolean any) {
        return Reservation.builder(clazz).realms(realms).any(any).counting(true)
                .criteria(criteria).build();
    }

    @Override
    Reservation reservation() {
        return reservationFor(clazz, criteria, realms, any);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CountSelection{clazz=");
        sb.append(clazz.getSimpleName());
        if(criteria != null) {
            sb.append(", criteria=").append(criteria);
        }
        sb.append(", realms=").append(realms);
        if(any) {
            sb.append(", any=true");
        }
        return sb.append('}').toString();
    }

}
