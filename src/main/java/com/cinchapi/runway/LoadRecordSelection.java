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

import javax.annotation.concurrent.Immutable;

/**
 * A {@link Selection} that loads a single {@link Record} by its ID.
 * <p>
 * The result is a single {@link Record} instance, or {@code null} if no record
 * with the given ID exists.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
@Immutable
final class LoadRecordSelection<T extends Record> extends DatabaseSelection<T> {

    /**
     * The record ID.
     */
    final long id;

    /**
     * Construct a new {@link LoadRecordSelection}.
     *
     * @param state the builder state
     */
    LoadRecordSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms);
        this.id = state.id;
    }

    @Override
    boolean isCombinable() {
        return true;
    }

    /**
     * Return a {@link Reservation} for a load-by-ID query with the given
     * parameters.
     *
     * @param clazz the target class
     * @param id the record ID
     * @param realms the realms filter
     * @param any whether to include descendants
     * @return the {@link Reservation}
     */
    static Reservation reservationFor(Class<?> clazz, long id, Realms realms,
            boolean any) {
        return Reservation.builder(clazz).realms(realms).any(any).id(id)
                .build();
    }

    @Override
    Reservation reservation() {
        return reservationFor(clazz, id, realms, any);
    }

    @Override
    public String toString() {
        return "LoadRecordSelection{clazz=" + clazz.getSimpleName() + ", id="
                + id + ", realms=" + realms + (any ? ", any=true" : "") + '}';
    }

}
