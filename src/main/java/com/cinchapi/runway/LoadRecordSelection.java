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

import com.google.common.base.MoreObjects.ToStringHelper;

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
        super(state.clazz, state.any, state.realms, state.filter);
        this.id = state.id;
    }

    @Override
    protected void describeSpec(ToStringHelper helper) {
        helper.add("id", id);
    }

    @Override
    DatabaseSelection<T> duplicate() {
        BuilderState<T> state = new BuilderState<>(clazz, any);
        state.id = id;
        state.filter = filter;
        state.realms = realms;
        return new LoadRecordSelection<>(state);
    }

    @Override
    boolean isCombinable() {
        return true;
    }

    @Override
    Reservation reservation() {
        return Reservation.builder(clazz).realms(realms).any(any).id(id)
                .build();
    }

}
