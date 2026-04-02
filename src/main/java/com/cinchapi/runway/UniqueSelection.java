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
import com.google.common.base.MoreObjects.ToStringHelper;

/**
 * A {@link Selection} that finds the unique {@link Record} matching optional
 * {@link Criteria}.
 * <p>
 * The result is a single {@link Record} of type {@code T}, or {@code null} if
 * no match exists. If more than one {@link Record} matches, a
 * {@link com.cinchapi.concourse.DuplicateEntryException
 * DuplicateEntryException} is thrown during execution.
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
@Immutable
final class UniqueSelection<T extends Record> extends DatabaseSelection<T> {

    /**
     * The query criteria, or {@code null} for finding the unique {@link Record}
     * of the target class without additional constraints.
     */
    @Nullable
    final Criteria criteria;

    /**
     * Construct a new {@link UniqueSelection}.
     *
     * @param state the builder state
     */
    UniqueSelection(BuilderState<T> state) {
        super(state.clazz, state.any, state.realms, state.filter);
        this.criteria = state.criteria;
    }

    @Override
    protected void describeSpec(ToStringHelper helper) {
        if(criteria != null) {
            helper.add("criteria", criteria);
        }
    }

    @Override
    DatabaseSelection<T> duplicate() {
        BuilderState<T> state = new BuilderState<>(clazz, any);
        state.criteria = criteria;
        state.filter = filter;
        state.unique = true;
        state.realms = realms;
        return new UniqueSelection<>(state);
    }

    @Override
    boolean isCombinable() {
        return false;
    }

    @Override
    boolean isUnique() {
        return true;
    }

    @Override
    Reservation reservation() {
        return Reservation.builder(clazz).realms(realms).any(any).unique(true)
                .criteria(criteria).build();
    }

}
