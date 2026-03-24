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

import com.cinchapi.concourse.lang.Criteria;

/**
 * A {@link Selection} that counts {@link Record Records} matching optional
 * {@link Criteria}.
 * <p>
 * The result is an {@link Integer} representing the count of matching
 * {@link Record Records}.
 * </p>
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
public final class CountSelection<T extends Record> extends Selection<T> {

    /**
     * The query criteria, or {@code null} for counting all records of the
     * target class.
     */
    @Nullable
    final Criteria criteria;

    /**
     * Construct a new {@link CountSelection}.
     *
     * @param clazz the target class
     * @param criteria the query criteria, or {@code null}
     * @param any whether to include descendants
     */
    CountSelection(Class<T> clazz, @Nullable Criteria criteria, boolean any) {
        super(clazz, any);
        this.criteria = criteria;
    }

    /**
     * Constrain this {@link CountSelection} to the given {@code realms}.
     *
     * @param realms the {@link Realms} filter
     * @return this {@link CountSelection} for chaining
     * @throws IllegalStateException if this {@link CountSelection} is not
     *             {@link State#PENDING}
     */
    public CountSelection<T> realms(Realms realms) {
        ensurePending();
        this.realms = realms;
        return this;
    }

    @Override
    boolean isCombinable() {
        return false;
    }

    @Override
    boolean isCounting() {
        return true;
    }

}
