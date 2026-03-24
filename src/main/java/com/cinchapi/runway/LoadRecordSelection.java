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

/**
 * A {@link Selection} that loads a single {@link Record} by its ID.
 * <p>
 * The result is a single {@link Record} instance, or {@code null} if no record
 * with the given ID exists.
 * </p>
 *
 * @param <T> the {@link Record} type
 * @author Jeff Nelson
 */
public final class LoadRecordSelection<T extends Record> extends Selection<T> {

    /**
     * The record ID.
     */
    final long id;

    /**
     * Construct a new {@link LoadRecordSelection}.
     *
     * @param clazz the target class
     * @param id the record ID
     * @param any whether to include descendants
     */
    LoadRecordSelection(Class<T> clazz, long id, boolean any) {
        super(clazz, any);
        this.id = id;
    }

    /**
     * Constrain this {@link LoadRecordSelection} to the given {@code realms}.
     *
     * @param realms the {@link Realms} filter
     * @return this {@link LoadRecordSelection} for chaining
     * @throws IllegalStateException if this {@link LoadRecordSelection} is not
     *             {@link State#PENDING}
     */
    public LoadRecordSelection<T> realms(Realms realms) {
        ensurePending();
        this.realms = realms;
        return this;
    }

    @Override
    boolean isCombinable() {
        return true;
    }

}
