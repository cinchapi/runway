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

/**
 * The outcome of a {@code $select*} method, pairing the caller-facing result
 * with an optional cache-safe value.
 * <p>
 * When a client-side filter is applied, {@link #result} contains the filtered
 * data and {@link #cacheValue} holds the pre-filter data that is safe to store
 * in the reservation cache. When no filter is applied, {@link #cacheValue} is
 * {@code null} and {@link #result} is already unfiltered.
 *
 * @param <R> the result type
 * @author Jeff Nelson
 */
final class SelectResult<R> {

    /**
     * The result to return to the caller, which may have been filtered.
     */
    final R result;

    /**
     * The unfiltered result that is safe to cache, or {@code null} if
     * {@link #result} is already unfiltered.
     */
    @Nullable
    final Object cacheValue;

    /**
     * Construct a new {@link SelectResult} with both a filtered result and a
     * separate cache-safe value.
     *
     * @param result the filtered result
     * @param cacheValue the unfiltered result for caching
     */
    SelectResult(R result, @Nullable Object cacheValue) {
        this.result = result;
        this.cacheValue = cacheValue;
    }

    /**
     * Construct a new {@link SelectResult} whose result is unfiltered and
     * therefore safe to cache directly.
     *
     * @param result the unfiltered result
     */
    SelectResult(R result) {
        this(result, null);
    }

}
