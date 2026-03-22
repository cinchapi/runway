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
 * A {@link Selections} provides positional access to the results of a
 * {@link DatabaseInterface#select(Selection...)} batch operation.
 * <p>
 * Results can be retrieved by index, corresponding to the order in which
 * {@link Selection Selections} were passed to
 * {@link DatabaseInterface#select(Selection...)}. The return type is unchecked
 * &mdash; the caller is responsible for casting to the appropriate type.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class Selections {

    /**
     * The executed {@link Selection Selections}.
     */
    private final Selection<?>[] selections;

    /**
     * The cursor position for {@link #next()}.
     */
    private int cursor = 0;

    /**
     * Construct a new {@link Selections} wrapping the given executed
     * {@link Selection} array.
     *
     * @param selections the executed selections
     */
    Selections(Selection<?>... selections) {
        this.selections = selections;
    }

    /**
     * Return the result of the {@link Selection} at the given {@code index}.
     * <p>
     * The return type is unchecked. For ID-based selections, this returns a
     * single {@link Record} (or {@code null}). For criteria-based or load-all
     * selections, this returns a {@link java.util.Set Set} of {@link Record
     * Records}.
     * </p>
     *
     * @param <T> the expected result type
     * @param index the zero-based index of the {@link Selection}
     * @return the result
     * @throws IndexOutOfBoundsException if the index is out of range
     * @throws IllegalStateException if the {@link Selection} has not finished
     *             execution
     */
    @SuppressWarnings("unchecked")
    public <T> T get(int index) {
        return (T) selections[index].get();
    }

    /**
     * Return the result of the next {@link Selection} in submission order.
     * <p>
     * Each call advances an internal cursor, so successive calls return
     * successive results. This enables idiomatic sequential access without
     * tracking indices.
     * </p>
     *
     * @param <T> the expected result type
     * @return the next result
     * @throws IndexOutOfBoundsException if all results have already been
     *             consumed
     */
    public <T> T next() {
        return get(cursor++);
    }

    /**
     * Return the number of {@link Selection Selections} in this batch.
     *
     * @return the selection count
     */
    public int size() {
        return selections.length;
    }

}
