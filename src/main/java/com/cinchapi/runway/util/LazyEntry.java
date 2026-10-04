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
package com.cinchapi.runway.util;

import java.util.Map.Entry;
import java.util.function.Supplier;

/**
 * An {@link Entry} whose value comes from a {@link Supplier} each time it is
 * read, so a caller that skips the entry by its key never produces its value.
 *
 * @author Jeff Nelson
 */
public class LazyEntry<K, V> implements Entry<K, V> {

    /**
     * The key.
     */
    private final K key;

    /**
     * The {@link Supplier} of the value.
     */
    private final Supplier<V> value;

    /**
     * Construct a new instance.
     *
     * @param key the key
     * @param value the {@link Supplier} that produces the value on each read
     */
    public LazyEntry(K key, Supplier<V> value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value.get();
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

}
