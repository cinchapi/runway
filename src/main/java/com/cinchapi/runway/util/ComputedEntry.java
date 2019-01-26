/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway.util;

import java.util.Map.Entry;
import java.util.function.Supplier;

/**
 * An {@link Entry} whose value is computed with a {@link Supplier}.
 *
 * @author Jeff Nelson
 */
public class ComputedEntry<K, V> implements Entry<K, V> {

    /**
     * The key.
     */
    private final K key;

    /**
     * The value computer.
     */
    private final Supplier<V> valueSupplier;

    /**
     * Construct a new instance.
     * 
     * @param entry
     */
    public ComputedEntry(Entry<K, Supplier<V>> entry) {
        this(entry.getKey(), entry.getValue());
    }

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param valueSupplier
     */
    public ComputedEntry(K key, Supplier<V> valueSupplier) {
        this.key = key;
        this.valueSupplier = valueSupplier;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return valueSupplier.get();
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

}
