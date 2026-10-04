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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link Map} whose values are stored, derived, or computed, and produced
 * only when a caller reads them.
 * <p>
 * {@link #put(Object, Object)} stores a value.
 * {@link #derive(Object, Supplier)} registers a value that is produced on its
 * first read and returned by every later read.
 * {@link #compute(Object, Supplier)} registers a value that is produced anew on
 * every read. A later registration of a key replaces the earlier one, whatever
 * their kinds.
 * </p>
 * <p>
 * Listing the entries produces no value. Each entry of a derived key is a
 * {@link DerivedEntry}, and each entry of a computed key is a
 * {@link ComputedEntry}, so a caller can tell the kinds apart and skip an entry
 * before reading its value.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class OnDemandMap<K, V> extends AbstractMap<K, V> {

    /**
     * The entry for each key.
     */
    private final Map<K, Entry<K, V>> entries = new HashMap<>();

    /**
     * The read-only view of {@link #entries} that {@link #entrySet()} returns.
     */
    private final Set<Entry<K, V>> view = new AbstractSet<Entry<K, V>>() {

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return Collections.unmodifiableCollection(entries.values())
                    .iterator();
        }

        @Override
        public int size() {
            return entries.size();
        }

    };

    /**
     * Register {@code key} so that its value is produced by {@code supplier} on
     * the first read that succeeds, and returned by every later read.
     *
     * @param key the key
     * @param supplier the {@link Supplier} of the value
     */
    public void derive(K key, Supplier<V> supplier) {
        entries.put(key, new DerivedEntry<>(key, supplier));
    }

    /**
     * Register {@code key} so that its value is produced anew by
     * {@code supplier} on every read.
     *
     * @param key the key
     * @param supplier the {@link Supplier} of the value
     */
    public void compute(K key, Supplier<V> supplier) {
        entries.put(key, new ComputedEntry<>(key, supplier));
    }

    /**
     * {@inheritDoc}
     * <p>
     * The result is the previous stored value. It is {@code null} when
     * {@code key} held no value or held a derived or computed value, which this
     * method does not produce.
     * </p>
     */
    @Override
    @Nullable
    public V put(K key, V value) {
        Entry<K, V> previous = entries.put(key,
                new SimpleImmutableEntry<>(key, value));
        return previous instanceof SimpleImmutableEntry ? previous.getValue()
                : null;
    }

    @Override
    @Nullable
    public V get(Object key) {
        Entry<K, V> entry = entries.get(key);
        return entry != null ? entry.getValue() : null;
    }

    @Override
    public boolean containsKey(Object key) {
        return entries.containsKey(key);
    }

    @Override
    public int size() {
        return entries.size();
    }

    /**
     * {@inheritDoc}
     * <p>
     * The result is an unmodifiable view, which reflects each later
     * registration of a key.
     * </p>
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return view;
    }

    /**
     * The entry of a key that {@link OnDemandMap#compute(Object, Supplier)}
     * registered. Each read of its value produces the value anew.
     */
    public static final class ComputedEntry<K, V> implements Entry<K, V> {

        /**
         * The key.
         */
        private final K key;

        /**
         * The {@link Supplier} that produces the value on each read.
         */
        private final Supplier<V> supplier;

        /**
         * Construct a new instance.
         *
         * @param key the key
         * @param supplier the {@link Supplier} that produces the value on each
         *            read
         */
        private ComputedEntry(K key, Supplier<V> supplier) {
            this.key = key;
            this.supplier = supplier;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return supplier.get();
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * The entry of a key that {@link OnDemandMap#derive(Object, Supplier)}
     * registered. The first read that succeeds produces the value, and every
     * later read returns it.
     * <p>
     * A read whose {@link Supplier} throws leaves no value, so the next read
     * runs the {@link Supplier} again. A read made while the {@link Supplier}
     * runs, such as one the {@link Supplier} makes itself, returns
     * {@code null}.
     * </p>
     */
    public static final class DerivedEntry<K, V> implements Entry<K, V> {

        /**
         * The key.
         */
        private final K key;

        /**
         * The {@link Supplier} that produces the value.
         */
        private final Supplier<V> supplier;

        /**
         * The value that {@link #supplier} produced, once {@link #isDerived} is
         * {@code true}.
         */
        @Nullable
        private V value = null;

        /**
         * Whether {@link #value} holds the value that {@link #supplier}
         * produced.
         */
        private boolean isDerived = false;

        /**
         * Whether {@link #supplier} is running.
         */
        private boolean isDeriving = false;

        /**
         * Construct a new instance.
         *
         * @param key the key
         * @param supplier the {@link Supplier} that produces the value
         */
        private DerivedEntry(K key, Supplier<V> supplier) {
            this.key = key;
            this.supplier = supplier;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        @Nullable
        public V getValue() {
            if(isDerived) {
                return value;
            }
            else if(isDeriving) {
                return null;
            }
            else {
                isDeriving = true;
                try {
                    value = supplier.get();
                    isDerived = true;
                    return value;
                }
                finally {
                    isDeriving = false;
                }
            }
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }

    }

}
