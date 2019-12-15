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
package com.cinchapi.runway.cache;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.cinchapi.common.collect.Collections;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.google.common.cache.Cache;
import com.google.common.cache.ForwardingCache;
import com.google.common.collect.Maps;

/**
 * A look-aside/demand-fill {@link Cache} that uses leases to guarantee cache
 * consistency.
 *
 * @author Jeff Nelson
 */
class LeasingCache<K, V> extends ForwardingCache<K, V> {

    /**
     * The actual cache.
     */
    private final Cache<K, V> delegate;

    /**
     * A mapping from each key to its current lease. Leases are created
     * on-demand in the {@link #lessor(Object)} method.
     */
    private final Map<K, AtomicLong> leases;

    /**
     * Construct a new instance.
     * 
     * @param cache
     */
    protected LeasingCache(Cache<K, V> cache) {
        this.delegate = cache;
        this.leases = Maps.newHashMap();
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader)
            throws ExecutionException {
        return get(lease(key), key, valueLoader);
    }

    /**
     * Execute the {@link #get(Object, Callable)} method with a consistency
     * guarantee provided by the {@code lease}.
     * <p>
     * In essence, this method keeps re-trying if lease validation fails because
     * the cache for {@code key} is updated during retrieval or load.
     * </p>
     * 
     * @param lease
     * @param key
     * @param valueLoader
     * @return the retrieved or loaded value
     * @throws ExecutionException
     */
    public V get(long lease, K key, Callable<? extends V> valueLoader)
            throws ExecutionException {
        V value = getIfPresent(key);
        if(value == null) {
            try {
                value = valueLoader.call();
            }
            catch (Exception e) {
                throw new ExecutionException(e.getMessage(), e);
            }
        }
        AtomicLong lessor = lessor(key);
        long newLease = System.currentTimeMillis();
        if(lessor.compareAndSet(lease, newLease) && put(newLease, key, value)) {
            return value;
        }
        else {
            return get(key, valueLoader);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void invalidate(Object key) {
        try {
            invalidate(lease((K) key), key);
        }
        catch (ClassCastException e) {/* ignore */}
    }

    /**
     * Execute the {@link #invalidate(Object)} method with a consistency
     * guarantee provided by the {@code lease}.
     * <p>
     * If the cache cannot be updated because lease validation fails (implying
     * that the cache was updated since the lease was acquired) this method
     * returns {@code false}.
     * </p>
     * 
     * @param lease
     * @param key
     * @return
     */
    @SuppressWarnings("unchecked")
    public boolean invalidate(long lease, Object key) {
        try {
            AtomicLong lessor = lessor((K) key);
            if(lessor.compareAndSet(lease, System.currentTimeMillis())) {
                super.invalidate(key);
                return true;
            }
        }
        catch (ClassCastException e) {/* ignore */}
        return false;

    }

    /**
     * Return a mapping from each of the {@code keys} to its current lease
     * value.
     * 
     * @param keys
     * @return the current lease values
     */
    public Map<K, Long> lease(Collection<K> keys) {
        return new AbstractMap<K, Long>() {

            @Override
            public Set<Entry<K, Long>> entrySet() {
                return LazyTransformSet.of(Collections.ensureSet(keys),
                        key -> new AbstractMap.SimpleImmutableEntry<>(key,
                                lease(key)));
            }

        };
    }

    /**
     * Return the current lease value for {@code key}.
     * 
     * @param key
     * @return the current lease value
     */
    public long lease(K key) {
        AtomicLong lessor = lessor(key);
        return lessor.get();
    }

    /**
     * Return the cache keys for which leases have been requested.
     * 
     * @return the cache keys for which leases have been requested
     */
    public Set<K> leased() {
        return leases.keySet();
    }

    @Override
    public void put(K key, V value) {
        put(lease(key), key, value);
    }

    /**
     * Execute the {@link #put(Object, Object)} method with a consistency
     * guarantee provided by the {@code lease}.
     * <p>
     * If the cache cannot be updated because lease validation fails (implying
     * that the cache was updated since the lease was acquired) this method
     * returns {@code false}.
     * </p>
     * 
     * @param lease
     * @param key
     * @param value
     * @return a boolean that indicates if the cache was updated
     */
    public boolean put(long lease, K key, V value) {
        AtomicLong lessor = lessor(key);
        if(lessor.compareAndSet(lease, System.currentTimeMillis())) {
            super.put(key, value);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    protected Cache<K, V> delegate() {
        return delegate;
    }

    /**
     * Get the lease counter from {@link #leases} or dynamically create a new
     * one if it doesn't exist.
     * 
     * @param key
     * @return the lease counter
     */
    private AtomicLong lessor(K key) {
        return leases.computeIfAbsent(key, ignore -> new AtomicLong(0));
    }

}
