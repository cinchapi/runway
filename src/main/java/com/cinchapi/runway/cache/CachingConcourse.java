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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import com.cinchapi.common.collect.Collections;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ForwardingConcourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.TransactionException;
import com.google.common.cache.Cache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;

/**
 * A {@link Concourse} wrapper that caches data {@link #select(Long) selected}
 * for faster subsequent lookups.
 * <p>
 * This class eagerly tries to cache an entire record's data whenever it is
 * selected from Concourse and will automatically invalidate the cache when it
 * detects a write for any cached record.
 * </p>
 * <p>
 * <strong>WARNING:</strong> This class assumes that external writes that would
 * cause the cache to become stale are handled externally. So, it is possible
 * for this class to return stale data if the provider isn't careful to ensure
 * that either 1) writes don't occur externally or 2) cache invalidation is
 * handled externally when appropriate.
 * </p>
 *
 * @author Jeff Nelson
 */
class CachingConcourse extends ForwardingConcourse {

    /**
     * The data cache.
     */
    private final LeasingCache<Long, Map<String, Set<Object>>> cache;

    /**
     * The current delegate. Points to {@link #cache} when {@link #stage()
     * staging} is not enabled. Otherwise points to a new
     * {@link NoOpLeasingCache}.
     */
    private LeasingCache<Long, Map<String, Set<Object>>> delegate;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     * @param cache
     */
    public CachingConcourse(Concourse concourse,
            Cache<Long, Map<String, Set<Object>>> cache) {
        super(concourse);
        this.cache = new LeasingCache<>(cache);
        this.delegate = this.cache;
    }

    @Override
    public void abort() {
        try {
            super.abort();
        }
        finally {
            delegate = this.cache;
        }
    }

    @Override
    public <T> Map<Long, Boolean> add(String key, T value,
            Collection<Long> records) {
        try {
            return super.add(key, value, records);
        }
        finally {
            for (long record : records) {
                delegate.invalidate(record);
            }
        }
    }

    @Override
    public <T> boolean add(String key, T value, long record) {
        if(super.add(key, value, record)) {
            try {
                return true;
            }
            finally {
                delegate.invalidate(record);
            }
        }
        else {
            return false;
        }
    }

    @Override
    public void clear(long record) {
        try {
            super.clear(record);
        }
        finally {
            delegate.invalidate(record);
        }
    }

    @Override
    public void clear(String key, Collection<Long> records) {
        try {
            super.clear(key, records);
        }
        finally {
            for (long record : records) {
                delegate.invalidate(record);
            }
        }
    }

    @Override
    public void clear(String key, long record) {
        try {
            super.clear(key, record);
        }
        finally {
            delegate.invalidate(record);
        }
    }

    @Override
    public boolean commit() {
        try {
            if(super.commit()) {
                // NOTE: The use of #cache instead of #delegate is intentional
                Map<Long, Long> leases = cache.lease(delegate.leased());
                for (Entry<Long, Long> entry : leases.entrySet()) {
                    long record = entry.getKey();
                    long lease = entry.getValue();
                    cache.invalidate(lease, record);
                }
                return true;
            }
            else {
                return false;
            }
        }
        finally {
            delegate = cache;
        }
    }

    @Override
    public Set<Long> insert(String json) {
        Set<Long> records = super.insert(json);
        try {
            return records;
        }
        finally {
            for (long record : records) {
                delegate.invalidate(record);
            }
        }
    }

    @Override
    public boolean insert(String json, long record) {
        if(super.insert(json, record)) {
            try {
                return true;
            }
            finally {
                delegate.invalidate(record);
            }
        }
        else {
            return false;
        }
    }

    @Override
    public <T> void reconcile(String key, long record, Collection<T> values) {
        try {
            super.reconcile(key, record, values);
        }
        finally {
            delegate.invalidate(record);
        }
    }

    @Override
    public <T> Map<Long, Boolean> remove(String key, T value,
            Collection<Long> records) {
        try {
            return super.remove(key, value, records);
        }
        finally {
            for (long record : records) {
                delegate.invalidate(record);
            }
        }
    }

    @Override
    public <T> boolean remove(String key, T value, long record) {
        if(super.remove(key, value, record)) {
            try {
                return true;
            }
            finally {
                delegate.invalidate(record);
            }
        }
        else {
            return false;
        }
    }

    @Override
    public void revert(Collection<String> keys, Collection<Long> records,
            Timestamp timestamp) {
        try {
            super.revert(keys, records, timestamp);
        }
        finally {
            for (long record : records) {
                delegate.invalidate(record);
            }
        }
    }

    @Override
    public void revert(Collection<String> keys, long record,
            Timestamp timestamp) {
        try {
            super.revert(keys, record, timestamp);
        }
        finally {
            delegate.invalidate(record);
        }
    }

    @Override
    public void revert(String key, Collection<Long> records,
            Timestamp timestamp) {
        try {
            super.revert(key, records, timestamp);
        }
        finally {
            for (long record : records) {
                delegate.invalidate(record);
            }
        }
    }

    @Override
    public void revert(String key, long record, Timestamp timestamp) {
        try {
            super.revert(key, record, timestamp);
        }
        finally {
            delegate.invalidate(record);
        }
    }

    @Override
    public <T> Map<Long, Map<String, Set<T>>> select(Collection<Long> records) {
        ConcurrentMap<Long, Map<String, Set<Object>>> view = delegate.asMap();
        Collection<Long> $records = Collections2.filter(records,
                record -> !view.containsKey(record));
        Map<Long, Long> leases = delegate.lease($records);
        Map<Long, Map<String, Set<T>>> data = $records.isEmpty()
                ? ImmutableMap.of()
                : super.select($records);
        return new AbstractMap<Long, Map<String, Set<T>>>() {

            Set<Long> keys = Collections.ensureSet(records);

            @SuppressWarnings("unchecked")
            @Override
            public Map<String, Set<T>> get(Object key) {
                if(keys.contains(key)) {
                    Long record = (Long) key;
                    Long lease = leases.get(record);
                    Map<String, Set<T>> stored;
                    if(lease != null) {
                        // The record was not previously cached, so pull it from
                        // the #data that was fetched and try to populate the
                        // cache using the reserved ticket. If the record, did
                        // not exist, but has subsequently been created, use
                        // #select(record) to get the most up-to-date snapshot
                        stored = data.get(record);
                        if(stored == null) {
                            stored = select(record);
                        }
                        delegate.put(lease, record, Map.class.cast(stored));
                    }
                    else {
                        // The item was previously cached, so rely on the
                        // #select(record) implementation to capture if. If, for
                        // some reason, the cache was intermittently
                        // invalidated, the #select(record) call will reload it
                        stored = select(record);
                    }
                    return stored;
                }
                else {
                    return null;
                }
            }

            @Override
            public Set<Entry<Long, Map<String, Set<T>>>> entrySet() {
                return LazyTransformSet.of(keys,
                        record -> new SimpleImmutableEntry<>(record,
                                get(record)));
            }

            @Override
            public int size() {
                return keys.size();
            }

        };

    }

    @Override
    public Map<String, Set<Object>> select(long record) {
        try {
            return delegate.get(record, () -> super.select(record));
        }
        catch (ExecutionException e) {
            return super.select(record);
        }
    }

    @Override
    public void set(String key, Object value, Collection<Long> records) {
        try {
            super.set(key, value, records);
        }
        finally {
            for (long record : records) {
                delegate.invalidate(record);
            }
        }
    }

    @Override
    public <T> void set(String key, T value, long record) {
        try {
            super.set(key, value, record);
        }
        finally {
            delegate.invalidate(record);
        }
    }

    @Override
    public void stage() throws TransactionException {
        try {
            super.stage();
        }
        finally {
            delegate = new NoOpLeasingCache<>();
        }
    }

    @Override
    public boolean verifyAndSwap(String key, Object expected, long record,
            Object replacement) {
        if(super.verifyAndSwap(key, expected, record, replacement)) {
            try {
                return true;
            }
            finally {
                delegate.invalidate(record);
            }
        }
        else {
            return false;
        }
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        try {
            super.verifyOrSet(key, value, record);
        }
        finally {
            delegate.invalidate(record);
        }
    }

    @Override
    protected ForwardingConcourse $this(Concourse concourse) {
        return new CachingConcourse(concourse, cache);
    }

}
