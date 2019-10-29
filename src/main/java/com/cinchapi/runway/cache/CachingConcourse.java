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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.DelegatingConcourse;
import com.cinchapi.concourse.Timestamp;
import com.google.common.cache.Cache;

/**
 * A {@link Concourse} wrapper that caches data {@link #select(Long) selected}
 * for faster
 * subsequent lookups.
 * <p>
 * This class will automatically invalidate the cache when it detects a write
 * for any cached record.
 * </p>
 *
 * @author Jeff Nelson
 */
class CachingConcourse extends DelegatingConcourse {

    /**
     * The cache.
     */
    private final Cache<Long, Map<String, Set<Object>>> cache;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     * @param cache
     */
    public CachingConcourse(Concourse concourse,
            Cache<Long, Map<String, Set<Object>>> cache) {
        super(concourse);
        this.cache = cache;
    }

    @Override
    public <T> Map<Long, Boolean> add(String key, T value,
            Collection<Long> records) {
        try {
            return super.add(key, value, records);
        }
        finally {
            for (long record : records) {
                cache.invalidate(record);
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
                cache.invalidate(record);
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
            cache.invalidate(record);
        }
    }

    @Override
    public void clear(String key, Collection<Long> records) {
        try {
            super.clear(key, records);
        }
        finally {
            for (long record : records) {
                cache.invalidate(record);
            }
        }
    }

    @Override
    public void clear(String key, long record) {
        try {
            super.clear(key, record);
        }
        finally {
            cache.invalidate(record);
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
                cache.invalidate(record);
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
                cache.invalidate(record);
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
            cache.invalidate(record);
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
                cache.invalidate(record);
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
                cache.invalidate(record);
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
                cache.invalidate(record);
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
            cache.invalidate(record);
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
                cache.invalidate(record);
            }
        }
    }

    @Override
    public void revert(String key, long record, Timestamp timestamp) {
        try {
            super.revert(key, record, timestamp);
        }
        finally {
            cache.invalidate(record);
        }
    }

    @Override
    public Map<String, Set<Object>> select(long record) {
        try {
            return cache.get(record, () -> super.select(record));
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
                cache.invalidate(record);
            }
        }
    }

    @Override
    public <T> void set(String key, T value, long record) {
        try {
            super.set(key, value, record);
        }
        finally {
            cache.invalidate(record);
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
                cache.invalidate(record);
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
            cache.invalidate(record);
        }
    }

}
