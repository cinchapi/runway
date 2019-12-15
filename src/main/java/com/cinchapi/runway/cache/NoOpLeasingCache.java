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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A {@link LeasingCache} that is backed by a {@link NoOpCache}.
 *
 * @author Jeff Nelson
 */
class NoOpLeasingCache<K, V> extends LeasingCache<K, V> {

    /**
     * Construct a new instance.
     * 
     * @param cache
     */
    protected NoOpLeasingCache() {
        super(new NoOpCache<>());
    }

    @Override
    public V get(long lease, K key, Callable<? extends V> valueLoader)
            throws ExecutionException {
        try {
            return valueLoader.call();
        }
        catch (Exception e) {
            throw new ExecutionException(e.getMessage(), e);
        }
    }

    @Override
    public boolean invalidate(long lease, Object key) {
        return false;
    }
    
    @Override
    public boolean put(long lease, K key, V value) {
        return false;
    }

}
