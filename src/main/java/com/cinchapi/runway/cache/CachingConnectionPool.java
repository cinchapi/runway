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

import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.util.ConcurrentLoadingQueue;
import com.google.common.cache.Cache;

/**
 * A {@link ConnectionPool} with supports for data {@link CachingConcourse
 * caching}.
 *
 * @author Jeff Nelson
 */
public class CachingConnectionPool extends ConnectionPool {

    /**
     * Construct a new instance.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @param poolSize
     */
    public CachingConnectionPool(String host, int port, String username,
            String password, String environment,
            Cache<Long, Map<String, Set<Object>>> cache) {
        super(() -> new CachingConcourse(
                Concourse.connect(host, port, username, password, environment),
                cache), ConnectionPool.DEFAULT_POOL_SIZE);
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return ConcurrentLoadingQueue.create(supplier::get);
    }

    @Override
    protected Concourse getConnection() {
        return available.poll();
    }

}
