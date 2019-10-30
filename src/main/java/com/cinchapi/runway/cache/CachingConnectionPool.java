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
import java.util.concurrent.Callable;

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

    // Connection Info
    private final String host;
    private final int port;
    private String username;
    private final String password;
    private final String environment;
    private final Cache<Long, Map<String, Set<Object>>> cache;

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
        super(host, port, username, password, environment, ConnectionPool.DEFAULT_POOL_SIZE);
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.environment = environment;
        this.cache = cache;
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return ConcurrentLoadingQueue.create(new Callable<Concourse>() {

            @Override
            public Concourse call() throws Exception {
                return new CachingConcourse(Concourse.connect(host, port,
                        username, password, environment), cache);
            }

        });
    }

    @Override
    protected Concourse getConnection() {
        return available.poll();
    }

}
