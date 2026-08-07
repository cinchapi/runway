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
package com.cinchapi.runway.db;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;

/**
 * A {@link ConcourseProvider} that draws each connection from a backing
 * {@link ConnectionPool} and owns the {@link ConnectionPool ConnectionPool's}
 * lifecycle.
 *
 * @author Jeff Nelson
 */
final class PooledConcourseProvider implements ConcourseProvider {

    /**
     * The backing {@link ConnectionPool}.
     */
    private final ConnectionPool connections;

    /**
     * Construct a new instance.
     *
     * @param connections the backing {@link ConnectionPool}
     */
    PooledConcourseProvider(ConnectionPool connections) {
        this.connections = connections;
    }

    @Override
    public Concourse request() {
        return connections.request();
    }

    @Override
    public void release(Concourse concourse) {
        connections.release(concourse);
    }

    @Override
    public void close() throws Exception {
        if(!connections.isClosed()) {
            connections.close();
        }
    }

}
