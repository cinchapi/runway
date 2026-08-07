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
 * A {@link ConcourseProvider} supplies the {@link Concourse} connection for
 * each database operation without revealing where the connection comes from.
 * <p>
 * Every {@link #request()} must be paired with a {@link #release(Concourse)} of
 * the returned connection, typically in a {@code finally} block. A provider may
 * treat {@link #release(Concourse)} as a no-op when it retains ownership of the
 * connection.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface ConcourseProvider {

    /**
     * Return a {@link ConcourseProvider} that draws from {@code connections}.
     *
     * @param connections the backing {@link ConnectionPool}
     * @return the pooled {@link ConcourseProvider}
     */
    static ConcourseProvider from(ConnectionPool connections) {
        return new PooledConcourseProvider(connections);
    }

    /**
     * Return a {@link Concourse} connection for a database operation.
     *
     * @return the connection
     */
    Concourse request();

    /**
     * Return a previously {@link #request() requested} {@code concourse}
     * connection.
     *
     * @param concourse the connection to return
     */
    void release(Concourse concourse);

    /**
     * Release every resource that this {@link ConcourseProvider} owns.
     * <p>
     * A provider that does not own its connection source does nothing.
     * </p>
     *
     * @throws Exception if the provider cannot close cleanly
     */
    default void close() throws Exception {}

}
