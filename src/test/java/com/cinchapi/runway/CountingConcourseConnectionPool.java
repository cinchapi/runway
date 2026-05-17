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
package com.cinchapi.runway;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;

/**
 * A {@link ConnectionPool} whose connections are {@link CountingConcourse}
 * instances, so a test can assert the read-RPC count of a Runway operation by
 * swapping this pool onto a {@link Runway}.
 *
 * @author Jeff Nelson
 */
final class CountingConcourseConnectionPool extends ConnectionPool {

    /**
     * Construct a new instance.
     *
     * @param supplier the source of {@link CountingConcourse} connections
     */
    CountingConcourseConnectionPool(Supplier<Concourse> supplier) {
        super(supplier, 1);
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return new ConcurrentLinkedQueue<>();
    }

    @Override
    protected Concourse getConnection() {
        return supplier.get();
    }

}
