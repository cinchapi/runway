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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.ForwardingConcourse;
import com.cinchapi.concourse.lang.CommandGroup;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link ConnectionPool} whose connections are {@link CountingConcourse}
 * instances, so a test can assert the read-RPC count of a Runway operation by
 * swapping this pool onto a {@link Runway}.
 *
 * @author Jeff Nelson
 */
final class CountingConcourseConnectionPool extends ConnectionPool {

    /**
     * Construct a new instance whose pooled {@link CountingConcourse}
     * connections each forward to a copy of {@code concourse}.
     *
     * @param concourse the {@link Concourse} whose connection is copied
     */
    CountingConcourseConnectionPool(Concourse concourse) {
        this(concourse, new AtomicInteger());
    }

    /**
     * Construct a new instance.
     *
     * @param concourse the {@link Concourse} whose connection is copied
     * @param rpcs the read-RPC tally shared by all pooled connections
     */
    private CountingConcourseConnectionPool(Concourse concourse,
            AtomicInteger rpcs) {
        super(() -> new CountingConcourse(
                Concourse.copyExistingConnection(concourse), rpcs), 1);
    }

    @Override
    protected Queue<Concourse> buildQueue(int size) {
        return new ConcurrentLinkedQueue<>();
    }

    @Override
    protected Concourse getConnection() {
        return supplier.get();
    }

    /**
     * A {@link ForwardingConcourse} that tallies the read RPCs issued against
     * it &mdash; the {@code find}, {@code get}, {@code navigate},
     * {@code search}, {@code select}, and batch {@code submit} forms that
     * Runway uses &mdash; so a test can assert how many round trips an
     * operation takes.
     *
     * @author Jeff Nelson
     */
    static final class CountingConcourse extends ForwardingConcourse {

        /**
         * The shared read-RPC tally.
         */
        private final AtomicInteger rpcs;

        /**
         * Construct a new instance.
         *
         * @param concourse the delegate {@link Concourse}
         * @param rpcs the shared read-RPC tally
         */
        CountingConcourse(Concourse concourse, AtomicInteger rpcs) {
            super(concourse);
            this.rpcs = rpcs;
        }

        /**
         * Return the read-RPC tally shared by every {@link CountingConcourse}
         * that a single {@link CountingConcourseConnectionPool} produces.
         *
         * @return the shared read-RPC tally
         */
        AtomicInteger rpcs() {
            return rpcs;
        }

        @Override
        protected ForwardingConcourse $this(Concourse concourse) {
            return new CountingConcourse(concourse, rpcs);
        }

        @Override
        public Set<Long> find(Criteria criteria) {
            rpcs.incrementAndGet();
            return super.find(criteria);
        }

        @Override
        public Set<Long> find(Criteria criteria, Order order) {
            rpcs.incrementAndGet();
            return super.find(criteria, order);
        }

        @Override
        public Set<Long> find(Criteria criteria, Order order, Page page) {
            rpcs.incrementAndGet();
            return super.find(criteria, order, page);
        }

        @Override
        public Set<Long> find(Criteria criteria, Page page) {
            rpcs.incrementAndGet();
            return super.find(criteria, page);
        }

        @Override
        public <T> T get(String key, long record) {
            rpcs.incrementAndGet();
            return super.get(key, record);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, Collection<Long> records) {
            rpcs.incrementAndGet();
            return super.navigate(keys, records);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, long record) {
            rpcs.incrementAndGet();
            return super.navigate(keys, record);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> navigate(
                Collection<String> keys, Criteria criteria) {
            rpcs.incrementAndGet();
            return super.navigate(keys, criteria);
        }

        @Override
        public Set<Long> search(String key, String query) {
            rpcs.incrementAndGet();
            return super.search(key, query);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<Long> records) {
            rpcs.incrementAndGet();
            return super.select(records);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria) {
            rpcs.incrementAndGet();
            return super.select(keys, criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Order order) {
            rpcs.incrementAndGet();
            return super.select(keys, criteria, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Order order,
                Page page) {
            rpcs.incrementAndGet();
            return super.select(keys, criteria, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(
                Collection<String> keys, Criteria criteria, Page page) {
            rpcs.incrementAndGet();
            return super.select(keys, criteria, page);
        }

        @Override
        public <T> Map<String, Set<T>> select(Collection<String> keys,
                long record) {
            rpcs.incrementAndGet();
            return super.select(keys, record);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria) {
            rpcs.incrementAndGet();
            return super.select(criteria);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Order order) {
            rpcs.incrementAndGet();
            return super.select(criteria, order);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Order order, Page page) {
            rpcs.incrementAndGet();
            return super.select(criteria, order, page);
        }

        @Override
        public <T> Map<Long, Map<String, Set<T>>> select(Criteria criteria,
                Page page) {
            rpcs.incrementAndGet();
            return super.select(criteria, page);
        }

        @Override
        public <T> Map<String, Set<T>> select(long record) {
            rpcs.incrementAndGet();
            return super.select(record);
        }

        @Override
        public <T> Set<T> select(String key, long record) {
            rpcs.incrementAndGet();
            return super.select(key, record);
        }

        @Override
        public List<Object> submit(CommandGroup group) {
            rpcs.incrementAndGet();
            return super.submit(group);
        }

    }

}
