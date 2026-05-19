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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Reader} that issues each read against the wrapped {@link Concourse}
 * at recording time.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class IncrementalReader extends AbstractReader {

    /**
     * Construct an {@link IncrementalReader} that borrows a {@link Concourse}
     * connection from {@code pool} and returns it to {@code pool} on
     * {@link #close()}.
     *
     * @param pool the {@link ConnectionPool} that owns the {@link Concourse}
     *            connection; must not be {@code null}
     */
    public IncrementalReader(ConnectionPool pool) {
        super(pool);
    }

    /**
     * Construct an {@link IncrementalReader} that issues against
     * {@code connection}. The caller retains ownership of the connection
     * lifecycle; {@link #close()} does <strong>not</strong> close it.
     *
     * @param connection the {@link Concourse} connection against which reads
     *            are issued; must not be {@code null}
     */
    public IncrementalReader(Concourse connection) {
        super(connection);
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria) {
        return Pending.of(concourse().select(criteria));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria) {
        return Pending.of(concourse().select(keys, criteria));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Order order) {
        return Pending.of(concourse().select(criteria, order));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Order order) {
        return Pending.of(concourse().select(keys, criteria, order));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Page page) {
        return Pending.of(concourse().select(criteria, page));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Page page) {
        return Pending.of(concourse().select(keys, criteria, page));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Order order, Page page) {
        return Pending.of(concourse().select(criteria, order, page));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(Set<String> keys,
            Criteria criteria, Order order, Page page) {
        return Pending.of(concourse().select(keys, criteria, order, page));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> select(
            Collection<Long> records) {
        return Pending.of(concourse().select(records));
    }

    @Override
    public Pending<Map<String, Set<Object>>> select(long record) {
        return Pending.of(concourse().select(record));
    }

    @Override
    public Pending<Map<String, Set<Object>>> select(Set<String> keys,
            long record) {
        return Pending.of(concourse().select(keys, record));
    }

    @Override
    public Pending<Set<Object>> select(String key, long record) {
        return Pending.of(concourse().select(key, record));
    }

    @Override
    public Pending<Object> get(String key, long record) {
        return Pending.of(concourse().get(key, record));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> navigate(
            Set<String> keys, long record) {
        return Pending.of(concourse().navigate(keys, record));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> navigate(
            Set<String> keys, Criteria criteria) {
        return Pending.of(concourse().navigate(keys, criteria));
    }

    @Override
    public Pending<Map<Long, Map<String, Set<Object>>>> navigate(
            Set<String> keys, Collection<Long> records) {
        return Pending.of(concourse().navigate(keys, records));
    }

    @Override
    public Pending<Set<Long>> find(Criteria criteria) {
        return Pending.of(concourse().find(criteria));
    }

    @Override
    public Pending<Long> count(String key, Criteria criteria) {
        return Pending.of(concourse().calculate().count(key, criteria));
    }

    @Override
    public Pending<Long> time() {
        return Pending.of(concourse().time().getMicros());
    }

    @Override
    protected void prepareDrain() {/* no-op */}

}
