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

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Concourse;
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
     * Construct a new {@link IncrementalReader}.
     *
     * @param concourse the {@link Concourse} connection against which reads are
     *            issued; must not be {@code null}
     */
    public IncrementalReader(Concourse concourse) {
        super(concourse);
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(criteria);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(keys,
                criteria);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Order order) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(criteria,
                order);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria, Order order) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(keys,
                criteria, order);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Page page) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(criteria,
                page);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria, Page page) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(keys,
                criteria, page);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Criteria criteria, Order order, Page page) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(criteria,
                order, page);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> select(
            Set<String> keys, Criteria criteria, Order order, Page page) {
        Map<Long, Map<String, Set<Object>>> result = concourse.select(keys,
                criteria, order, page);
        return () -> result;
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria) {
        Set<Long> result = concourse.find(criteria);
        return () -> result;
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria, Order order) {
        Set<Long> result = concourse.find(criteria, order);
        return () -> result;
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria, Page page) {
        Set<Long> result = concourse.find(criteria, page);
        return () -> result;
    }

    @Override
    public Supplier<Set<Long>> find(Criteria criteria, Order order, Page page) {
        Set<Long> result = concourse.find(criteria, order, page);
        return () -> result;
    }

    @Override
    public Supplier<Map<String, Set<Object>>> select(long record) {
        Map<String, Set<Object>> result = concourse.select(record);
        return () -> result;
    }

    @Override
    public Supplier<Map<String, Set<Object>>> select(Set<String> keys,
            long record) {
        Map<String, Set<Object>> result = concourse.select(keys, record);
        return () -> result;
    }

    @Override
    public Supplier<Set<Object>> select(String key, long record) {
        Set<Object> result = concourse.select(key, record);
        return () -> result;
    }

    @Override
    public Supplier<Object> get(String key, long record) {
        Object result = concourse.get(key, record);
        return () -> result;
    }

    @Override
    public Supplier<Map<Long, Map<String, Set<Object>>>> navigate(
            Set<String> keys, long record) {
        Map<Long, Map<String, Set<Object>>> result = concourse.navigate(keys,
                record);
        return () -> result;
    }

    @Override
    public Supplier<Long> count(String key, Criteria criteria) {
        long result = concourse.calculate().count(key, criteria);
        return () -> result;
    }

    @Override
    protected void prepareDrain() {/* no-op */}

}
