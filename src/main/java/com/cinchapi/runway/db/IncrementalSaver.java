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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.google.common.base.Preconditions;

/**
 * A {@link Saver} that executes every recording call synchronously against the
 * wrapped {@link Concourse} connection.
 * <p>
 * Each {@code audit}/{@code find} round-trips immediately and invokes the
 * supplied {@link Consumer validator} inline so a validation failure throws
 * before any subsequent write is recorded. Each write call goes straight to the
 * connection. {@link #commit()} and {@link #abort()} delegate directly to the
 * underlying connection's transaction primitives.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public final class IncrementalSaver implements Saver {

    /**
     * The {@link Concourse} connection that every recording call targets.
     */
    private final Concourse concourse;

    /**
     * Construct a new {@link IncrementalSaver} backed by {@code concourse}.
     *
     * @param concourse the {@link Concourse} connection that every recording
     *            call targets; must not be {@code null}
     */
    public IncrementalSaver(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
    }

    @Override
    public void stage() {
        concourse.stage();
    }

    @Override
    public boolean commit() {
        return concourse.commit();
    }

    @Override
    public void abort() {
        concourse.abort();
    }

    @Override
    public void audit(long record,
            Consumer<Map<Timestamp, List<String>>> validator) {
        validator.accept(concourse.audit(record));
    }

    @Override
    public void find(Criteria criteria, Consumer<Set<Long>> validator) {
        validator.accept(concourse.find(criteria));
    }

    @Override
    public void select(String key, Criteria criteria,
            Consumer<Map<Long, Set<Object>>> consumer) {
        consumer.accept(concourse.select(key, criteria));
    }

    @Override
    public void set(String key, Object value, long record) {
        concourse.set(key, value, record);
    }

    @Override
    public void add(String key, Object value, long record) {
        concourse.add(key, value, record);
    }

    @Override
    public void remove(String key, Object value, long record) {
        concourse.remove(key, value, record);
    }

    @Override
    public void clear(String key, long record) {
        concourse.clear(key, record);
    }

    @Override
    public void clear(long record) {
        concourse.clear(record);
    }

    @Override
    public void verifyOrSet(String key, Object value, long record) {
        concourse.verifyOrSet(key, value, record);
    }

    @Override
    public void reconcile(String key, long record, Collection<?> values) {
        if(values.isEmpty()) {
            concourse.clear(key, record);
        }
        else {
            concourse.reconcile(key, record, values.toArray());
        }
    }

    @Override
    public void reconcile(String key, long record, Object[] values) {
        if(values.length == 0) {
            concourse.clear(key, record);
        }
        else {
            concourse.reconcile(key, record, values);
        }
    }

}
