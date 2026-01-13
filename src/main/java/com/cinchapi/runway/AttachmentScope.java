/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.runway;

import java.util.Set;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A scoped attachment of {@link AdHocDataSource AdHocDataSources}
 * to a {@link Runway} instance.
 * <p>
 * An {@link AttachmentScope} is returned by
 * {@link Runway#attach(AdHocDataSource...)} and provides a
 * {@link DatabaseInterface} that delegates to the underlying {@link Runway}
 * while the attached sources are active. When {@link #close() closed}, all
 * attached sources are automatically detached.
 * </p>
 * <p>
 * This class implements {@link AutoCloseable} for use with try-with-resources:
 * </p>
 *
 * <pre>
 * {@code
 * try (AttachmentScope scope = runway.attach(source)) {
 *     scope.load(MyAdHocRecord.class); // Uses attached source
 * }
 * // Source automatically detached
 * }
 * </pre>
 *
 * @author Jeff Nelson
 */
public class AttachmentScope implements DatabaseInterface, AutoCloseable {

    /**
     * The underlying {@link Runway} instance.
     */
    private final Runway runway;

    /**
     * The attached sources, held for detachment on close.
     */
    private final AdHocDataSource<?>[] sources;

    /**
     * Flag indicating whether this scope has been closed.
     */
    private boolean closed = false;

    /**
     * Construct a new instance.
     *
     * @param runway the underlying {@link Runway}
     * @param sources the attached sources
     */
    AttachmentScope(Runway runway, AdHocDataSource<?>[] sources) {
        this.runway = runway;
        this.sources = sources;
    }

    @Override
    public void close() {
        if(!closed) {
            for (AdHocDataSource<?> source : sources) {
                runway.detach(source);
            }
            closed = true;
        }
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        return runway.find(clazz, criteria, order, page, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return runway.find(clazz, criteria, order, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return runway.find(clazz, criteria, page, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return runway.find(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        return runway.findAny(clazz, criteria, order, page, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return runway.findAny(clazz, criteria, order, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return runway.findAny(clazz, criteria, page, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return runway.findAny(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> T findAnyUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return runway.findAnyUnique(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> T findUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return runway.findUnique(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> T load(Class<T> clazz, long id, Realms realms) {
        return runway.load(clazz, id, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Realms realms) {
        return runway.load(clazz, order, page, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Realms realms) {
        return runway.load(clazz, order, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Realms realms) {
        return runway.load(clazz, page, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Realms realms) {
        return runway.load(clazz, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Page page, Realms realms) {
        return runway.loadAny(clazz, order, page, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Realms realms) {
        return runway.loadAny(clazz, order, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Realms realms) {
        return runway.loadAny(clazz, page, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Realms realms) {
        return runway.loadAny(clazz, realms);
    }

}
