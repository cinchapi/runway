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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.google.common.base.Preconditions;

/**
 * A {@link DatabaseInterface} that unifies persistent data from {@link Runway}
 * with ad-hoc data from {@link AdHocDatabase AdHocDatabases}.
 * <p>
 * A {@link FederatedRunway} routes queries to different data sources based on
 * the requested class. Queries for registered {@link AdHocRecord} types are
 * handled by their corresponding {@link AdHocDatabase}. All other queries are
 * routed to the underlying {@link Runway} instance.
 * </p>
 * <h2>Example</h2>
 * <pre>
 * {@code
 * AdHocDatabase<ReportRecord> reports = new AdHocDatabase<>(
 *     ReportRecord.class, () -> generateReports());
 *
 * FederatedRunway db = FederatedRunway.builder()
 *     .defaultTo(runway)
 *     .register(reports)
 *     .build();
 *
 * // Routes to runway
 * db.load(User.class);
 *
 * // Routes to reports AdHocDatabase
 * db.find(ReportRecord.class, criteria);
 * }
 * </pre>
 *
 * @author Jeff Nelson
 */
public final class FederatedRunway implements DatabaseInterface {

    /**
     * Return a new {@link Builder} for constructing a {@link FederatedRunway}.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The default {@link Runway} for persistent record types.
     */
    private final Runway runway;

    /**
     * Mapping from class to the {@link AdHocDatabase} that handles it.
     */
    private final Map<Class<? extends AdHocRecord>, AdHocDatabase<?>> registry;

    /**
     * Construct a new instance.
     *
     * @param runway the default Runway
     * @param registry the class-to-database mappings
     */
    private FederatedRunway(Runway runway,
            Map<Class<? extends AdHocRecord>, AdHocDatabase<?>> registry) {
        this.runway = runway;
        this.registry = registry;
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        return resolve(clazz).find(clazz, criteria, order, page, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return resolve(clazz).find(clazz, criteria, order, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return resolve(clazz).find(clazz, criteria, page, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return resolve(clazz).find(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        return resolveHierarchy(clazz).findAny(clazz, criteria, order, page,
                realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return resolveHierarchy(clazz).findAny(clazz, criteria, order, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return resolveHierarchy(clazz).findAny(clazz, criteria, page, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return resolveHierarchy(clazz).findAny(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> T findAnyUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return resolveHierarchy(clazz).findAnyUnique(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> T findUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return resolve(clazz).findUnique(clazz, criteria, realms);
    }

    @Override
    public <T extends Record> T load(Class<T> clazz, long id, Realms realms) {
        return resolve(clazz).load(clazz, id, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Realms realms) {
        return resolve(clazz).load(clazz, order, page, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Realms realms) {
        return resolve(clazz).load(clazz, order, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Realms realms) {
        return resolve(clazz).load(clazz, page, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Realms realms) {
        return resolve(clazz).load(clazz, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Page page, Realms realms) {
        return resolveHierarchy(clazz).loadAny(clazz, order, page, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Realms realms) {
        return resolveHierarchy(clazz).loadAny(clazz, order, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Realms realms) {
        return resolveHierarchy(clazz).loadAny(clazz, page, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Realms realms) {
        return resolveHierarchy(clazz).loadAny(clazz, realms);
    }

    /**
     * Resolve the {@link DatabaseInterface} for the given class.
     * <p>
     * Return the registered {@link AdHocDatabase} if the class is explicitly
     * registered, otherwise return the default {@link Runway}.
     * </p>
     *
     * @param clazz the class being queried
     * @return the appropriate database
     */
    private DatabaseInterface resolve(Class<? extends Record> clazz) {
        AdHocDatabase<?> db = registry.get(clazz);
        return db != null ? db : runway;
    }

    /**
     * Resolve the {@link DatabaseInterface} for hierarchy queries.
     * <p>
     * Check if any registered class is assignable from the requested class.
     * If so, return that database; otherwise return the default
     * {@link Runway}.
     * </p>
     *
     * @param clazz the class being queried
     * @return the appropriate database
     */
    private DatabaseInterface resolveHierarchy(Class<? extends Record> clazz) {
        // First try exact match
        AdHocDatabase<?> db = registry.get(clazz);
        if(db != null) {
            return db;
        }
        // Then check if any registered class is a subtype of the requested
        // class
        for (Map.Entry<Class<? extends AdHocRecord>, AdHocDatabase<?>> entry
                : registry.entrySet()) {
            if(clazz.isAssignableFrom(entry.getKey())) {
                return entry.getValue();
            }
        }
        return runway;
    }

    /**
     * A builder for constructing {@link FederatedRunway} instances.
     *
     * @author Jeff Nelson
     */
    public static final class Builder {

        /**
         * The default Runway.
         */
        private Runway runway;

        /**
         * The class-to-database mappings.
         */
        private final Map<Class<? extends AdHocRecord>, AdHocDatabase<?>> registry;

        /**
         * Construct a new instance.
         */
        private Builder() {
            this.registry = new LinkedHashMap<>();
        }

        /**
         * Build and return the configured {@link FederatedRunway}.
         *
         * @return the federated runway
         * @throws IllegalStateException if no default Runway is set
         */
        public FederatedRunway build() {
            Preconditions.checkState(runway != null,
                    "A default Runway must be set");
            return new FederatedRunway(runway, registry);
        }

        /**
         * Set the default {@link Runway} for persistent record types.
         *
         * @param runway the default Runway
         * @return this builder
         */
        public Builder defaultTo(Runway runway) {
            this.runway = runway;
            return this;
        }

        /**
         * Register an {@link AdHocDatabase} to handle queries for its
         * registered class.
         * <p>
         * Queries for the database's registered class will be routed to it.
         * For hierarchy queries (e.g., {@code loadAny}), queries for any
         * supertype of the registered class will also be routed to this
         * database.
         * </p>
         *
         * @param db the ad-hoc database to register
         * @return this builder
         */
        public Builder register(AdHocDatabase<?> db) {
            this.registry.put(db.registeredClass(), db);
            return this;
        }
    }

}

