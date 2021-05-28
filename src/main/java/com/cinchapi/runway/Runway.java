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
package com.cinchapi.runway;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import com.cinchapi.ccl.Parser;
import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.common.concurrent.ExecutorRaceService;
import com.cinchapi.common.function.TriConsumer;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.ValueState;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.cinchapi.concourse.server.plugin.util.Versions;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logging;
import com.cinchapi.concourse.util.Parsers;
import com.cinchapi.runway.cache.CachingConnectionPool;
import com.cinchapi.runway.util.Paging;
import com.github.zafarkhaja.semver.Version;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

/**
 * {@link Runway} is the ORM controller for Concourse.
 * <p>
 * {@link Runway} generally provides methods to retrieve {@link Record} objects.
 * Subsequent interaction with Records is done using instance methods.
 * </p>
 * <p>
 * If an application has multiple {@link Runway} instances, implicit
 * {@link Record#save() saving} is disabled in which case the application must
 * use the {@link #save(Record...)} method provided by this controller.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class Runway implements AutoCloseable, DatabaseInterface {

    // NOTE: Internal methods within a $ prefix are ones that return raw
    // database results and are intended to be consumed by other methods in this
    // class.

    /**
     * Return a builder that can be used to precisely configure a {@link Runway}
     * instance.
     * 
     * @return a {@link Runway} builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Return a {@link Runway} instance that is connected to Concourse using the
     * default connection parameters.
     * 
     * @return a {@link Runway} instance
     */
    public static Runway connect() {
        return builder().build();
    }

    /**
     * Return a {@link Runway} instance that is connected to Concourse using the
     * provided connection parameters.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @return a {@link Runway} instance
     * @deprecated use {@link #builder()} instead
     */
    @Deprecated
    public static Runway connect(String host, int port, String username,
            String password) {
        return builder().host(host).port(port).username(username)
                .password(password).build();
    }

    /**
     * Return a {@link Runway} instance that is connected to Concourse using the
     * provided connection parameters.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     * @return a {@link Runway} instance
     * @deprecated use {@link #builder()} instead
     */
    @Deprecated
    public static Runway connect(String host, int port, String username,
            String password, String environment) {
        return builder().host(host).port(port).username(username)
                .password(password).environment(environment).build();
    }

    /**
     * Return a {@link List} based order specification.
     * 
     * @param order
     * @return the list-based order
     */
    private static List<String> backwardsCompatible(Order order) {
        List<String> components = Lists.newArrayList();
        for (OrderComponent component : order.spec()) {
            if(component.timestamp() != null) {
                throw new UnsupportedOperationException(
                        "An OrderComponent with a timestamp is not backwards compatible");
            }
            else {
                String prefix = component.direction() == Direction.ASCENDING
                        ? Record.SORT_DIRECTION_ASCENDING_PREFIX
                        : Record.SORT_DIRECTION_DESCENDING_PREFIX;
                components.add(prefix + component.key());
            }
        }
        return components;
    }

    /**
     * Call
     * {@link Record#load(Class, long, TLongObjectMap, ConnectionPool, Runway, Map)}
     * and handle any errors with the {@link #onLoadFailureHandler}.
     * 
     * @param <T>
     * @param clazz
     * @param id
     * @param connections
     * @param runway
     * @param data
     * @return the loaded {@link Record} instance
     */
    private static <T extends Record> T loadWithErrorHandling(Class<T> clazz,
            long id, ConnectionPool connections, Runway runway,
            @Nullable Map<String, Set<Object>> data) {
        try {
            return Record.load(clazz, id, new TLongObjectHashMap<>(),
                    connections, runway, data);
        }
        catch (Exception e) {
            runway.onLoadFailureHandler.accept(clazz, id, e);
            throw e;
        }
    }

    /**
     * The default {@link #onLoadFailureHandler}.
     */
    private static TriConsumer<Class<? extends Record>, Long, Throwable> DEFAULT_ON_LOAD_FAILURE_HANDLER = (
            clazz, record, error) -> record.toString();

    /**
     * A mapping from each {@link Record} class to all of its descendants. This
     * facilitates querying across hierarchies.
     */
    private static final Multimap<Class<?>, Class<?>> hierarchies;

    /**
     * A collection of all the active {@link Runway} instances.
     */
    private static Set<Runway> instances = Sets.newHashSet();

    /**
     * The record where metadata is stored. We typically store some transient
     * metadata for transaction routing within this record (so its only visible
     * within the specific transaction) and we clear it before commit time.
     */
    private static long METADATA_RECORD = -1;

    /**
     * Placeholder for a {@code null} {@link Order} parameter.
     */
    private static Order NO_ORDER = null;

    /**
     * Placeholder for a {@code null} {@link Page} parameter.
     */
    private static Page NO_PAGINATION = null;

    static {
        // NOTE: Scanning the classpath adds startup costs proportional to the
        // number of classes defined. We do this once at startup to minimize the
        // effect of the cost.
        hierarchies = HashMultimap.create();
        Logging.disable(Reflections.class);
        Reflections.log = null; // turn off reflection logging
        Reflections reflection = new Reflections(new SubTypesScanner());
        reflection.getSubTypesOf(Record.class).forEach(type -> {
            hierarchies.put(type, type);
            reflection.getSubTypesOf(type)
                    .forEach(subType -> hierarchies.put(type, subType));
        });
    }

    /**
     * The amount of time to wait for a bulk select to complete before streaming
     * the data.
     */
    @VisibleForTesting
    protected int bulkSelectTimeoutMillis = 0; // make configurable?

    /**
     * A connection pool to the underlying Concourse database.
     */
    /* package */ final ConnectionPool connections;

    /**
     * An {@link ExecutorService} for async tasks.
     */
    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * A flag that indicates whether the connected server supports result set
     * sorting and pagination.
     */
    private final boolean hasNativeSortingAndPagination;

    /**
     * Whenever an exception is thrown during a {@link Runway#load(long)
     * load} operation, the provided {@code onLoadFailureHandler} receives
     * the record's class, id and error for processing.
     */
    private TriConsumer<Class<? extends Record>, Long, Throwable> onLoadFailureHandler = DEFAULT_ON_LOAD_FAILURE_HANDLER;

    /**
     * The strategy for {@link #read(Concourse, Criteria, Order, Page)
     * loading} data from the database.
     */
    private ReadStrategy readStrategy = ReadStrategy.AUTO;

    /**
     * The maximum number of records to buffer in memory when selecting data
     * from the database. This is only relevant when the {@link #readStrategy}
     * is not {@link ReadStrategy#BULK}.
     */
    private int streamingReadBufferSize = 1000;

    /**
     * A mapping from a transaction id to the set of records that are waiting to
     * be saved within that transaction. We use this collection to ensure that a
     * record being saved only links to an existing record in the database or a
     * record that will later exist (e.g. waiting to be saved).
     */
    private final TLongObjectMap<Set<Record>> waitingToBeSaved = new TLongObjectHashMap<Set<Record>>();

    /**
     * Construct a new instance.
     * 
     * @param connections a Concourse {@link ConnectionPool}
     */
    private Runway(ConnectionPool connections) {
        this.connections = connections;
        instances.add(this);
        if(instances.size() > 1) {
            Record.PINNED_RUNWAY_INSTANCE = null;
        }
        else {
            Record.PINNED_RUNWAY_INSTANCE = this;
        }
        Concourse concourse = connections.request();
        try {
            Version target = Version.forIntegers(0, 10);
            Version actual = Versions
                    .parseSemanticVersion(concourse.getServerVersion());
            this.hasNativeSortingAndPagination = actual
                    .greaterThanOrEqualTo(target);
        }
        finally {
            connections.release(concourse);
        }
    }

    @Override
    public void close() throws Exception {
        if(!connections.isClosed()) {
            connections.close();
        }
        instances.remove(this);
        if(instances.size() == 1) {
            Record.PINNED_RUNWAY_INSTANCE = instances.iterator().next();
        }
        else {
            Record.PINNED_RUNWAY_INSTANCE = null;
        }
        executor.shutdownNow();
    }

    @Override
    public <T extends Record> int count(Class<T> clazz, Realms realms) {
        return count($Criteria.amongRealms(realms, $Criteria.forClass(clazz)));
    }

    @Override
    public <T extends Record> int count(Class<T> clazz, Criteria criteria,
            Realms realms) {
        if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
            return count($Criteria.amongRealms(realms,
                    $Criteria.withinClass(clazz, criteria)));
        }
        else {
            return filter(clazz, criteria, NO_ORDER, NO_PAGINATION, realms)
                    .size();
        }
    }

    @Override
    public <T extends Record> int countAny(Class<T> clazz, Realms realms) {
        return count($Criteria.amongRealms(realms,
                $Criteria.forClassHierarchy(clazz)));
    }

    @Override
    public <T extends Record> int countAny(Class<T> clazz, Criteria criteria,
            Realms realms) {
        if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
            return count($Criteria.amongRealms(realms,
                    $Criteria.withinClass(clazz, criteria)));
        }
        else {
            return filterAny(clazz, criteria, NO_ORDER, NO_PAGINATION, realms)
                    .size();
        }
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Realms realms) {
        Concourse concourse = connections.request();
        try {
            if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                Map<Long, Map<String, Set<Object>>> data = $find(concourse,
                        clazz, criteria, NO_ORDER, NO_PAGINATION, realms);
                return instantiateAll(clazz, data);
            }
            else {
                return filter(clazz, criteria, NO_ORDER, NO_PAGINATION, realms);
            }
        }
        finally {
            connections.release(concourse);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                    Map<Long, Map<String, Set<Object>>> data = $find(concourse,
                            clazz, criteria, order, NO_PAGINATION, realms);
                    return instantiateAll(clazz, data);
                }
                else {
                    return filter(clazz, criteria, order, NO_PAGINATION,
                            realms);
                }
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return find(clazz, criteria, backwardsCompatible(order)).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                    Map<Long, Map<String, Set<Object>>> data = $find(concourse,
                            clazz, criteria, order, page, realms);
                    return instantiateAll(clazz, data);
                }
                else {
                    return filter(clazz, criteria, order, page, realms);
                }
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return find(clazz, criteria, backwardsCompatible(order)).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                    Map<Long, Map<String, Set<Object>>> data = $find(concourse,
                            clazz, criteria, NO_ORDER, page, realms);
                    return instantiateAll(clazz, data);
                }
                else {
                    return filter(clazz, criteria, NO_ORDER, page, realms);
                }
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return find(clazz, criteria).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Realms realms) {
        Concourse concourse = connections.request();
        try {
            if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                Map<Long, Map<String, Set<Object>>> data = $findAny(concourse,
                        clazz, criteria, NO_ORDER, NO_PAGINATION, realms);
                return instantiateAll(data);
            }
            else {
                return filterAny(clazz, criteria, NO_ORDER, NO_PAGINATION,
                        realms);
            }
        }
        finally {
            connections.release(concourse);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                    Map<Long, Map<String, Set<Object>>> data = $findAny(
                            concourse, clazz, criteria, order, NO_PAGINATION,
                            realms);
                    return instantiateAll(data);
                }
                else {
                    return filterAny(clazz, criteria, order, NO_PAGINATION,
                            realms);
                }
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return findAny(clazz, criteria, backwardsCompatible(order)).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                    Map<Long, Map<String, Set<Object>>> data = $findAny(
                            concourse, clazz, criteria, order, page, realms);
                    return instantiateAll(data);
                }
                else {
                    return filterAny(clazz, criteria, order, page, realms);
                }
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return findAny(clazz, criteria, backwardsCompatible(order)).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .skip(page.skip()).limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                    Map<Long, Map<String, Set<Object>>> data = $findAny(
                            concourse, clazz, criteria, NO_ORDER, page, realms);
                    return instantiateAll(data);
                }
                else {
                    return filterAny(clazz, criteria, NO_ORDER, page, realms);
                }
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return findAny(clazz, criteria).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .skip(page.skip()).limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Record> T findAnyUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        Concourse concourse = connections.request();
        try {
            if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                Map<Long, Map<String, Set<Object>>> data = $findAny(concourse,
                        clazz, criteria, NO_ORDER, NO_PAGINATION, realms);
                if(data.isEmpty()) {
                    return null;
                }
                else if(data.size() == 1) {
                    return (T) instantiate(data.keySet().iterator().next(),
                            data.values().iterator().next());
                }
                else {
                    throw new DuplicateEntryException(
                            new com.cinchapi.concourse.thrift.DuplicateEntryException(
                                    AnyStrings.format(
                                            "There are more than one records that match {} in the hierarchy of {}",
                                            criteria, clazz)));
                }
            }
            else {
                Set<T> records = filterAny(clazz, criteria, NO_ORDER,
                        NO_PAGINATION, realms);
                if(records.isEmpty()) {
                    return null;
                }
                else if(records.size() == 1) {
                    return records.iterator().next();
                }
                else {
                    throw new DuplicateEntryException(
                            new com.cinchapi.concourse.thrift.DuplicateEntryException(
                                    AnyStrings.format(
                                            "There are more than one records that match {} in the hierarchy of {}",
                                            criteria, clazz)));
                }

            }
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Find the one record of type {@code clazz} that matches the
     * {@code criteria}. If more than one record matches, throw a
     * {@link DuplicateEntryException}.
     * 
     * @param clazz
     * @param criteria
     * @return the one matching record
     * @throws DuplicateEntryException
     * @deprecated use {@link #findUnique(Class, BuildableState)}
     */
    public <T extends Record> T findOne(Class<T> clazz,
            BuildableState criteria) {
        return findUnique(clazz, criteria);
    }

    /**
     * Find the one record of type {@code clazz} that matches the
     * {@code criteria}. If more than one record matches, throw a
     * {@link DuplicateEntryException}.
     * 
     * @param clazz
     * @param criteria
     * @return the one matching record
     * @throws DuplicateEntryException
     * @deprecated use {@link, #findUnique(Class, Criteria)}
     */
    public <T extends Record> T findOne(Class<T> clazz, Criteria criteria) {
        return findUnique(clazz, criteria);
    }

    @Override
    public <T extends Record> T findUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        Concourse concourse = connections.request();
        try {
            if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                Map<Long, Map<String, Set<Object>>> data = $find(concourse,
                        clazz, criteria, NO_ORDER, NO_PAGINATION, realms);
                if(data.isEmpty()) {
                    return null;
                }
                else if(data.size() == 1) {
                    return (T) instantiate(clazz,
                            data.keySet().iterator().next(),
                            data.values().iterator().next());
                }
                else {
                    throw new DuplicateEntryException(
                            new com.cinchapi.concourse.thrift.DuplicateEntryException(
                                    AnyStrings.format(
                                            "There are more than one records that match {} in {}",
                                            criteria, clazz)));
                }
            }
            else {
                Set<T> records = filterAny(clazz, criteria, NO_ORDER,
                        NO_PAGINATION, realms);
                if(records.isEmpty()) {
                    return null;
                }
                else if(records.size() == 1) {
                    return records.iterator().next();
                }
                else {
                    throw new DuplicateEntryException(
                            new com.cinchapi.concourse.thrift.DuplicateEntryException(
                                    AnyStrings.format(
                                            "There are more than one records that match {} in {}",
                                            criteria, clazz)));
                }
            }
        }
        finally {
            connections.release(concourse);
        }
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Realms realms) {
        Concourse concourse = connections.request();
        try {
            Map<Long, Map<String, Set<Object>>> data = $load(concourse, clazz,
                    NO_ORDER, NO_PAGINATION, realms);
            return instantiateAll(clazz, data);
        }
        finally {
            connections.release(concourse);
        }
    }

    @Override
    public <T extends Record> T load(Class<T> clazz, long id, Realms realms) {
        if(hierarchies.get(clazz).size() > 1) {
            // The provided clazz has descendants, so it is possible that the
            // Record with the #id is actually a member of a subclass
            Concourse connection = connections.request();
            try {
                String section = connection.get(Record.SECTION_KEY, id);
                if(section != null) {
                    clazz = Reflection.getClassCasted(section);
                }
            }
            finally {
                connections.release(connection);
            }
        }
        if(!realms.names().isEmpty()) {
            Concourse connection = connections.request();
            try {
                Set<String> $realms = MoreObjects.firstNonNull(
                        connection.select(Record.REALMS_KEY, id),
                        ImmutableSet.of());
                if(Sets.intersection($realms, realms.names()).isEmpty()) {
                    return null; // TODO: what to do here?
                }
            }
            finally {
                connections.release(connection);
            }
        }
        return instantiate(clazz, id, null);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                Map<Long, Map<String, Set<Object>>> data = $load(concourse,
                        clazz, order, NO_PAGINATION, realms);
                return instantiateAll(clazz, data);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return load(clazz, backwardsCompatible(order));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                Map<Long, Map<String, Set<Object>>> data = $load(concourse,
                        clazz, order, page, realms);
                return instantiateAll(clazz, data);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return load(clazz, backwardsCompatible(order)).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .skip(page.skip()).limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                Map<Long, Map<String, Set<Object>>> data = $load(concourse,
                        clazz, NO_ORDER, page, realms);
                return instantiateAll(clazz, data);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return load(clazz).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .skip(page.skip()).limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Realms realms) {
        Concourse concourse = connections.request();
        try {
            Map<Long, Map<String, Set<Object>>> data = $loadAny(concourse,
                    clazz, NO_ORDER, NO_PAGINATION, realms);
            return instantiateAll(data);
        }
        finally {
            connections.release(concourse);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                Map<Long, Map<String, Set<Object>>> data = $loadAny(concourse,
                        clazz, order, NO_PAGINATION, realms);
                return instantiateAll(data);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return load(clazz, backwardsCompatible(order)).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Page page, Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                Map<Long, Map<String, Set<Object>>> data = $loadAny(concourse,
                        clazz, order, page, realms);
                return instantiateAll(data);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return load(clazz, backwardsCompatible(order)).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .skip(page.skip()).limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Realms realms) {
        if(hasNativeSortingAndPagination) {
            Concourse concourse = connections.request();
            try {
                Map<Long, Map<String, Set<Object>>> data = $loadAny(concourse,
                        clazz, NO_ORDER, page, realms);
                return instantiateAll(data);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            return load(clazz).stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty())
                    .skip(page.skip()).limit(page.limit())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    /**
     * Save all the changes in all of the {@code records} using a single ACID
     * transaction, which means that all the changes must be save or none of
     * them will.
     * <p>
     * <strong>NOTE:</strong> If there is only one record provided, this method
     * has the same effect as {@link Record#save(Runway)}.
     * </p>
     * 
     * @param records one or more records to be saved
     * @return {@code true} if all the changes are atomically saved
     */
    public boolean save(Record... records) {
        if(records.length == 1) {
            Concourse concourse = connections.request();
            try {
                return records[0].save(concourse, Sets.newHashSet(), this);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            Concourse concourse = connections.request();
            long transactionId = Time.now();
            Record current = null;
            try {
                concourse.stage();
                concourse.set("transaction_id", transactionId, METADATA_RECORD);
                Set<Record> waiting = Sets.newHashSet(records);
                Set<Record> seen = Sets.newHashSet();
                waitingToBeSaved.put(transactionId, waiting);
                for (Record record : records) {
                    current = record;
                    record.saveWithinTransaction(concourse, seen);
                }
                concourse.clear("transaction_id", METADATA_RECORD);
                return concourse.commit();
            }
            catch (Throwable t) {
                concourse.abort();
                if(current != null) {
                    current.errors.add(t);
                }
                return false;
            }
            finally {
                waitingToBeSaved.remove(transactionId);
                connections.release(concourse);
            }
        }
    }

    /**
     * Search for records in {@code clazz} that match the search {@query} across
     * any of the provided {@code keys}.
     * 
     * @param clazz
     * @param query
     * @param keys
     * @return the matching search results
     */
    public <T extends Record> Set<T> search(Class<T> clazz, String query,
            String... keys) {
        Concourse concourse = connections.request();
        try {
            Set<Long> ids = $search(concourse, clazz, query, keys);
            return instantiateAll(clazz, ids);
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Search for records across the hierarchy of {@code clazz} that match the
     * search {@query} across any of the provided {@code keys}.
     * 
     * @param clazz
     * @param query
     * @param keys
     * @return the matching search results
     */
    public <T extends Record> Set<T> searchAny(Class<T> clazz, String query,
            String... keys) {
        Concourse concourse = connections.request();
        try {
            Set<Long> ids = $searchAny(concourse, clazz, query, keys);
            return instantiateAll(ids);
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Load a record by {@code id} without knowing its class.
     * 
     * @param id
     * @return the loaded record
     */
    <T extends Record> T load(long id) {
        return instantiate(id, null);
    }

    /**
     * Perform the find operation using the {@code concourse} handler.
     * 
     * @param concourse
     * @param clazz
     * @param criteria
     * @return the result set
     */
    private <T extends Record> Map<Long, Map<String, Set<Object>>> $find(
            Concourse concourse, Class<T> clazz, Criteria criteria,
            @Nullable Order order, @Nullable Page page,
            @Nonnull Realms realms) {
        criteria = $Criteria.amongRealms(realms,
                $Criteria.withinClass(clazz, criteria));
        return read(concourse, criteria, order, page);
    }

    /**
     * Perform the "find any" operation using the {@code concourse} handler.
     * 
     * @param concourse
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param realms
     * @return the result set
     */
    private <T extends Record> Map<Long, Map<String, Set<Object>>> $findAny(
            Concourse concourse, Class<T> clazz, Criteria criteria,
            @Nullable Order order, @Nullable Page page,
            @Nonnull Realms realms) {
        criteria = $Criteria.amongRealms(realms,
                $Criteria.accrossClassHierachy(clazz, criteria));
        return read(concourse, criteria, order, page);
    }

    /**
     * Return the ids of all the {@code Record}s in the {@code clazz}, using the
     * provided {@code concourse} connection.
     * 
     * @param concourse
     * @param clazz
     * @param order
     * @param page
     * @param realms
     * @return the records in the class
     */
    private <T extends Record> Map<Long, Map<String, Set<Object>>> $load(
            Concourse concourse, Class<T> clazz, @Nullable Order order,
            @Nullable Page page, @Nonnull Realms realms) {
        Criteria criteria = $Criteria.amongRealms(realms,
                $Criteria.forClass(clazz));
        return read(concourse, criteria, order, page);
    }

    /**
     * Return the ids of all the {@code Record}s in the {@code clazz} hierarchy,
     * using the provided {@code concourse} connection.
     * 
     * @param concourse
     * @param clazz
     * @param order
     * @param page
     * @param realms
     * @return the records in the class hierarchy
     */
    private <T extends Record> Map<Long, Map<String, Set<Object>>> $loadAny(
            Concourse concourse, Class<T> clazz, @Nullable Order order,
            @Nullable Page page, Realms realms) {
        Criteria criteria = $Criteria.amongRealms(realms,
                $Criteria.forClassHierarchy(clazz));
        return read(concourse, criteria, order, page);
    }

    /**
     * Perform a search.
     * 
     * @param concourse
     * @param clazz
     * @param query
     * @param keys
     * @return the ids of the records that match the search
     */
    private <T extends Record> Set<Long> $search(Concourse concourse,
            Class<T> clazz, String query, String... keys) {
        return Arrays.stream(keys).map(key -> concourse.search(key, query))
                .flatMap(Set::stream)
                .filter(record -> concourse.get(Record.SECTION_KEY, record)
                        .equals(clazz.getName()))
                .collect(Collectors.toSet());
    }

    /**
     * Internal method to perform a search across a {@code clazz} hierarchy and
     * return the matching ids.
     * 
     * @param concourse
     * @param clazz
     * @param query
     * @param keys
     * @return the ids of the records that match the search
     */
    @SuppressWarnings("rawtypes")
    private <T extends Record> Set<Long> $searchAny(Concourse concourse,
            Class<T> clazz, String query, String... keys) {
        Collection<Class<?>> hierarchy = hierarchies.get(clazz);
        Predicate<Long> filter = null;
        for (Class cls : hierarchy) {
            Predicate<Long> $filter = record -> concourse
                    .get(Record.SECTION_KEY, record).equals(cls.getName());
            if(filter == null) {
                filter = $filter;
            }
            else {
                filter = filter.or($filter);
            }
        }
        return Arrays.stream(keys).map(key -> concourse.search(key, query))
                .flatMap(Set::stream).filter(filter)
                .collect(Collectors.toSet());
    }

    /**
     * Return the number of {@link Record records} that match the
     * {@code criteria}.
     * 
     * @param criteria
     * @return the number of matching records
     */
    private int count(Criteria criteria) {
        Concourse concourse = connections.request();
        try {
            return concourse.find(criteria).size();
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Perform local {@code criteria} resolution and return all the records in
     * {@code clazz} that match.
     * 
     * @param <T>
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param realms
     * @return the matching records in {@code clazz}
     */
    private <T extends Record> Set<T> filter(Class<T> clazz, Criteria criteria,
            @Nullable Order order, @Nullable Page page,
            @Nonnull Realms realms) {
        Set<T> records = order == null ? load(clazz) : load(clazz, order);
        Parser parser = Parsers.create($Criteria.amongRealms(realms, criteria));
        String[] keys = parser.analyze().keys().toArray(Array.containing());
        records = Sets.filter(records,
                record -> parser.evaluate(record.mmap(keys)));
        if(page != null) {
            records = Paging.paginate(records, page);
        }
        return records;
    }

    /**
     * Perform local {@code criteria} resolution and return all the records in
     * the hierarchy of {@code clazz} that match.
     * 
     * @param <T>
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param realms
     * @return the matching records in the {@code clazz} hierarchy
     */
    private <T extends Record> Set<T> filterAny(Class<T> clazz,
            Criteria criteria, @Nullable Order order, @Nullable Page page,
            @Nonnull Realms realms) {
        Set<T> records = order == null ? loadAny(clazz) : loadAny(clazz, order);
        Parser parser = Parsers.create($Criteria.amongRealms(realms, criteria));
        String[] keys = parser.analyze().keys().toArray(Array.containing());
        records = Sets.filter(records,
                record -> parser.evaluate(record.mmap(keys)));
        if(page != null) {
            records = Paging.paginate(records, page);
        }
        return records;
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and
     * has the specified {@code id}.
     * <p>
     * If a {@link #cache} is NOT provided (e.g. {@link NopOpCache} is being
     * used), multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation. If a cache is provided, the rules (e.g.
     * expiration policy, etc) of said cache govern when multiple calls to this
     * method return the same instance for the provided parameters or not.
     * </p>
     * 
     * @param clazz
     * @param id
     * @param existing
     * @param data
     * @return the loaded {@link Record} instance
     */
    private <T extends Record> T instantiate(Class<T> clazz, long id,
            @Nullable Map<String, Set<Object>> data) {
        return loadWithErrorHandling(clazz, id, connections, this, data);
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and
     * has the specified {@code id}.
     * <p>
     * Unlike {@link #instantiate(Class, long, TLongObjectHashMap, Map)} this
     * method
     * does not need to know the desired {@link Class} of the loaded
     * {@link Record}.
     * </p>
     * <p>
     * If a {@link #cache} is NOT provided (e.g. {@link NopOpCache} is being
     * used), multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation. If a cache is provided, the rules (e.g.
     * expiration policy, etc) of said cache govern when multiple calls to this
     * method return the same instance for the provided parameters or not.
     * </p>
     * 
     * @param id
     * @param existing
     * @param data
     * @return the loaded {@link Record} instance
     */
    private <T extends Record> T instantiate(long id,
            @Nullable Map<String, Set<Object>> data) {
        if(data == null) {
            // Since the desired class isn't specified, we must
            // prematurely select the record's data to determine it.
            Concourse connection = connections.request();
            try {
                data = connection.select(id);
            }
            finally {
                connections.release(connection);
            }
        }
        String section = (String) Iterables
                .getLast(data.get(Record.SECTION_KEY));
        Class<T> clazz = Reflection.getClassCasted(section);
        return loadWithErrorHandling(clazz, id, connections, this, data);
    }

    /**
     * Create a {@link Record} instance of type {@code clazz} (or one of its
     * descendants) for each entry in the {@code data}
     * 
     * @param clazz
     * @param data
     * @return the instantiated {@link Record}s
     */
    private <T extends Record> Set<T> instantiateAll(Class<T> clazz,
            Map<Long, Map<String, Set<Object>>> data) {
        Set<T> records = LazyTransformSet.of(data.keySet(), id -> {
            return instantiate(clazz, id, data.get(id));
        });
        return records;
    }

    /**
     * Create a {@link Record} instance of type {@code clazz} (or one of its
     * descendants) for each of the {@code ids}.
     * 
     * @param clazz
     * @param ids
     * @return the instantiated {@link Record}s
     */
    private <T extends Record> Set<T> instantiateAll(Class<T> clazz,
            Set<Long> ids) {
        AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
        Set<T> records = LazyTransformSet.of(ids, id -> {
            if(data.get() == null) {
                data.set(stream(ids));
            }
            return instantiate(clazz, id, data.get().get(id));
        });
        return records;
    }

    /**
     * Create a {@link Record} instance for each entry in the {@code data}
     * 
     * @param data
     * @return the instantiated {@link Record}s
     */
    private <T extends Record> Set<T> instantiateAll(
            Map<Long, Map<String, Set<Object>>> data) {
        Set<T> records = LazyTransformSet.of(data.keySet(), id -> {
            return instantiate(id, data.get(id));
        });
        return records;
    }

    /**
     * Create a {@link Record} instance for each of the {@code ids}.
     * <p>
     * The {@link Record} class will be determined by the data stored for each
     * of the {@code ids}.
     * </p>
     * 
     * @param ids
     * @return the instantiated {@link Record}s
     */
    private <T extends Record> Set<T> instantiateAll(Set<Long> ids) {
        AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
        Set<T> records = LazyTransformSet.of(ids, id -> {
            if(data.get() == null) {
                data.set(stream(ids));
            }
            return instantiate(id, data.get().get(id));
        });
        return records;
    }

    /**
     * Internal utility method to dispatch a "select" request" for a
     * {@code criteria} based on whether the {@code order} and/or {@code page}
     * params are non-null.
     * 
     * @param concourse
     * @param criteria
     * @param order
     * @param page
     * @return the data for the matching records
     */
    @SuppressWarnings("unchecked")
    private Map<Long, Map<String, Set<Object>>> read(Concourse concourse,
            Criteria criteria, @Nullable Order order, @Nullable Page page) {
        // Define the execution paths
        Function<Concourse, Map<Long, Map<String, Set<Object>>>> select;
        Function<Concourse, Set<Long>> find;
        if(order != null && page != null) {
            select = c -> c.select(criteria, order, page);
            find = c -> c.find(criteria, order, page);
        }
        else if(order == null && page == null) {
            select = c -> c.select(criteria);
            find = c -> c.find(criteria);
        }
        else if(order != null) {
            select = c -> c.select(criteria, order);
            find = c -> c.find(criteria, order);
        }
        else { // page != null
            select = c -> c.select(criteria, page);
            find = c -> c.find(criteria, page);
        }
        // Choose the execution path based on the #readStrategy
        Map<Long, Map<String, Set<Object>>> data;
        if(readStrategy == ReadStrategy.BULK) {
            data = select.apply(concourse);
        }
        else if(readStrategy == ReadStrategy.STREAM) {
            Set<Long> ids = find.apply(concourse);
            data = stream(ids);
        }
        else { // ReadStrategy.AUTO
            Callable<Map<Long, Map<String, Set<Object>>>> bulk = () -> {
                Concourse backup = Concourse.copyExistingConnection(concourse);
                try {
                    return select.apply(backup);
                }
                finally {
                    backup.close();
                }
            };
            Callable<Map<Long, Map<String, Set<Object>>>> stream = () -> {
                // In case the bulk select takes too long or an error occurs,
                // fall back to finding the matching ids and incrementally
                // stream the result set.
                Concourse backup = Concourse.copyExistingConnection(concourse);
                try {
                    Set<Long> ids = find.apply(backup);
                    return stream(ids);
                }
                finally {
                    backup.close();
                }
            };
            ExecutorRaceService<Map<Long, Map<String, Set<Object>>>> track = new ExecutorRaceService<>(
                    executor);
            Future<Map<Long, Map<String, Set<Object>>>> future;
            try {
                future = bulkSelectTimeoutMillis > 0
                        ? track.raceWithHeadStart(bulkSelectTimeoutMillis,
                                TimeUnit.MILLISECONDS, bulk, stream)
                        : track.race(bulk, stream);
                data = future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }
        return data;
    }

    /**
     * Intelligently select all the data for the {@code ids} from
     * {@code concourse}.
     * <p>
     * This method assumes that it will be executed asynchronously from a normal
     * read operations so it takes its own connection from the
     * {@link #connections} pool instead of being passed one.
     * </p>
     * 
     * @param concourse
     * @param ids
     * @return the selected data
     */
    private Map<Long, Map<String, Set<Object>>> stream(Set<Long> ids) {
        // The data for the ids is asynchronously selected in the background in
        // a manner that staggers/buffers the amount of data by only selecting
        // {@link #recordsPerSelectBufferSize} from the database at a time.
        return new AbstractMap<Long, Map<String, Set<Object>>>() {

            /**
             * The cached {@link #entrySet()}.
             */
            Set<Entry<Long, Map<String, Set<Object>>>> entrySet = null;

            /**
             * The data that has been loaded from the data into memory. For
             * the items that have been pulled from the {@link #pending}
             * queue.
             */
            Map<Long, Map<String, Set<Object>>> loaded = Maps
                    .newHashMapWithExpectedSize(ids.size()); // TODO: create
                                                             // compound
                                                             // hashmap... that
                                                             // will look across
                                                             // multiple
                                                             // hashmaps until
                                                             // it finds the
                                                             // right value

            /**
             * A FIFO list of record ids that are pending database
             * selection. Items from this queue are popped off in increments
             * of {@value #BULK_SELECT_BUFFER_SIZE} and selected from
             * Concourse.
             */
            Queue<Long> pending = Queues.newArrayDeque(ids);

            @Override
            public Set<Entry<Long, Map<String, Set<Object>>>> entrySet() {
                if(entrySet == null) {
                    entrySet = LazyTransformSet.of(ids, id -> {
                        Map<String, Set<Object>> data = loaded.get(id);
                        while (data == null) {
                            // There is currently no data loaded OR the
                            // currently loaded data does not contain the id. If
                            // that is the case, assume that all unconsumed ids
                            // prior to this one have been skipped and buffer in
                            // data incrementally until the data for this id is
                            // found.
                            int i = 0;
                            Set<Long> records = Sets
                                    .newLinkedHashSetWithExpectedSize(
                                            streamingReadBufferSize);
                            while (pending.peek() != null
                                    && i < streamingReadBufferSize) {
                                records.add(pending.poll());
                            }
                            Concourse concourse = connections.request();
                            try {
                                loaded.putAll(concourse.select(records));
                            }
                            finally {
                                connections.release(concourse);
                            }
                            data = loaded.get(id);
                        }
                        return new AbstractMap.SimpleImmutableEntry<>(id, data);
                    });
                }
                return entrySet;
            }

            @Override
            public Set<Long> keySet() {
                return ids;
            }

        };

    }

    /**
     * Builder for {@link Runway} connections. This is returned from
     * {@link #builder()}.
     *
     * @author Jeff Nelson
     */
    public static class Builder {

        private Cache<Long, Map<String, Set<Object>>> cache;
        private String environment = "";
        private String host = "localhost";
        private TriConsumer<Class<? extends Record>, Long, Throwable> onLoadFailureHandler = null;
        private String password = "admin";
        private int port = 1717;
        private ReadStrategy readStrategy = null;
        private int streamingReadBufferSize = 100;
        private String username = "admin";

        /**
         * Build the configured {@link Runway} and return the instance.
         * 
         * @return a {@link Runway} instance
         */
        public Runway build() {
            ConnectionPool connections = cache == null
                    ? ConnectionPool.newCachedConnectionPool(host, port,
                            username, password, environment)
                    : new CachingConnectionPool(host, port, username, password,
                            environment, cache);
            Runway db = new Runway(connections);
            db.streamingReadBufferSize = streamingReadBufferSize;
            db.readStrategy = MoreObjects.firstNonNull(readStrategy,
                    cache != null ? ReadStrategy.STREAM : ReadStrategy.AUTO);
            if(onLoadFailureHandler != null) {
                db.onLoadFailureHandler = onLoadFailureHandler;
            }
            return db;
        }

        /**
         * Set the connection's cache.
         * 
         * @param cache
         * @return this builder
         * @deprecated {@link Record} caching has been deprecated in favor of
         *             raw data caching for better performance; please use
         *             {@link #withCache(Cache)} to provide a cache instance.
         */
        @Deprecated
        public Builder cache(Cache<Long, Record> cache) {
            return this;
        }

        /**
         * Set the connection's environment.
         * 
         * @param environment
         * @return this builder
         */
        public Builder environment(String environment) {
            this.environment = environment;
            return this;
        }

        /**
         * Set the connection's host.
         * 
         * @param host
         * @return this builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Set the handler for processing load failures.
         * <p>
         * Whenever an exception is thrown during a {@link Runway#load(long)
         * load} operation, the provided {@code onLoadFailureHandler} receives
         * the record's class, id and error for processing.
         * </p>
         * 
         * @param onLoadFailureHandler
         * @return this builder
         */
        public Builder onLoadFailure(
                TriConsumer<Class<? extends Record>, Long, Throwable> onLoadFailureHandler) {
            this.onLoadFailureHandler = onLoadFailureHandler;
            return this;
        }

        /**
         * Set the connection's password.
         * 
         * @param password
         * @return this builder
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Set the connection's port.
         * 
         * @param port
         * @return this builder
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Set the {@link ReadStrategy} for the {@link Runway} instance.
         * <p>
         * The default {@link ReadStrategy} varies based on the
         * {@link #withCache(Cache) cache} setting.
         * </p>
         * 
         * @param readStrategy
         * @return this builder
         */
        public Builder readStrategy(ReadStrategy readStrategy) {
            this.readStrategy = readStrategy;
            return this;
        }

        /**
         * Set the maximum number of records that should be buffered in memory
         * when streaming data from the database. This is only relevant if the
         * {@link #readStrategy(ReadStrategy) read strategy} is not
         * {@link ReadStrategy#BULK}.
         * 
         * @param max
         * @return this builder
         * @deprecated use {@link #streamingReadBufferSize(int)} instead
         */
        @Deprecated
        public Builder recordsPerSelectBufferSize(int max) {
            return streamingReadBufferSize(max);
        }

        /**
         * Set the maximum number of records that should be buffered in memory
         * when streaming data from the database. This is only relevant if the
         * {@link #readStrategy(ReadStrategy) read strategy} is not
         * {@link ReadStrategy#BULK}.
         * 
         * @param max
         * @return this builder
         */
        public Builder streamingReadBufferSize(int max) {
            this.streamingReadBufferSize = max;
            return this;
        }

        /**
         * Set the connection's username.
         * 
         * @param username
         * @return this builder
         */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        /**
         * Set the connection's cache.
         * 
         * @param cache
         * @return this builder
         */
        public Builder withCache(Cache<Long, Map<String, Set<Object>>> cache) {
            this.cache = cache;
            return this;
        }
    }

    /**
     * The {@link ReadStrategy} determines how {@link Runway}
     * {@link Runway#read(Concourse, Criteria, Order, Page) reads} data from
     * Concourse in response to a request.
     *
     * @author Jeff Nelson
     */
    public enum ReadStrategy {
        /**
         * Select the {@link #BULK} or {@link #STREAM} strategy on a
         * read-by-read basis (usually depending upon which will return results
         * faster).
         */
        AUTO,

        /**
         * Use Concourse's {@code select} method to read all the data for all
         * the records that match a request, at once.
         */
        BULK,

        /**
         * Use Concourse's {@code find} method to find the ids of all the
         * records that match a request and incrementally read the data for
         * those records on-the-fly, as needed. When using this strategy,
         * further tuning is possible using
         * {@link Runway#Builder#streamingReadBufferSize(int)}.
         */
        STREAM
    }

    /**
     * Internal utility class for Database {@link Criteria} with support for
     * {@link Runway} specific semantics.
     *
     * @author Jeff Nelson
     */
    private static class $Criteria {

        /**
         * Utility method do ensure that the {@code criteria} is limited to
         * querying objects that belong to a specific {@code clazz} hierarchy.
         * 
         * @param criteria
         * @param parent class
         * 
         * @return the updated {@code criteria}
         */
        public static <T> Criteria accrossClassHierachy(Class<T> clazz,
                Criteria criteria) {
            return Criteria.where().group(forClassHierarchy(clazz)).and()
                    .group(criteria).build();
        }

        /**
         * Utility method to ensure that the {@code criteria} is limited to
         * records that exist in the {@code realms}.
         * 
         * @param realms
         * @param criteria
         * @return limiting {@link Criteria}
         */
        public static Criteria amongRealms(Realms realms, Criteria criteria) {
            if(realms.names().isEmpty()) {
                return criteria;
            }
            else {
                Iterator<String> it = realms.names().iterator();
                ValueState vs = Criteria.where().key(Record.REALMS_KEY)
                        .operator(Operator.EQUALS).value(it.next());
                while (it.hasNext()) {
                    vs.or().key(Record.REALMS_KEY).operator(Operator.EQUALS)
                            .value(it.next());
                }
                return Criteria.where().group(criteria).and().group(vs);
            }
        }

        /**
         * Return a {@link Criteria} to find records within {@code clazz}.
         * 
         * @param clazz
         * @return the {@link Criteria}
         */
        public static <T> Criteria forClass(Class<T> clazz) {
            return Criteria.where().key(Record.SECTION_KEY)
                    .operator(Operator.EQUALS).value(clazz.getName()).build();
        }

        /**
         * Return a {@link Criteria} to find records across the {@code clazz}
         * hierarchy.
         * 
         * @param clazz
         * @return the {@link Criteria}
         */
        @SuppressWarnings("rawtypes")
        public static <T> Criteria forClassHierarchy(Class<T> clazz) {
            Collection<Class<?>> hierarchy = hierarchies.get(clazz);
            BuildableState criteria = null;
            for (Class cls : hierarchy) {
                if(criteria == null) {
                    criteria = Criteria.where().key(Record.SECTION_KEY)
                            .operator(Operator.EQUALS).value(cls.getName());
                }
                else {
                    criteria.or().key(Record.SECTION_KEY)
                            .operator(Operator.EQUALS).value(cls.getName());
                }
            }
            return criteria.build();
        }

        /**
         * Utility method to ensure that the {@code criteria} is limited to
         * querying objects that belong to a specific {@code clazz}.
         * 
         * @param clazz
         * @param criteria
         * @return limiting {@link Criteria}
         */
        public static <T> Criteria withinClass(Class<T> clazz,
                Criteria criteria) {
            return Criteria.where().group(forClass(clazz)).and().group(criteria)
                    .build();
        }

    }
}
