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

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cinchapi.ccl.syntax.ConditionTree;
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
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.ValueState;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.cinchapi.concourse.server.plugin.util.Versions;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record.ConstraintViolationException;
import com.cinchapi.runway.Record.InvalidRecordException;
import com.cinchapi.runway.Record.Snapshot;
import com.cinchapi.runway.Record.StaticAnalysis;
import com.cinchapi.runway.cache.CachingConnectionPool;
import com.cinchapi.runway.util.Pagination;
import com.github.zafarkhaja.semver.Version;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
     * Return {@code true} if the given {@link Order} and {@link Page} indicate
     * that no sorting or pagination is requested, meaning the query can be
     * handled without client-side stream manipulation.
     *
     * @param order
     * @param page
     * @return {@code true} if neither sorting nor pagination is required
     */
    private static boolean doesNotRequireSortingOrPagination(Order order,
            Page page) {
        return order == null && page == null;
    }

    /**
     * Call
     * {@link Record#load(Class, long, TLongObjectMap, ConnectionPool, Runway, Map)}
     * and handle any errors with the {@link #onLoadFailureHandler}.
     *
     * @param <T>
     * @param clazz
     * @param id
     * @param loaded a {@link ConcurrentMap} used to track loaded {@link Record}
     *            references
     * @param connections
     * @param runway
     * @param data
     * @return the loaded {@link Record} instance
     */
    private static <T extends Record> T loadWithErrorHandling(Class<T> clazz,
            long id, ConcurrentMap<Long, Record> loaded,
            ConnectionPool connections, Runway runway,
            @Nullable Map<String, Set<Object>> data,
            @Nullable Map<Long, Map<String, Set<Object>>> targets) {
        try {
            return Record.load(clazz, id, loaded, connections, runway, data,
                    targets);
        }
        catch (Exception e) {
            if(e instanceof InvalidRecordException) {
                // For consistency with Audience framework, return "null" for
                // invalid records so that they are indistinguishable from valid
                // Records that are not visible to an Audience.
                return null;
            }
            else {
                if(e instanceof ConstraintViolationException) {
                    // Backwards compatibility for when constraint violations
                    // were noted via an IllegalStateException.
                    e = new IllegalStateException(e.getMessage());
                }
                runway.onLoadFailureHandler.accept(clazz, id, e);
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
    }

    /**
     * Restore the mutable metadata on each {@link Record} from a previously
     * captured snapshot.
     * <p>
     * This is used during spurious save failure retry to undo the side effects
     * that {@link Record#saveWithinTransaction(Concourse, Map, Map, boolean)
     * saveWithinTransaction} performs on metadata fields (checksum, realm
     * flags, author), since the transaction was aborted and none of those
     * mutations should persist.
     * </p>
     *
     * @param snapshot a mapping from {@link Record} to its captured
     *            {@link Record.Snapshot}
     */
    private static void restore(Map<Record, Record.Snapshot> snapshot) {
        for (Entry<Record, Record.Snapshot> entry : snapshot.entrySet()) {
            entry.getKey().restore(entry.getValue());
        }
    }

    /**
     * The maximum number of times a spurious save failure is retried before
     * giving up.
     */
    private static final int MAX_SPURIOUS_SAVE_RETRIES = 5;

    /**
     * The default {@link #onLoadFailureHandler}.
     */
    private static TriConsumer<Class<? extends Record>, Long, Throwable> DEFAULT_ON_LOAD_FAILURE_HANDLER = (
            clazz, record, error) -> record.toString();

    /**
     * A collection of all the active {@link Runway} instances.
     */
    private static Set<Runway> instances = Sets.newHashSet();

    /**
     * Placeholder for a {@code null} {@link Order} parameter.
     */
    private static Order NO_ORDER = null;

    /**
     * Placeholder for a {@code null} {@link Page} parameter.
     */
    private static Page NO_PAGINATION = null;

    /**
     * Placeholder for a {@code null} {@link Criteria} parameter.
     */
    private static Criteria NO_CRITERIA = null;

    static {
        // Perform static analysis on initialization.
        StaticAnalysis.instance();
    }

    /**
     * The {@link Page pagination} parameter to use to limit the number of
     * database records returned when trying to verify or enforce a uniqueness
     * constraint.
     */
    private static Page UNIQUE_PAGINATION = Page.sized(2);

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
     * Whenever an exception is thrown during a {@link Runway#load(long) load}
     * operation, the provided {@code onLoadFailureHandler} receives the
     * record's class, id and error for processing.
     */
    private TriConsumer<Class<? extends Record>, Long, Throwable> onLoadFailureHandler = DEFAULT_ON_LOAD_FAILURE_HANDLER;

    /**
     * The strategy for {@link #read(Concourse, Criteria, Order, Page) loading}
     * data from the database.
     */
    private ReadStrategy readStrategy = ReadStrategy.AUTO;

    /**
     * The strategy for handling spurious {@link TransactionException
     * TransactionExceptions} during {@link #save(Record...) save} operations.
     */
    private SpuriousSaveFailureStrategy spuriousSaveFailureStrategy = SpuriousSaveFailureStrategy.FAIL_FAST;

    /**
     * The maximum number of records to buffer in memory when selecting data
     * from the database. This is only relevant when the {@link #readStrategy}
     * is not {@link ReadStrategy#BULK}.
     */
    private int streamingReadBufferSize = 1000;

    /**
     * A flag that indicates if the connected server has enough functionality to
     * facilitate pre-selecting linked {@link Record Records}.
     * <p>
     * This functionality is supported in Concourse 0.11.3+
     * </p>
     * <p>
     * For diagnostic purposes, this value can be disabled, regardless of
     * Concourse version, using {@link Builder#disablePreSelectLinkedRecords()}.
     * </p>
     */
    private final boolean supportsPreSelectLinkedRecords;

    /**
     * The strategy for pre-selecting data for {@link Collection
     * Collection&lt;Record&gt;} fields.
     */
    private CollectionPreSelectStrategy collectionPreSelectStrategy = CollectionPreSelectStrategy.NONE;

    /**
     * A queue of records that have been successfully saved and are waiting for
     * save notification processing.
     */
    private BlockingQueue<Record> saveNotificationQueue;

    /**
     * An executor service dedicated to processing save notifications.
     */
    private ExecutorService saveNotificationExecutor;

    /**
     * The consumer that processes save notifications for records.
     */
    @Nullable
    private Consumer<Record> saveListener;

    /**
     * The cached {@link Gateway} instance that provides intelligent routing to
     * database operations. Lazily initialized when first accessed.
     */
    private Gateway gateway = null;

    /**
     * Thread-local storage for attached {@link AdHocDataSource} instances. Each
     * thread maintains its own set of attached sources, enabling request-scoped
     * or context-scoped attachment.
     * <p>
     * The set is lazily initialized only when {@link #attach} is called to
     * avoid unnecessary allocations for threads that never use attachment.
     * </p>
     */
    private final ThreadLocal<Set<AdHocDataSource<?>>> attached = new ThreadLocal<>();

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
                    .greaterThanOrEqualTo(target)
                    || actual.equals(
                            Versions.parseSemanticVersion("0.0.0-SNAPSHOT"));
            target = Version.forIntegers(0, 11, 3);
            this.supportsPreSelectLinkedRecords = actual
                    .greaterThanOrEqualTo(target)
                    || actual.equals(
                            Versions.parseSemanticVersion("0.0.0-SNAPSHOT"));
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Attach one or more {@link AdHocDataSource AdHocDataSources} to this
     * {@link Runway} instance for the current thread.
     * <p>
     * When a source is attached, queries for its {@link AdHocDataSource#type()
     * type} are routed to the source instead of Runway's underlying database.
     * This enables transparent federation of programmatic data sources
     * alongside persistent data.
     * </p>
     * <p>
     * The returned {@link DatabaseInterface} delegates to this {@link Runway}
     * instance and implements {@link AutoCloseable} to automatically detach all
     * sources when closed. Both the returned handle and this {@link Runway}
     * instance can be used for queries while sources are attached.
     * </p>
     * <p>
     * <strong>Note:</strong> Full-text {@link #search} operations are not
     * supported for attached sources. Search always queries the underlying
     * database. Use {@link #find} with appropriate {@link Criteria} for
     * filtering ad-hoc data.
     * </p>
     * <h2>Usage</h2>
     *
     * <pre>
     * {@code
     * try (DatabaseInterface db = runway.attach(source)) {
     *     db.load(MyAdHocRecord.class); // Uses attached source
     *     runway.load(MyAdHocRecord.class); // Also uses attached source
     * }
     * // Sources automatically detached
     * }
     * </pre>
     *
     * @param sources the {@link AdHocDataSource AdHocDataSources} to attach
     * @return a {@link DatabaseInterface} that auto-detaches on close
     */
    public AttachmentScope attach(AdHocDataSource<?>... sources) {
        Set<AdHocDataSource<?>> set = attached.get();
        if(set == null) {
            set = new LinkedHashSet<>();
            attached.set(set);
        }
        for (AdHocDataSource<?> source : sources) {
            set.add(source);
        }
        return new AttachmentScope(this, sources);
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
        if(saveNotificationExecutor != null) {
            saveNotificationExecutor.shutdownNow();
        }
    }

    @Override
    public <T extends Record> int count(Class<T> clazz, Criteria criteria,
            Realms realms) {
        Set<AdHocDataSource<?>> sources = getAttachedSources(clazz);
        if(!sources.isEmpty()) {
            int count = 0;
            for (AdHocDataSource<?> source : sources) {
                count += criteria == null ? source.count(clazz, realms)
                        : source.count(clazz, criteria, realms);
            }
            return count;
        }
        else if(criteria == null) {
            // No criteria means count all records of this class
            return $count(
                    $Criteria.amongRealms(realms, $Criteria.forClass(clazz)));
        }
        else if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
            return $count($Criteria.amongRealms(realms,
                    $Criteria.withinClass(clazz, criteria)));
        }
        else {
            return filter(clazz, criteria, NO_ORDER, NO_PAGINATION, realms)
                    .size();
        }
    }

    @Override
    public <T extends Record> int count(Class<T> clazz, Realms realms) {
        return count(clazz, NO_CRITERIA, realms);
    }

    @Override
    public <T extends Record> int countAny(Class<T> clazz, Criteria criteria,
            Realms realms) {
        Set<AdHocDataSource<?>> sources = getAttachedSourcesForHierarchy(clazz);
        if(!sources.isEmpty()) {
            int count = 0;
            for (AdHocDataSource<?> source : sources) {
                count += criteria == null ? source.countAny(clazz, realms)
                        : source.countAny(clazz, criteria, realms);
            }
            return count;
        }
        else if(criteria == null) {
            // No criteria means count all records in the class hierarchy
            return $count($Criteria.amongRealms(realms,
                    $Criteria.forClassHierarchy(clazz)));
        }
        else if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
            return $count($Criteria.amongRealms(realms,
                    $Criteria.accrossClassHierachy(clazz, criteria)));
        }
        else {
            return filterAny(clazz, criteria, NO_ORDER, NO_PAGINATION, realms)
                    .size();
        }
    }

    @Override
    public <T extends Record> int countAny(Class<T> clazz, Realms realms) {
        return countAny(clazz, NO_CRITERIA, realms);
    }

    /**
     * Detach an {@link AdHocDataSource} from this {@link Runway} instance for
     * the current thread.
     *
     * @param source the source to detach
     */
    public void detach(AdHocDataSource<?> source) {
        Set<AdHocDataSource<?>> set = attached.get();
        if(set != null) {
            set.remove(source);
        }
    }

    /**
     * Detach all {@link AdHocDataSource AdHocDataSources} for the given
     * {@link AdHocRecord} class from this {@link Runway} instance for the
     * current thread.
     *
     * @param clazz the {@link AdHocRecord} class whose sources should be
     *            detached
     */
    public void detach(Class<? extends AdHocRecord> clazz) {
        Set<AdHocDataSource<?>> set = attached.get();
        if(set != null) {
            set.removeIf(source -> source.type().equals(clazz));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        Set<AdHocDataSource<?>> sources = getAttachedSources(clazz);
        if(!sources.isEmpty()) {
            if(sources.size() == 1) {
                return sources.iterator().next().find(clazz, criteria, order,
                        page, realms);
            }
            else {
                Set<T> results = new LinkedHashSet<>();
                for (AdHocDataSource<?> source : sources) {
                    results.addAll(source.find(clazz, criteria, realms));
                }
                if(order != null) {
                    results = DatabaseInterface.sort(results,
                            backwardsCompatible(order));
                }
                if(page != null) {
                    results = results.stream().skip(page.skip())
                            .limit(page.limit()).collect(Collectors
                                    .toCollection(LinkedHashSet::new));
                }
                return results;
            }
        }
        else if(hasNativeSortingAndPagination
                || doesNotRequireSortingOrPagination(order, page)) {
            // When native sorting/pagination is supported OR no
            // sorting/pagination is requested, the database can handle the
            // query directly without client-side stream manipulation.
            if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                Concourse concourse = connections.request();
                try {
                    Map<Long, Map<String, Set<Object>>> data = $find(concourse,
                            clazz, criteria, order, page, realms);
                    return instantiateAll(clazz, data);
                }
                finally {
                    connections.release(concourse);
                }
            }
            else {
                return filter(clazz, criteria, order, page, realms);
            }
        }
        else {
            // Legacy servers lack native sorting/pagination, so results must
            // be fetched and processed client-side.
            Stream<T> stream = order != null
                    ? find(clazz, criteria, backwardsCompatible(order)).stream()
                    : find(clazz, criteria).stream();
            stream = stream.filter(record -> realms.names().isEmpty() || !Sets
                    .intersection(record.realms(), realms.names()).isEmpty());
            if(page != null) {
                stream = stream.skip(page.skip()).limit(page.limit());
            }
            return stream.collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return find(clazz, criteria, order, NO_PAGINATION, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return find(clazz, criteria, NO_ORDER, page, realms);
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return find(clazz, criteria, NO_ORDER, NO_PAGINATION, realms);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        Set<AdHocDataSource<?>> sources = getAttachedSourcesForHierarchy(clazz);
        if(!sources.isEmpty()) {
            if(sources.size() == 1) {
                return sources.iterator().next().findAny(clazz, criteria, order,
                        page, realms);
            }
            else {
                Set<T> results = new LinkedHashSet<>();
                for (AdHocDataSource<?> source : sources) {
                    results.addAll(source.findAny(clazz, criteria, realms));
                }
                if(order != null) {
                    results = DatabaseInterface.sort(results,
                            backwardsCompatible(order));
                }
                if(page != null) {
                    results = results.stream().skip(page.skip())
                            .limit(page.limit()).collect(Collectors
                                    .toCollection(LinkedHashSet::new));
                }
                return results;
            }
        }
        else if(hasNativeSortingAndPagination
                || doesNotRequireSortingOrPagination(order, page)) {
            // When native sorting/pagination is supported OR no
            // sorting/pagination is requested, the database can handle the
            // query directly without client-side stream manipulation.
            if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
                Concourse concourse = connections.request();
                try {
                    Map<Long, Map<String, Set<Object>>> data = $findAny(
                            concourse, clazz, criteria, order, page, realms);
                    return instantiateAll(data);
                }
                finally {
                    connections.release(concourse);
                }
            }
            else {
                return filterAny(clazz, criteria, order, page, realms);
            }
        }
        else {
            // Legacy servers lack native sorting/pagination, so results must
            // be fetched and processed client-side.
            Stream<T> stream = order != null
                    ? findAny(clazz, criteria, backwardsCompatible(order))
                            .stream()
                    : findAny(clazz, criteria).stream();
            stream = stream.filter(record -> realms.names().isEmpty() || !Sets
                    .intersection(record.realms(), realms.names()).isEmpty());
            if(page != null) {
                stream = stream.skip(page.skip()).limit(page.limit());
            }
            return stream.collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return findAny(clazz, criteria, order, NO_PAGINATION, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return findAny(clazz, criteria, NO_ORDER, page, realms);
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return findAny(clazz, criteria, NO_ORDER, NO_PAGINATION, realms);
    }

    @Override
    public <T extends Record> T findAnyUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        Set<T> results = findAny(clazz, criteria, NO_ORDER, UNIQUE_PAGINATION,
                realms);
        if(results.isEmpty()) {
            return null;
        }
        else if(results.size() == 1) {
            return results.iterator().next();
        }
        else {
            throw new DuplicateEntryException(
                    new com.cinchapi.concourse.thrift.DuplicateEntryException(
                            AnyStrings.format(
                                    "There are more than one records that match {} in the hierarchy of {}",
                                    criteria, clazz)));
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
        Set<T> results = find(clazz, criteria, NO_ORDER, UNIQUE_PAGINATION,
                realms);
        if(results.isEmpty()) {
            return null;
        }
        else if(results.size() == 1) {
            return results.iterator().next();
        }
        else {
            throw new DuplicateEntryException(
                    new com.cinchapi.concourse.thrift.DuplicateEntryException(
                            AnyStrings.format(
                                    "There are more than one records that match {} in {}",
                                    criteria, clazz)));
        }
    }

    @Override
    public Gateway gateway() {
        if(gateway == null) {
            gateway = DatabaseInterface.super.gateway();
        }
        return gateway;
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> T load(Class<T> clazz, long id, Realms realms) {
        Set<AdHocDataSource<?>> sources = getAttachedSources(clazz);
        if(!sources.isEmpty()) {
            for (AdHocDataSource<?> source : sources) {
                T result = source.load(clazz, id, realms);
                if(result != null) {
                    return result;
                }
            }
            return null;
        }
        else {
            Concourse connection = null;
            try {
                if(StaticAnalysis.instance().getClassHierarchy(clazz)
                        .size() > 1) {
                    // The provided clazz has descendants, so it is possible
                    // that the Record with the #id is actually a member of a
                    // subclass
                    connection = ensureValidConnection(connection);
                    String section = connection.get(Record.SECTION_KEY, id);
                    if(section != null) {
                        clazz = Reflection.getClassCasted(section);
                    }
                }
                if(!realms.names().isEmpty()) {
                    connection = ensureValidConnection(connection);
                    Set<String> $realms = MoreObjects.firstNonNull(
                            connection.select(Record.REALMS_KEY, id),
                            ImmutableSet.of());
                    if(Sets.intersection($realms, realms.names()).isEmpty()) {
                        return null; // TODO: what to do here?
                    }
                }
                Map<String, Set<Object>> data = null;
                Map<Long, Map<String, Set<Object>>> targets = null;
                if(collectionPreSelectStrategy == CollectionPreSelectStrategy.NAVIGATE) {
                    Set<String> navigatePaths = getNavigatePathsForClassIfSupported(
                            clazz);
                    if(navigatePaths != null) {
                        connection = ensureValidConnection(connection);
                        targets = connection.navigate(navigatePaths, id);
                    }
                }
                else if(collectionPreSelectStrategy == CollectionPreSelectStrategy.BULK_SELECT) {
                    connection = ensureValidConnection(connection);
                    Set<String> paths = getPathsForClassIfSupported(clazz);
                    data = paths != null ? connection.select(paths, id)
                            : connection.select(id);
                    Map<Long, Map<String, Set<Object>>> seed = Maps
                            .newHashMap();
                    seed.put(id, data);
                    targets = prefetchLinks(connection, seed);
                }
                return instantiate(clazz, id, data, targets);
            }
            finally {
                if(connection != null) {
                    connections.release(connection);
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Realms realms) {
        Set<AdHocDataSource<?>> sources = getAttachedSources(clazz);
        if(!sources.isEmpty()) {
            if(sources.size() == 1) {
                return sources.iterator().next().load(clazz, order, page,
                        realms);
            }
            else {
                Set<T> results = new LinkedHashSet<>();
                for (AdHocDataSource<?> source : sources) {
                    results.addAll(source.load(clazz, realms));
                }
                if(order != null) {
                    results = DatabaseInterface.sort(results,
                            backwardsCompatible(order));
                }
                if(page != null) {
                    results = results.stream().skip(page.skip())
                            .limit(page.limit()).collect(Collectors
                                    .toCollection(LinkedHashSet::new));
                }
                return results;
            }
        }
        else if(hasNativeSortingAndPagination
                || doesNotRequireSortingOrPagination(order, page)) {
            // When native sorting/pagination is supported OR no
            // sorting/pagination is requested, the database can handle the
            // query directly without client-side stream manipulation.
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
            // Legacy servers lack native sorting/pagination, so results must
            // be fetched and processed client-side.
            Stream<T> stream = order != null
                    ? load(clazz, backwardsCompatible(order)).stream()
                    : load(clazz).stream();
            stream = stream.filter(record -> realms.names().isEmpty() || !Sets
                    .intersection(record.realms(), realms.names()).isEmpty());
            if(page != null) {
                stream = stream.skip(page.skip()).limit(page.limit());
            }
            return stream.collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Realms realms) {
        return load(clazz, order, NO_PAGINATION, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Realms realms) {
        return load(clazz, NO_ORDER, page, realms);
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz, Realms realms) {
        return load(clazz, NO_ORDER, NO_PAGINATION, realms);
    }

    @SuppressWarnings("deprecation")
    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Page page, Realms realms) {
        Set<AdHocDataSource<?>> sources = getAttachedSourcesForHierarchy(clazz);
        if(!sources.isEmpty()) {
            if(sources.size() == 1) {
                return sources.iterator().next().loadAny(clazz, order, page,
                        realms);
            }
            else {
                Set<T> results = new LinkedHashSet<>();
                for (AdHocDataSource<?> source : sources) {
                    results.addAll(source.loadAny(clazz, realms));
                }
                if(order != null) {
                    results = DatabaseInterface.sort(results,
                            backwardsCompatible(order));
                }
                if(page != null) {
                    results = results.stream().skip(page.skip())
                            .limit(page.limit()).collect(Collectors
                                    .toCollection(LinkedHashSet::new));
                }
                return results;
            }
        }
        else if(hasNativeSortingAndPagination
                || doesNotRequireSortingOrPagination(order, page)) {
            // When native sorting/pagination is supported OR no
            // sorting/pagination is requested, the database can handle the
            // query directly without client-side stream manipulation.
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
            // Legacy servers lack native sorting/pagination, so results must
            // be fetched and processed client-side.
            Stream<T> stream = order != null
                    ? loadAny(clazz, backwardsCompatible(order)).stream()
                    : loadAny(clazz).stream();
            stream = stream.filter(record -> realms.names().isEmpty() || !Sets
                    .intersection(record.realms(), realms.names()).isEmpty());
            if(page != null) {
                stream = stream.skip(page.skip()).limit(page.limit());
            }
            return stream.collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Realms realms) {
        return loadAny(clazz, order, NO_PAGINATION, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Realms realms) {
        return loadAny(clazz, NO_ORDER, page, realms);
    }

    @Override
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Realms realms) {
        return loadAny(clazz, NO_ORDER, NO_PAGINATION, realms);
    }

    @Override
    public <T extends Record> T loadNullSafe(Class<T> clazz, long id,
            Realms realms) {
        try {
            return DatabaseInterface.super.loadNullSafe(clazz, id, realms);
        }
        catch (Exception e) {
            onLoadFailureHandler.accept(clazz, id, e);
            throw e;
        }
    }

    /**
     * Register a listener that will be called <strong>after</strong> any
     * {@link Record} of the specified {@code type} (or a subclass) is
     * successfully saved.
     *
     * @param type the {@link Record} type (or superclass) to listen for
     * @param listener a consumer that processes saved {@link Record Records} of
     *            the specified type
     * @return this for chaining
     * @deprecated Use {@link Properties#onSave(Class, Consumer)} via
     *             {@link #properties()} instead.
     */
    @Deprecated
    public <T extends Record> Runway onSave(Class<T> type,
            Consumer<T> listener) {
        properties().onSave(type, listener);
        return this;
    }

    /**
     * Register a listener that will be called <strong>after</strong> any
     * {@link Record} is successfully saved.
     *
     * @param listener a consumer that processes saved {@link Record Records}
     * @return this for chaining
     * @deprecated Use {@link Properties#onSave(Consumer)} via
     *             {@link #properties()} instead.
     */
    @Deprecated
    public Runway onSave(Consumer<Record> listener) {
        properties().onSave(Record.class, listener);
        return this;
    }

    /**
     * {@link Concourse#ping() Ping} the database and return {@code true} if it
     * is accessible.
     *
     * @return the database ping status
     */
    public boolean ping() {
        Concourse concourse = connections.request();
        try {
            return concourse.ping();
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Return the interface that exposes the properties of this {@link Runway}
     * instance.
     *
     * @return the {@link Properties}
     */
    public Properties properties() {
        return new Properties();
    }

    /**
     * Save all changes in the provided {@code records} using a single ACID
     * transaction.
     * <p>
     * All changes are committed atomically &mdash; either every {@link Record}
     * is persisted or none are. When the {@link SpuriousSaveFailureStrategy} is
     * {@link SpuriousSaveFailureStrategy#RETRY RETRY}, a
     * {@link TransactionException} that is not caused by actual data staleness
     * is automatically retried in a new transaction.
     * <p>
     * When {@code preventStaleWrites} is {@code true}, each {@link Record} in
     * the object graph is checked for staleness inside the transaction before
     * its data is written. If any {@link Record} has been externally modified
     * since it was last loaded or saved, a {@link StaleDataException} is thrown
     * and no data is persisted. This guarantees that a save will never silently
     * overwrite data that was changed by another process or transaction after
     * the {@link Record} was last synchronized. This is especially useful in
     * multi-writer environments where concurrent updates to the same
     * {@link Record Records} are possible.
     * <p>
     * <strong>NOTE:</strong> Enabling {@code preventStaleWrites} adds latency
     * because an audit query is issued for every {@link Record} in the object
     * graph before each write. For save operations that touch large object
     * graphs, this overhead may be significant. When disabled, saves are faster
     * but external modifications may be silently overwritten.
     *
     * @param preventStaleWrites if {@code true}, reject the save when any
     *            {@link Record} in the object graph has stale data
     * @param records one or more {@link Record Records} to save
     * @return {@code true} if all changes are atomically saved
     * @throws StaleDataException if {@code preventStaleWrites} is {@code true}
     *             and any {@link Record} has been externally modified
     */
    public boolean save(boolean preventStaleWrites, Record... records) {
        Concourse concourse = connections.request();
        Record current = null;
        try {
            boolean retrySpuriousSaveFailure = spuriousSaveFailureStrategy == SpuriousSaveFailureStrategy.RETRY;
            Map<Record, Snapshot> snapshots = retrySpuriousSaveFailure
                    ? new HashMap<>()
                    : null;
            Map<Record, Boolean> seen = new HashMap<>();
            int attempts = 0;
            while (true) {
                try {
                    seen.clear();
                    concourse.stage();
                    for (Record record : records) {
                        Supplier<Boolean> override = record.overrideSave();
                        if(override != null && !override.get()) {
                            // Early exit the entire transaction because an
                            // overriden save has failed.
                            concourse.abort();
                            return false;
                        }
                        else if(override != null) {
                            continue;
                        }
                        else {
                            current = record;
                            record.assign(this);
                            record.saveWithinTransaction(concourse, seen,
                                    snapshots, preventStaleWrites);
                        }
                    }
                    if(concourse.commit()) {
                        seen.entrySet().stream().filter(e -> e.getValue())
                                .map(e -> e.getKey()).forEach(record -> {
                                    enqueueSaveNotification(record);
                                    record.checkpoint();
                                });
                        return true;
                    }
                    else if(attempts > MAX_SPURIOUS_SAVE_RETRIES) {
                        return false;
                    }
                    else {
                        // Trigger catch block below for potential retry
                        throw new TransactionException();
                    }
                }
                catch (Throwable t) {
                    concourse.abort();
                    if(t instanceof TransactionException
                            && retrySpuriousSaveFailure
                            && ++attempts <= MAX_SPURIOUS_SAVE_RETRIES
                            && Arrays.stream(records).noneMatch(
                                    r -> r.hasStaleDataWithinTransaction(
                                            concourse))) {
                        // NOTE: Only root records are checked for stale data
                        // because linked records that are recursively saved may
                        // show false positives when concurrent saves share the
                        // same linked record.
                        restore(snapshots);
                        continue;
                    }
                    else if(t instanceof StaleDataException) {
                        throw (StaleDataException) t;
                    }
                    else {
                        for (Record record : seen.keySet()) {
                            if(record.inZombieState(concourse)) {
                                // TODO: this is currently disabled because
                                // zombie detection throughout the codebase is
                                // inconsistent and we may need to delete it all
                                // together
                                // concourse.clear(record.id());
                            }
                        }
                        if(current != null) {
                            current.errors.add(t);
                        }
                        return false;
                    }
                }
            }
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Save all changes in the provided {@code records} using a single ACID
     * transaction.
     * <p>
     * All changes are committed atomically &mdash; either every {@link Record}
     * is persisted or none are. When the {@link SpuriousSaveFailureStrategy} is
     * {@link SpuriousSaveFailureStrategy#RETRY RETRY}, a
     * {@link TransactionException} that is not caused by actual data staleness
     * is automatically retried in a new transaction.
     * </p>
     *
     * @param records one or more {@link Record Records} to save
     * @return {@code true} if all changes are atomically saved
     */
    public boolean save(Record... records) {
        return save(false, records);
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
     * Queue up a record for save notification processing.
     *
     * @param record the record that was saved
     */
    /* package */ final void enqueueSaveNotification(Record record) {
        if(saveListener != null) {
            saveNotificationQueue.offer(record);
        }
    }

    /**
     * If the {@link #collectionPreSelectStrategy} is
     * {@link CollectionPreSelectStrategy#NAVIGATE}, return the navigate paths
     * for {@code clazz} and all descendants.
     *
     * @param clazz
     * @return the navigate paths, or {@code null} if unsupported
     */
    final Set<String> getNavigatePathsForClassHierarchyIfSupported(
            Class<? extends Record> clazz) {
        return collectionPreSelectStrategy == CollectionPreSelectStrategy.NAVIGATE
                && StaticAnalysis.instance()
                        .hasCollectionRecordFieldInClassHierarchy(clazz)
                                ? StaticAnalysis.instance()
                                        .getNavigatePathsHierarchy(clazz)
                                : null;
    }

    /**
     * If the {@link #collectionPreSelectStrategy} is
     * {@link CollectionPreSelectStrategy#NAVIGATE}, return the navigate paths
     * for {@code clazz}.
     *
     * @param clazz
     * @return the navigate paths, or {@code null} if unsupported or the class
     *         has no {@link Collection Collection&lt;Record&gt;} fields
     */
    final Set<String> getNavigatePathsForClassIfSupported(
            Class<? extends Record> clazz) {
        return collectionPreSelectStrategy == CollectionPreSelectStrategy.NAVIGATE
                && StaticAnalysis.instance()
                        .hasCollectionRecordFieldInClass(clazz)
                                ? StaticAnalysis.instance()
                                        .getNavigatePaths(clazz)
                                : null;
    }

    /**
     * If this instance {@link #supportsPreSelectLinkedRecords} return the
     * {@link #PATHS_BY_CLASS_HIERARCHY} for {@code clazz}.
     *
     * @param clazz
     * @return the paths
     */
    final Set<String> getPathsForClassHierarchyIfSupported(
            Class<? extends Record> clazz) {
        return supportsPreSelectLinkedRecords && StaticAnalysis.instance()
                .hasFieldOfTypeRecordInClassHierarchy(clazz)
                        ? StaticAnalysis.instance().getPathsHierarchy(clazz)
                        : null;
    }

    /**
     * If this instance {@link #supportsPreSelectLinkedRecords} return the
     * {@link #PATHS_BY_CLASS} for {@code clazz}.
     *
     * @param clazz
     * @return the paths
     */
    final Set<String> getPathsForClassIfSupported(
            Class<? extends Record> clazz) {
        return supportsPreSelectLinkedRecords
                && StaticAnalysis.instance().hasFieldOfTypeRecordInClass(clazz)
                        ? StaticAnalysis.instance().getPaths(clazz)
                        : null;
    }

    /**
     * Load a record by {@code id} without knowing its class.
     *
     * @param id
     * @return the loaded record
     */
    <T extends Record> T load(long id) {
        return instantiate(id, null, null);
    }

    /**
     * Return the number of {@link Record records} that match the
     * {@code criteria}.
     *
     * @param criteria
     * @return the number of matching records
     */
    private int $count(Criteria criteria) {
        Concourse concourse = connections.request();
        try {
            return concourse.find(criteria).size();
        }
        finally {
            connections.release(concourse);
        }
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
        Set<String> paths = getPathsForClassIfSupported(clazz);
        return read(concourse, paths, criteria, order, page);
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
        Set<String> paths = getPathsForClassHierarchyIfSupported(clazz);
        return read(concourse, paths, criteria, order, page);
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
        Set<String> paths = getPathsForClassIfSupported(clazz);
        return read(concourse, paths, criteria, order, page);
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
        Set<String> paths = getPathsForClassHierarchyIfSupported(clazz);
        return read(concourse, paths, criteria, order, page);
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
        Collection<Class<?>> hierarchy = StaticAnalysis.instance()
                .getClassHierarchy(clazz);
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
     * Lazily initialize the save notification infrastructure (queue and
     * executor) if it has not already been set up. This allows {@link #onSave}
     * listeners to be registered after the {@link Runway} instance is built.
     */
    private synchronized void ensureSaveNotificationInfrastructure() {
        if(saveNotificationQueue == null) {
            saveNotificationQueue = new LinkedBlockingQueue<>();
            ThreadFactory threadFactory = r -> {
                Thread thread = new Thread(r,
                        "runway-save-notification-worker");
                thread.setDaemon(true);
                return thread;
            };
            saveNotificationExecutor = Executors
                    .newSingleThreadExecutor(threadFactory);
            saveNotificationExecutor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Record record = saveNotificationQueue.take();
                        try {
                            saveListener.accept(record);
                        }
                        catch (Exception e) {
                            // Silently swallow exceptions
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
        }
    }

    /**
     * Return a valid {@link Concourse} connection, reusing {@code connection}
     * if it is non-{@code null} or requesting a fresh one from the pool
     * otherwise.
     *
     * @param connection the existing connection to reuse, or {@code null} if
     *            none has been acquired yet
     * @return a non-{@code null} {@link Concourse} connection
     */
    private Concourse ensureValidConnection(Concourse connection) {
        return connection == null ? connections.request() : connection;
    }

    /**
     * Scan all values in {@code data} for {@link Link} instances whose targets
     * are not in {@code fetched}.
     *
     * @param data the data to scan
     * @param fetched record IDs already known
     * @return new target IDs to fetch
     */
    private Set<Long> extractLinkTargets(
            Map<Long, Map<String, Set<Object>>> data, Set<Long> fetched) {
        Set<Long> targets = new HashSet<>();
        for (Map<String, Set<Object>> record : data.values()) {
            for (Set<Object> values : record.values()) {
                for (Object value : values) {
                    if(value instanceof Link) {
                        long target = ((Link) value).longValue();
                        if(!fetched.contains(target)) {
                            targets.add(target);
                        }
                    }
                }
            }
        }
        return targets;
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
        ConcourseCompiler compiler = ConcourseCompiler.get();
        ConditionTree ast = (ConditionTree) compiler
                .parse($Criteria.amongRealms(realms, criteria));
        String[] keys = compiler.analyze(ast).keys()
                .toArray(Array.containing());
        Predicate<T> filter = record -> compiler.evaluate(ast,
                record.mmap(keys));
        if(page != null) {
            return Pagination.applyFilterAndPage(
                    $page -> order == null ? load(clazz, $page)
                            : load(clazz, order, $page),
                    filter, page);
        }
        else {
            Set<T> records = order == null ? load(clazz) : load(clazz, order);
            return records.stream().filter(filter)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
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
        ConcourseCompiler compiler = ConcourseCompiler.get();
        ConditionTree ast = (ConditionTree) compiler
                .parse($Criteria.amongRealms(realms, criteria));
        String[] keys = compiler.analyze(ast).keys()
                .toArray(Array.containing());
        Predicate<T> filter = record -> compiler.evaluate(ast,
                record.mmap(keys));
        if(page != null) {
            return Pagination
                    .applyFilterAndPage(
                            $page -> order == null ? loadAny(clazz, $page)
                                    : loadAny(clazz, order, $page),
                            filter, page);
        }
        else {
            Set<T> records = order == null ? loadAny(clazz)
                    : loadAny(clazz, order);
            return records.stream().filter(filter)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
    }

    /**
     * Return all {@link AdHocDataSource AdHocDataSources} attached for the
     * exact {@code clazz}.
     *
     * @param clazz the class to check
     * @return the attached sources (may be empty)
     */
    private <T extends Record> Set<AdHocDataSource<?>> getAttachedSources(
            Class<T> clazz) {
        Set<AdHocDataSource<?>> set = attached.get();
        if(set == null) {
            return ImmutableSet.of();
        }
        else {
            Set<AdHocDataSource<?>> sources = new LinkedHashSet<>();
            for (AdHocDataSource<?> source : set) {
                if(source.type().equals(clazz)) {
                    sources.add(source);
                }
            }
            return sources;
        }
    }

    /**
     * Return all {@link AdHocDataSource AdHocDataSources} attached for classes
     * in the hierarchy of {@code clazz}.
     * <p>
     * This method finds all attached sources whose class is assignable from the
     * requested class (i.e., sources that handle subclasses of the requested
     * class).
     * </p>
     *
     * @param clazz the class to check
     * @return the attached sources (may be empty)
     */
    private <T extends Record> Set<AdHocDataSource<?>> getAttachedSourcesForHierarchy(
            Class<T> clazz) {
        Set<AdHocDataSource<?>> set = attached.get();
        if(set == null) {
            return ImmutableSet.of();
        }
        else {
            Set<AdHocDataSource<?>> sources = new LinkedHashSet<>();
            for (AdHocDataSource<?> source : set) {
                if(clazz.isAssignableFrom(source.type())) {
                    sources.add(source);
                }
            }
            return sources;
        }
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and has the
     * specified {@code id}.
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
     * @param loaded
     * @param existing
     * @param data
     * @return the loaded {@link Record} instance
     */
    private <T extends Record> T instantiate(Class<T> clazz, long id,
            ConcurrentMap<Long, Record> loaded,
            @Nullable Map<String, Set<Object>> data,
            @Nullable Map<Long, Map<String, Set<Object>>> targets) {
        return loadWithErrorHandling(clazz, id, loaded, connections, this, data,
                targets);
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and has the
     * specified {@code id}.
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
            @Nullable Map<String, Set<Object>> data,
            @Nullable Map<Long, Map<String, Set<Object>>> targets) {
        return instantiate(clazz, id, new ConcurrentHashMap<>(), data, targets);
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and has the
     * specified {@code id}.
     * <p>
     * Unlike {@link #instantiate(Class, long, TLongObjectHashMap, Map)} this
     * method does not need to know the desired {@link Class} of the loaded
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
     * @param loaded
     * @param existing
     * @param data
     * @return the loaded {@link Record} instance
     */
    private <T extends Record> T instantiate(long id,
            ConcurrentMap<Long, Record> loaded,
            @Nullable Map<String, Set<Object>> data,
            @Nullable Map<Long, Map<String, Set<Object>>> targets) {
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
        return loadWithErrorHandling(clazz, id, loaded, connections, this, data,
                targets);
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and has the
     * specified {@code id}.
     * <p>
     * Unlike {@link #instantiate(Class, long, TLongObjectHashMap, Map)} this
     * method does not need to know the desired {@link Class} of the loaded
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
            @Nullable Map<String, Set<Object>> data,
            @Nullable Map<Long, Map<String, Set<Object>>> targets) {
        return instantiate(id, new ConcurrentHashMap<>(), data, targets);
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
        ConcurrentMap<Long, Record> loaded = new ConcurrentHashMap<>();
        Map<Long, Map<String, Set<Object>>> targets = resolveLinkCollections(
                clazz, data);
        Map<Long, Map<String, Set<Object>>> $targets = targets;
        Set<T> records = LazyTransformSet.of(data.entrySet(), entry -> {
            return instantiate(clazz, entry.getKey(), loaded, entry.getValue(),
                    $targets);
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
        AtomicReference<Map<Long, Map<String, Set<Object>>>> dests = new AtomicReference<>();
        ConcurrentMap<Long, Record> loaded = new ConcurrentHashMap<>();
        Set<T> records = LazyTransformSet.of(ids, id -> {
            if(data.get() == null) {
                Set<String> paths = getPathsForClassHierarchyIfSupported(clazz);
                data.set(stream(paths, ids));
                dests.set(resolveLinkCollectionsHierarchy(clazz, data.get(),
                        ids));
            }
            return instantiate(clazz, id, loaded, data.get().get(id),
                    dests.get());
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
        ConcurrentMap<Long, Record> loaded = new ConcurrentHashMap<>();
        Map<Long, Map<String, Set<Object>>> targets = resolveLinkCollections(
                null, data);
        Map<Long, Map<String, Set<Object>>> $targets = targets;
        Set<T> records = LazyTransformSet.of(data.entrySet(), entry -> {
            return instantiate(entry.getKey(), loaded, entry.getValue(),
                    $targets);
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
        AtomicReference<Map<Long, Map<String, Set<Object>>>> dests = new AtomicReference<>();
        ConcurrentMap<Long, Record> loaded = new ConcurrentHashMap<>();
        Set<T> records = LazyTransformSet.of(ids, id -> {
            if(data.get() == null) {
                data.set(stream(null, ids));
                dests.set(resolveLinkCollections(null, data.get()));
            }
            return instantiate(id, loaded, data.get().get(id), dests.get());
        });
        return records;
    }

    /**
     * Collect all {@link Record Records} reachable through {@link Link Links}
     * from the initial {@code data} by iteratively discovering and
     * batch-fetching targets.
     *
     * @param concourse the connection to use
     * @param data the initial data from the query; not mutated
     * @return a new map containing {@code data} plus all reachable linked
     *         record data
     */
    private Map<Long, Map<String, Set<Object>>> prefetchLinks(
            Concourse concourse, Map<Long, Map<String, Set<Object>>> data) {
        Map<Long, Map<String, Set<Object>>> pool = Maps.newHashMap(data);
        // BFS over the link graph: each iteration fetches one depth level.
        // #fetched tracks visited IDs so cycles in the link graph terminate
        // naturally.
        Set<Long> fetched = Sets.newHashSet(data.keySet());
        Set<Long> frontier = extractLinkTargets(data, fetched);
        while (!frontier.isEmpty()) {
            Map<Long, Map<String, Set<Object>>> batch = concourse
                    .select(frontier);
            pool.putAll(batch);
            fetched.addAll(frontier);
            frontier = extractLinkTargets(batch, fetched);
        }
        return pool;
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
            @Nullable Set<String> paths, Criteria criteria,
            @Nullable Order order, @Nullable Page page) {
        // Define the execution paths
        Function<Concourse, Map<Long, Map<String, Set<Object>>>> select;
        Function<Concourse, Set<Long>> find;
        if(order != null && page != null) {
            select = c -> paths != null ? c.select(paths, criteria, order, page)
                    : c.select(criteria, order, page);
            find = c -> c.find(criteria, order, page);
        }
        else if(order == null && page == null) {
            select = c -> paths != null ? c.select(paths, criteria)
                    : c.select(criteria);
            find = c -> c.find(criteria);
        }
        else if(order != null) {
            select = c -> paths != null ? c.select(paths, criteria, order)
                    : c.select(criteria, order);
            find = c -> c.find(criteria, order);
        }
        else { // page != null
            select = c -> paths != null ? c.select(paths, criteria, page)
                    : c.select(criteria, page);
            find = c -> c.find(criteria, page);
        }
        // Choose the execution path based on the #readStrategy
        Map<Long, Map<String, Set<Object>>> data;
        if(readStrategy == ReadStrategy.BULK) {
            data = select.apply(concourse);
        }
        else if(readStrategy == ReadStrategy.STREAM) {
            Set<Long> ids = find.apply(concourse);
            data = stream(paths, ids);
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
                    return stream(paths, ids);
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
     * Resolve destination {@link Record} data for {@link Collection
     * Collection&lt;Record&gt;} fields based on the configured
     * {@link #collectionPreSelectStrategy}.
     *
     * @param clazz the target class, or {@code null} for untyped loads
     *            (disqualifies {@link CollectionPreSelectStrategy#NAVIGATE})
     * @param data the initial query data
     * @return pre-fetched targets keyed by record ID, or {@code null} when the
     *         strategy is {@link CollectionPreSelectStrategy#NONE}
     */
    private Map<Long, Map<String, Set<Object>>> resolveLinkCollections(
            @Nullable Class<? extends Record> clazz,
            Map<Long, Map<String, Set<Object>>> data) {
        return resolveLinkedCollections(clazz, false, data.keySet(), data);
    }

    /**
     * Resolve destination {@link Record} data for {@link Collection
     * Collection&lt;Record&gt;} fields using class hierarchy navigation paths.
     *
     * @param clazz the target class, or {@code null} for untyped loads
     * @param data the initial query data
     * @param ids the record IDs for navigation
     * @return pre-fetched targets keyed by record ID, or {@code null}
     */
    private Map<Long, Map<String, Set<Object>>> resolveLinkCollectionsHierarchy(
            @Nullable Class<? extends Record> clazz,
            Map<Long, Map<String, Set<Object>>> data, Set<Long> ids) {
        return resolveLinkedCollections(clazz, true, ids, data);
    }

    /**
     * Dispatch link collection resolution based on the configured
     * {@link #collectionPreSelectStrategy}.
     * <p>
     * When {@code hierarchy} is {@code true}, navigate paths are resolved for
     * {@code clazz} and all its descendants; otherwise, only for {@code clazz}
     * itself.
     * </p>
     *
     * @param clazz the target class, or {@code null} for untyped loads
     *            (disqualifies {@link CollectionPreSelectStrategy#NAVIGATE})
     * @param hierarchy if {@code true}, resolve navigate paths across the full
     *            class hierarchy; if {@code false}, resolve for {@code clazz}
     *            alone
     * @param navigateIds the record IDs to pass to {@code navigate()}
     * @param data the initial query data (used by
     *            {@link CollectionPreSelectStrategy#BULK_SELECT BULK_SELECT} to
     *            discover {@link Link} targets)
     * @return pre-fetched targets keyed by record ID, or {@code null}
     */
    @SuppressWarnings("deprecation")
    private Map<Long, Map<String, Set<Object>>> resolveLinkedCollections(
            @Nullable Class<? extends Record> clazz, boolean hierarchy,
            Set<Long> navigateIds, Map<Long, Map<String, Set<Object>>> data) {
        if(data.isEmpty()) {
            return null;
        }
        Set<String> navigatePaths = null;
        if(clazz != null) {
            navigatePaths = hierarchy
                    ? getNavigatePathsForClassHierarchyIfSupported(clazz)
                    : getNavigatePathsForClassIfSupported(clazz);
        }
        if(navigatePaths != null) {
            Concourse concourse = connections.request();
            try {
                return concourse.navigate(navigatePaths, navigateIds);
            }
            finally {
                connections.release(concourse);
            }
        }
        else if(collectionPreSelectStrategy == CollectionPreSelectStrategy.BULK_SELECT) {
            Concourse concourse = connections.request();
            try {
                return prefetchLinks(concourse, data);
            }
            finally {
                connections.release(concourse);
            }
        }
        else {
            // NONE — each linked record is fetched individually
            return null;
        }
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
    private Map<Long, Map<String, Set<Object>>> stream(
            @Nullable Set<String> paths, Set<Long> ids) {
        // The data for the ids is asynchronously selected in the background in
        // a manner that staggers/buffers the amount of data by only selecting
        // {@link #recordsPerSelectBufferSize} from the database at a time.
        return new AbstractMap<Long, Map<String, Set<Object>>>() {

            /**
             * The cached {@link #entrySet()}.
             */
            Set<Entry<Long, Map<String, Set<Object>>>> entrySet = null;

            /**
             * The data that has been loaded from the data into memory. For the
             * items that have been pulled from the {@link #pending} queue.
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
             * A FIFO list of record ids that are pending database selection.
             * Items from this queue are popped off in increments of
             * {@value #BULK_SELECT_BUFFER_SIZE} and selected from Concourse.
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
                                loaded.putAll(paths != null
                                        ? concourse.select(paths, records)
                                        : concourse.select(records));
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
        private boolean disablePreSelectLinkedRecords = false;
        private List<Map.Entry<Class<? extends Record>, Consumer<? extends Record>>> saveListeners = new ArrayList<>();
        private SpuriousSaveFailureStrategy spuriousSaveFailureStrategy = SpuriousSaveFailureStrategy.FAIL_FAST;
        private CollectionPreSelectStrategy collectionPreSelectStrategy = null;

        /**
         * Build the configured {@link Runway} and return the instance.
         *
         * @return a {@link Runway} instance
         */
        @SuppressWarnings("unchecked")
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
            db.spuriousSaveFailureStrategy = spuriousSaveFailureStrategy;
            if(onLoadFailureHandler != null) {
                db.onLoadFailureHandler = onLoadFailureHandler;
            }
            if(disablePreSelectLinkedRecords) {
                Reflection.set("supportsPreSelectLinkedRecords", false, db); // (authorized)
                collectionPreSelectStrategy = CollectionPreSelectStrategy.NONE;
            }
            if(collectionPreSelectStrategy != null) {
                db.collectionPreSelectStrategy = collectionPreSelectStrategy;
            }

            // Initialize save notification components if a listener is provided
            if(!saveListeners.isEmpty()) {
                List<Entry<Class<? extends Record>, Consumer<? extends Record>>> listeners = new ArrayList<>(
                        saveListeners);
                db.saveListener = record -> {
                    for (Entry<Class<? extends Record>, Consumer<? extends Record>> entry : listeners) {
                        if(entry.getKey().isAssignableFrom(record.getClass())) {
                            try {
                                Consumer<Record> consumer = (Consumer<Record>) entry
                                        .getValue();
                                consumer.accept(record);
                            }
                            catch (Exception e) {
                                // Swallow and continue to next matching
                                // listener
                            }
                        }
                    }
                };
                db.saveNotificationQueue = new LinkedBlockingQueue<>();
                ThreadFactory threadFactory = r -> {
                    Thread thread = new Thread(r,
                            "runway-save-notification-worker");
                    thread.setDaemon(true);
                    return thread;
                };
                db.saveNotificationExecutor = Executors
                        .newSingleThreadExecutor(threadFactory);
                db.saveNotificationExecutor.submit(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Record record = db.saveNotificationQueue.take();
                            try {
                                db.saveListener.accept(record);
                            }
                            catch (Exception e) {
                                // Silently swallow exceptions from the
                                // composed listener
                            }
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                });
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
         * Set the strategy for pre-selecting data for {@link Collection
         * Collection&lt;Record&gt;} fields.
         * <p>
         * The default is {@link CollectionPreSelectStrategy#NONE}.
         * </p>
         *
         * @param strategy the {@link CollectionPreSelectStrategy} to use
         * @return this builder
         */
        public Builder collectionPreSelectStrategy(
                CollectionPreSelectStrategy strategy) {
            this.collectionPreSelectStrategy = strategy;
            return this;
        }

        /**
         * Disable the "pre-select" feature that improves performance by
         * selecting data for linked records instead of making multiple database
         * roundtrips. Generally speaking, it is never advised to disable
         * pre-select, but this option exists for debugging the behaviour of
         * reading using the new functionality vs the legacy method.
         *
         * @return this builder
         */
        public Builder disablePreSelectLinkedRecords() {
            this.disablePreSelectLinkedRecords = true;
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
         * Provide a listener that will be called <strong>after</strong> a
         * record of the specified {@code type} (or any subclass) is
         * successfully saved.
         * <p>
         * Save listening is designed for implementing side-effects that occur
         * after a record is successfully persisted to the database. This is
         * ideal for operations such as:
         * <ul>
         * <li>Triggering notifications or events</li>
         * <li>Updating external systems</li>
         * <li>Logging or auditing changes</li>
         * <li>Performing asynchronous tasks that depend on the record being
         * saved</li>
         * </ul>
         * </p>
         * <p>
         * The {@code listener} is only invoked for records that are instances
         * of {@code type} (including subclasses). For example, registering a
         * listener for {@code Player.class} will fire for {@code Player}
         * records and any subclass of {@code Player}, but not for unrelated
         * {@link Record} types.
         * </p>
         * <p>
         * This method is <strong>compositional</strong>: calling it multiple
         * times adds additional listeners rather than replacing previous ones.
         * All matching listeners fire in registration order. If a listener
         * throws an exception, it is caught and suppressed, and subsequent
         * matching listeners still fire.
         * </p>
         * <p>
         * The listener is executed asynchronously in a dedicated thread to
         * prevent blocking the main application flow.
         * </p>
         * <p>
         * <strong>Important:</strong> Save listeners should not modify the
         * state of the saved record. If you need to modify a record during the
         * save process, use the {@link Record#beforeSave} hook instead, which
         * is called before the record is persisted.
         * </p>
         *
         * @param type the {@link Record} type (or superclass) to listen for
         * @param listener a consumer that processes saved records of the
         *            specified type
         * @return this builder
         */
        public <T extends Record> Builder onSave(Class<T> type,
                Consumer<T> listener) {
            saveListeners.add(new SimpleImmutableEntry<>(type, listener));
            return this;
        }

        /**
         * Provide a listener that will be called <strong>after</strong> any
         * record is successfully saved.
         * <p>
         * This is equivalent to calling {@link #onSave(Class, Consumer)
         * onSave(Record.class, listener)}.
         * </p>
         * <p>
         * This method is <strong>compositional</strong>: calling it multiple
         * times adds additional listeners rather than replacing previous ones.
         * All matching listeners fire in registration order.
         * </p>
         *
         * @param listener a consumer that processes saved records
         * @return this builder
         */
        public Builder onSave(Consumer<Record> listener) {
            return onSave(Record.class, listener);
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
         * Set the {@link SpuriousSaveFailureStrategy} for the {@link Runway}
         * instance.
         * <p>
         * The default is {@link SpuriousSaveFailureStrategy#FAIL_FAST}, which
         * immediately propagates any {@code TransactionException} during a
         * {@link Runway#save(Record...) save}.
         * </p>
         * <p>
         * Setting this to {@link SpuriousSaveFailureStrategy#RETRY} causes
         * {@link Runway} to automatically retry a failed save when none of the
         * involved {@link Record Records} have stale data, which indicates that
         * the {@code TransactionException} was spurious.
         * </p>
         *
         * @param strategy the {@link SpuriousSaveFailureStrategy} to use
         * @return this builder
         */
        public Builder spuriousSaveFailureStrategy(
                SpuriousSaveFailureStrategy strategy) {
            this.spuriousSaveFailureStrategy = strategy;
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
     * Properties about this {@link Runway} instance that support post-build
     * configuration and inspection.
     *
     * @author Jeff Nelson
     */
    public class Properties {

        /**
         * Return the current {@link CollectionPreSelectStrategy} for this
         * {@link Runway} instance.
         *
         * @return the {@link CollectionPreSelectStrategy}
         */
        public CollectionPreSelectStrategy collectionPreSelectStrategy() {
            return collectionPreSelectStrategy;
        }

        /**
         * Set the {@link CollectionPreSelectStrategy} for this {@link Runway}
         * instance.
         *
         * @param strategy the {@link CollectionPreSelectStrategy} to use
         * @return this {@link Properties} for chaining
         */
        public Properties collectionPreSelectStrategy(
                CollectionPreSelectStrategy strategy) {
            collectionPreSelectStrategy = strategy;
            return this;
        }

        /**
         * Register a listener that will be called <strong>after</strong> any
         * {@link Record} of the specified {@code type} (or a subclass) is
         * successfully saved.
         * <p>
         * The new listener is chained with any previously registered listeners
         * &mdash; it does not replace them.
         * </p>
         *
         * @param type the {@link Record} type (or superclass) to listen for
         * @param listener a consumer that processes saved {@link Record
         *            Records} of the specified type
         * @return this {@link Properties} for chaining
         */
        @SuppressWarnings("unchecked")
        public <T extends Record> Properties onSave(Class<T> type,
                Consumer<T> listener) {
            ensureSaveNotificationInfrastructure();
            Consumer<Record> previous = saveListener;
            saveListener = record -> {
                if(type.isAssignableFrom(record.getClass())) {
                    try {
                        ((Consumer<Record>) (Consumer<?>) listener)
                                .accept(record);
                    }
                    catch (Exception e) {
                        // Swallow to match builder behavior
                    }
                }
                if(previous != null) {
                    previous.accept(record);
                }
            };
            return this;
        }

        /**
         * Register a listener that will be called <strong>after</strong> any
         * {@link Record} is successfully saved.
         * <p>
         * This is equivalent to calling {@link #onSave(Class, Consumer)
         * onSave(Record.class, listener)}.
         * </p>
         *
         * @param listener a consumer that processes saved {@link Record
         *            Records}
         * @return this {@link Properties} for chaining
         */
        public Properties onSave(Consumer<Record> listener) {
            return onSave(Record.class, listener);
        }

        /**
         * Return {@code true} if this {@link Runway} client and the underlying
         * {@link Concourse} deployment allow linked records to be pre-selected.
         *
         * @return a boolean that indicates if pre-selection is supported
         */
        public boolean supportsPreSelectLinkedRecords() {
            return supportsPreSelectLinkedRecords;
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
        public static <T extends Record> Criteria accrossClassHierachy(
                Class<T> clazz, Criteria criteria) {
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
        public static <T extends Record> Criteria forClassHierarchy(
                Class<T> clazz) {
            Collection<Class<?>> hierarchy = StaticAnalysis.instance()
                    .getClassHierarchy(clazz);
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
