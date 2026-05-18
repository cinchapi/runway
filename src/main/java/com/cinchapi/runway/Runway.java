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

import static com.cinchapi.runway.DatabaseInterface.duplicateEntryException;

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
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.common.concurrent.JoinableExecutorService;
import com.cinchapi.common.function.TriConsumer;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.BuildableState;
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
import com.cinchapi.runway.db.BatchReader;
import com.cinchapi.runway.db.BatchSaver;
import com.cinchapi.runway.db.IncrementalReader;
import com.cinchapi.runway.db.IncrementalSaver;
import com.cinchapi.runway.db.Pending;
import com.cinchapi.runway.db.Reader;
import com.cinchapi.runway.db.Saver;
import com.cinchapi.runway.util.Obligations;
import com.cinchapi.runway.util.Pagination;
import com.github.zafarkhaja.semver.Version;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
 * <h2>Ad Hoc Data Sources</h2>
 * <p>
 * An {@link AdHocDataSource} can be {@link #attach(AdHocDataSource) attached}
 * to a {@link Runway} to transparently contribute records to {@link #find} and
 * {@link #load} results alongside those stored in the database. Attached
 * sources remain active until they are {@link #detach(AdHocDataSource)
 * detached}.
 * </p>
 *
 * <h2>Reserving Query Results</h2>
 * <p>
 * {@link Runway} supports pre-fetching query results for later consumption on
 * the same thread. Call {@link #reserve()} to open a reservation scope, then
 * execute {@link Selection Selections} via
 * {@link #select(Selection, Selection...)}. The results are cached so that
 * subsequent {@link #find} and {@link #load} calls with matching query
 * signatures return the cached data instead of querying the database again.
 * Call {@link #unreserve()} to release the cached data when the reservation
 * scope ends.
 * </p>
 *
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
     * Return all known {@link Record} types.
     *
     * @return a {@link Set} containing all the known {@link Record} subclasses
     */
    public static Set<Class<? extends Record>> getKnownRecordTypes() {
        return Record.StaticAnalysis.instance().types();
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
     * Build the database {@link Criteria} for a single {@link Selection} to be
     * used in a combined multi-select query.
     *
     * @param selection the {@link Selection}
     * @return the {@link Criteria}
     */
    private static Criteria buildSelectionCriteria(
            DatabaseSelection<?> selection) {
        Criteria base;
        if(selection instanceof LoadRecordSelection) {
            long id = ((LoadRecordSelection<?>) selection).id;
            base = Criteria.where().key(Record.IDENTIFIER_KEY)
                    .operator(Operator.EQUALS).value(id).build();
        }
        else if(selection instanceof FindSelection) {
            Criteria criteria = ((FindSelection<?>) selection).criteria;
            base = selection.any
                    ? $Criteria.accrossClassHierachy(selection.clazz, criteria)
                    : $Criteria.withinClass(selection.clazz, criteria);
        }
        else {
            base = selection.any ? $Criteria.forClassHierarchy(selection.clazz)
                    : $Criteria.forClass(selection.clazz);
        }
        return $Criteria.amongRealms(selection.realms, base);
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
     * that {@link Record#saveWithinTransaction saveWithinTransaction} performs
     * on metadata fields (checksum, realm flags, author), since the transaction
     * was aborted and none of those mutations should persist.
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
     * Return {@code true} if a server running {@code actual} provides a feature
     * gated on {@code target} &mdash; either {@code actual} is at least
     * {@code target}, or it is the {@link #DEVELOPMENT_VERSION}, which is
     * treated as providing every feature.
     * 
     * @param actual the connected server's {@link Version}
     * @param target the minimum {@link Version} that provides the feature
     *
     * @return {@code true} if {@code actual} provides the feature
     */
    private static boolean isActualVersionGreaterThanOrEquals(Version actual,
            Version target) {
        return actual.greaterThanOrEqualTo(target)
                || actual.equals(DEVELOPMENT_VERSION);
    }

    /**
     * The maximum number of times a spurious save failure is retried before
     * giving up.
     */
    private static final int MAX_SPURIOUS_SAVE_RETRIES = 5;

    /**
     * The development {@link Version} sentinel; a server reporting this version
     * is treated as providing every feature, regardless of the {@link Version}
     * a given feature is gated on.
     */
    private static final Version DEVELOPMENT_VERSION = Versions
            .parseSemanticVersion("0.0.0-SNAPSHOT");

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

    static {
        // Perform static analysis on initialization.
        StaticAnalysis.instance();
    }

    /**
     * A connection pool to the underlying Concourse database.
     */
    /* package */ final ConnectionPool connections;

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
     * The strategy for loading data from the database.
     */
    private ReadStrategy readStrategy = ReadStrategy.BULK;

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
     */
    private final boolean supportsPreSelectLinkedRecords;

    /**
     * A flag that indicates if the connected server supports native
     * {@code count} calculations using the {@code $id$} identifier key, which
     * efficiently counts matching {@link Record Records} without transferring
     * all their IDs.
     * <p>
     * This functionality is supported in Concourse 0.12.2+
     * </p>
     */
    private final boolean supportsNativeCount;

    /**
     * A flag that indicates if the connected server supports the
     * {@code prepare()}/{@code submit()} Command API for batched round trips.
     * <p>
     * This functionality is supported in Concourse 1.0.0+
     * </p>
     */
    private final boolean supportsBulkCommands;

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
    @SuppressWarnings("deprecation")
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
     * Thread-local cache of pre-fetched query results. When
     * {@link #select(Selection, Selection...)} executes selections, results are
     * stored here keyed by their query signature. Subsequent calls to
     * {@link #find} or {@link #load} check the reserve before going to the
     * database. Initialized by {@link #reserve()} and cleared by
     * {@link #unreserve()}.
     */
    private final ThreadLocal<Map<Reservation, Object>> reservations = new ThreadLocal<>();

    /**
     * The executor for running isolated selections concurrently.
     */
    private final JoinableExecutorService selector;

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
            Version actual = Versions
                    .parseSemanticVersion(concourse.getServerVersion());
            this.hasNativeSortingAndPagination = isActualVersionGreaterThanOrEquals(
                    actual, Version.forIntegers(0, 10));
            this.supportsPreSelectLinkedRecords = isActualVersionGreaterThanOrEquals(
                    actual, Version.forIntegers(0, 11, 3));
            this.supportsNativeCount = isActualVersionGreaterThanOrEquals(
                    actual, Version.forIntegers(0, 12, 2));
            this.supportsBulkCommands = isActualVersionGreaterThanOrEquals(
                    actual, Version.forIntegers(1, 0, 0));
        }
        finally {
            connections.release(concourse);
        }
        this.selector = JoinableExecutorService
                .create(Runtime.getRuntime().availableProcessors());
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
        Obligations.runAll(() -> {
            if(!connections.isClosed()) {
                connections.close();
            }
        }, () -> {
            instances.remove(this);
            if(instances.size() == 1) {
                Record.PINNED_RUNWAY_INSTANCE = instances.iterator().next();
            }
            else {
                Record.PINNED_RUNWAY_INSTANCE = null;
            }
        }, () -> {
            selector.shutdownNow();
        }, () -> {
            if(saveNotificationExecutor != null) {
                saveNotificationExecutor.shutdownNow();
            }
        });
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

    @SuppressWarnings("deprecation")
    @Override
    public Gateway gateway() {
        if(gateway == null) {
            gateway = DatabaseInterface.super.gateway();
        }
        return gateway;
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
     * Activate the thread-local reservation cache. When active,
     * {@link #select(Selection...)} calls cache their results so that
     * subsequent reads &mdash; {@link #find}, {@link #count}, {@link #load},
     * and reads through the {@link Audience} framework &mdash; return the
     * cached data instead of querying the database.
     * <p>
     * The reservation cache is separate from the {@link #select(Selection...)}
     * API itself. {@code select()} is independently useful for grouping and
     * combining multiple queries into fewer database round trips. Calling
     * {@code reserve()} adds a caching layer on top: pre-fetched results are
     * stored and made available to later reads within the same thread.
     * <p>
     * A typical usage pattern in HTTP middleware:
     * <ol>
     * <li>Call {@code reserve()} at the start of a request.</li>
     * <li>Call {@code select(...)} to pre-fetch data the route handler will
     * need.</li>
     * <li>The handler calls {@code find()}, {@code count()}, or {@code load()}
     * &mdash; directly or through an {@link Audience} &mdash; and receives
     * cached results.</li>
     * <li>Call {@link #unreserve()} at the end of the request to clear the
     * cache.</li>
     * </ol>
     * <p>
     * If a reserve already exists on the current thread, it is cleared and
     * replaced.
     */
    public void reserve() {
        reservations.set(new HashMap<>());
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
            // NOTE: Snapshots are taken on every save because
            // saveWithinTransaction mutates Record metadata (__checksum,
            // _hasModifiedRealms, _author) before saver.commit() runs. If
            // a queued validator throws inside commit(), the in-memory
            // state must be rolled back so a subsequent save() of the
            // same Record still observes hasUnsavedChanges() and writes
            // the record's fields.
            Map<Record, Snapshot> snapshots = new HashMap<>();
            Map<Record, Boolean> seen = new HashMap<>();
            int attempts = 0;
            while (true) {
                Saver saver = supportsBulkCommands ? new BatchSaver(concourse)
                        : new IncrementalSaver(concourse);
                try {
                    seen.clear();
                    saver.stage();
                    for (Record record : records) {
                        Supplier<Boolean> override = record.overrideSave();
                        if(override != null && !override.get()) {
                            // Early exit the entire transaction because an
                            // overriden save has failed.
                            saver.abort();
                            return false;
                        }
                        else if(override != null) {
                            continue;
                        }
                        else {
                            current = record;
                            record.assign(this);
                            record.saveWithinTransaction(saver, seen, snapshots,
                                    preventStaleWrites);
                        }
                    }
                    if(saver.commit()) {
                        seen.entrySet().stream().filter(e -> e.getValue())
                                .map(e -> e.getKey()).forEach(record -> {
                                    enqueueSaveNotification(record);
                                    record.checkpoint();
                                });
                        return true;
                    }
                    else if(attempts > MAX_SPURIOUS_SAVE_RETRIES) {
                        restore(snapshots);
                        return false;
                    }
                    else {
                        // Trigger catch block below for potential retry
                        throw new TransactionException();
                    }
                }
                catch (Throwable t) {
                    saver.abort();
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
                        restore(snapshots);
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
                        restore(snapshots);
                        // A deferred Unique check throws from commit() after
                        // the loop advances #current, so blame the Record
                        // the violation names rather than the last one.
                        Record named = null;
                        if(t instanceof ConstraintViolationException) {
                            named = ((ConstraintViolationException) t).record();
                        }
                        Record offender = named != null ? named : current;
                        if(offender != null) {
                            offender.errors.add(t);
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
        Set<Long> ids;
        Concourse concourse = connections.request();
        try {
            ids = $search(concourse, clazz, query, keys);
        }
        finally {
            connections.release(concourse);
        }
        Map<Long, Map<String, Set<Object>>> data = stream(
                getPathsForClassHierarchyIfSupported(clazz), ids);
        Map<Long, Map<String, Set<Object>>> targets = prefetchLinkTargets(
                getNavigatePathsForClassHierarchyIfSupported(clazz), ids, data);
        return instantiateAll(clazz, data, targets);
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
        Set<Long> ids;
        Concourse concourse = connections.request();
        try {
            ids = $searchAny(concourse, clazz, query, keys);
        }
        finally {
            connections.release(concourse);
        }
        Map<Long, Map<String, Set<Object>>> data = stream(
                getPathsForClassHierarchyIfSupported(clazz), ids);
        Map<Long, Map<String, Set<Object>>> targets = prefetchLinkTargets(
                getNavigatePathsForClassHierarchyIfSupported(clazz), ids, data);
        return instantiateAll(data, targets);
    }

    @Override
    public Selections select(Selection<?>... options) {
        Preconditions.checkArgument(options.length > 0);
        DatabaseSelection<?>[] selections = Arrays.stream(options)
                .peek(option -> Preconditions.checkState(
                        option.state() == Selection.State.PENDING
                                || option.state() == Selection.State.RESOLVED,
                        "Selection has already been submitted"))
                .map(DatabaseSelection::resolve)
                .toArray(DatabaseSelection[]::new);
        if(selections.length == 1) {
            DatabaseSelection<?> selection = selections[0];
            if(selection.state == Selection.State.RESOLVED) {
                selection.setState(Selection.State.FINISHED);
            }
            else {
                try (Reader reader = supportsBulkCommands
                        ? new BatchReader(connections)
                        : new IncrementalReader(connections)) {
                    $selectWithPossibleSources(reader, selection, null);
                    reader.drain();
                }
                reserve(selection);
            }
            return new Selections(selections);
        }
        else {
            List<DatabaseSelection<?>> unique = Arrays.stream(selections)
                    .map(SelectionKey::new).distinct().map(key -> key.selection)
                    .collect(Collectors.toList());
            if(supportsBulkCommands) {
                List<DatabaseSelection<?>> dispatched = new ArrayList<>();
                try (Reader reader = new BatchReader(connections)) {
                    for (DatabaseSelection<?> selection : unique) {
                        if(selection.state == Selection.State.RESOLVED) {
                            selection.setState(Selection.State.FINISHED);
                        }
                        else {
                            $selectWithPossibleSources(reader, selection, null);
                            dispatched.add(selection);
                        }
                    }
                    reader.drain();
                }
                for (DatabaseSelection<?> selection : dispatched) {
                    reserve(selection);
                }
            }
            else {
                List<DatabaseSelection<?>> isolated = new ArrayList<>();
                List<DatabaseSelection<?>> combinable = new ArrayList<>();
                Set<String> combinedClasses = Sets.newHashSet();
                outer: for (DatabaseSelection<?> selection : unique) {
                    if(selection.state == Selection.State.RESOLVED) {
                        selection.setState(Selection.State.FINISHED);
                        continue outer; /* (authorized short circuit) */
                    }
                    // NOTE: Must manually attempt to recall here because it
                    // won't register as cached when dispatching route if a
                    // combination occurs and gets dispatched
                    Object cached = recallAndPossiblyFilter(selection);
                    if(cached != null) {
                        selection.setResult(cached);
                        selection.setState(Selection.State.FINISHED);
                        continue outer;
                    }
                    Set<AdHocDataSource<?>> sources = selection.any
                            ? getAttachedSourcesForHierarchy(selection.clazz)
                            : getAttachedSources(selection.clazz);
                    if(!sources.isEmpty()) {
                        try (Reader reader = new IncrementalReader(
                                connections)) {
                            $selectWithPossibleSources(reader, selection,
                                    sources);
                            reader.drain();
                        }
                        reserve(selection);
                        continue outer;
                    }
                    else if(selection.isCombinable()) {
                        // NOTE: #demux partitions combined results by class
                        // name only, so same-class selections with different
                        // criteria would each receive the union. Isolate
                        // conflicting ones to ensure correct per-criteria
                        // filtering.
                        Set<String> classes = selection.any
                                ? StaticAnalysis.instance()
                                        .getClassHierarchy(selection.clazz)
                                        .stream().map(Class::getName)
                                        .collect(Collectors.toSet())
                                : ImmutableSet.of(selection.clazz.getName());
                        boolean conflict = false;
                        for (String clazz : classes) {
                            if(combinedClasses.contains(clazz)) {
                                conflict = true;
                                break;
                            }
                        }
                        if(!conflict) {
                            combinedClasses.addAll(classes);
                            combinable.add(selection);
                            continue outer;
                        }
                    }
                    isolated.add(selection);
                }
                BuildableState combined = null;
                for (DatabaseSelection<?> selection : combinable) {
                    selection.ensurePending();
                    Criteria criteria = buildSelectionCriteria(selection);
                    combined = combined == null
                            ? Criteria.where().group(criteria)
                            : combined.or().group(criteria);
                }
                if(combined != null) {
                    try (Reader reader = new IncrementalReader(connections)) {
                        AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
                        read(reader, null, combined, null, null)
                                .onResolve(data::set);
                        reader.drain();
                        for (DatabaseSelection<?> selection : combinable) {
                            demux(reader, selection, data.get());
                        }
                    }
                }
                if(!isolated.isEmpty()) {
                    Runnable[] tasks = new Runnable[isolated.size()];
                    for (int i = 0; i < isolated.size(); i++) {
                        DatabaseSelection<?> selection = isolated.get(i);
                        tasks[i] = () -> {
                            try (Reader reader = new IncrementalReader(
                                    connections)) {
                                $select(reader, selection);
                                reader.drain();
                            }
                        };
                    }
                    selector.join(tasks);

                    // Reservation cannot happen in the async threads above
                    // because it needs access to the #reservations thread
                    // local.
                    for (DatabaseSelection<?> selection : isolated) {
                        reserve(selection);
                    }
                }
            }

            // Propagate results to duplicates
            for (DatabaseSelection<?> selection : selections) {
                if(selection.state == Selection.State.RESOLVED) {
                    selection.setState(Selection.State.FINISHED);
                }
                else if(selection.state != Selection.State.FINISHED) {
                    if(DatabaseSelection.isNoFilter(selection.filter)) {
                        DatabaseSelection<?> canonical = unique.stream()
                                .filter(item -> item.reservation()
                                        .equals(selection.reservation()))
                                .findFirst()
                                .orElseThrow(() -> new IllegalStateException(
                                        "No canonical selection found for "
                                                + selection));
                        selection.setResult(canonical.result);
                        selection.setState(Selection.State.FINISHED);
                    }
                    else {
                        throw new IllegalStateException(
                                "Filtered duplicate selection was not "
                                        + "independently executed: "
                                        + selection);
                    }
                }
            }
            return new Selections(selections);
        }
    }

    /**
     * Deactivate the thread-local reservation cache and release all cached
     * results. After this call, {@link #find}, {@link #count}, and
     * {@link #load} will query the database directly until {@link #reserve()}
     * is called again.
     */
    public void unreserve() {
        reservations.remove();
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
     * Return the navigate paths for {@code clazz} and all descendants.
     *
     * @param clazz
     * @return the navigate paths, or {@code null} if no pre-fetchable
     *         destinations are reachable
     */
    final Set<String> getNavigatePathsForClassHierarchyIfSupported(
            Class<? extends Record> clazz) {
        Set<String> paths = StaticAnalysis.instance()
                .getNavigatePathsHierarchy(clazz);
        return paths != null && !paths.isEmpty() ? paths : null;
    }

    /**
     * Return the navigate paths for {@code clazz}.
     *
     * @param clazz
     * @return the navigate paths, or {@code null} if no pre-fetchable
     *         destinations are reachable
     */
    final Set<String> getNavigatePathsForClassIfSupported(
            Class<? extends Record> clazz) {
        Set<String> paths = StaticAnalysis.instance().getNavigatePaths(clazz);
        return paths != null && !paths.isEmpty() ? paths : null;
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
     * Record on {@code reader} a count of the {@link Record Records} matching
     * {@code criteria}.
     *
     * @param reader the {@link Reader} that records the count operation
     * @param criteria the {@link Criteria} that identifies the records
     * @return a {@link Pending} of the count
     */
    private Pending<Integer> $count(Reader reader, Criteria criteria) {
        if(supportsNativeCount) {
            return reader.count(Record.IDENTIFIER_KEY, criteria)
                    .map(Long::intValue);
        }
        else {
            return reader.find(criteria).map(Set::size);
        }
    }

    /**
     * Record on {@code reader} a read for the {@link Record Records} of
     * {@code clazz} that match {@code criteria}, scoped to {@code realms} and
     * shaped by {@code order} and {@code page}, together with the
     * {@code navigate()} pre-fetch of their {@link Link} targets.
     *
     * @param reader the {@link Reader} that records the reads
     * @param clazz the target {@link Record} class (used to scope the lookup to
     *            instances of exactly this class)
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} to apply to the result set, or
     *            {@code null} for unsorted results
     * @param page the {@link Page} that limits the result set, or {@code null}
     *            for the full result set
     * @param realms the {@link Realms} that scope the lookup
     * @param <T> the {@link Record} type
     * @return a {@link Read} pairing the matching records' data with the
     *         navigate pre-fetch of their {@link Link} targets
     */
    private <T extends Record> Read $find(Reader reader, Class<T> clazz,
            Criteria criteria, @Nullable Order order, @Nullable Page page,
            @Nonnull Realms realms) {
        criteria = $Criteria.amongRealms(realms,
                $Criteria.withinClass(clazz, criteria));
        Set<String> paths = getPathsForClassIfSupported(clazz);
        Set<String> navigatePaths = getNavigatePathsForClassIfSupported(clazz);
        Pending<Map<Long, Map<String, Set<Object>>>> data = read(reader, paths,
                criteria, order, page);
        Pending<Map<Long, Map<String, Set<Object>>>> navigated = prefetchNavigate(
                reader, navigatePaths, criteria, page, data);
        return new Read(data, navigated);
    }

    /**
     * Record on {@code reader} a read for the {@link Record Records} across
     * {@code clazz}'s hierarchy that match {@code criteria}, scoped to
     * {@code realms} and shaped by {@code order} and {@code page}, together
     * with the {@code navigate()} pre-fetch of their {@link Link} targets.
     *
     * @param reader the {@link Reader} that records the reads
     * @param clazz the {@link Record} class whose hierarchy is queried
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} to apply to the result set, or
     *            {@code null} for unsorted results
     * @param page the {@link Page} that limits the result set, or {@code null}
     *            for the full result set
     * @param realms the {@link Realms} that scope the lookup
     * @param <T> the {@link Record} type
     * @return a {@link Read} pairing the matching records' data with the
     *         navigate pre-fetch of their {@link Link} targets
     */
    private <T extends Record> Read $findAny(Reader reader, Class<T> clazz,
            Criteria criteria, @Nullable Order order, @Nullable Page page,
            @Nonnull Realms realms) {
        criteria = $Criteria.amongRealms(realms,
                $Criteria.accrossClassHierachy(clazz, criteria));
        Set<String> paths = getPathsForClassHierarchyIfSupported(clazz);
        Set<String> navigatePaths = getNavigatePathsForClassHierarchyIfSupported(
                clazz);
        Pending<Map<Long, Map<String, Set<Object>>>> data = read(reader, paths,
                criteria, order, page);
        Pending<Map<Long, Map<String, Set<Object>>>> navigated = prefetchNavigate(
                reader, navigatePaths, criteria, page, data);
        return new Read(data, navigated);
    }

    /**
     * Record on {@code reader} a read for every {@link Record} of
     * {@code clazz}, scoped to {@code realms} and shaped by {@code order} and
     * {@code page}, together with the {@code navigate()} pre-fetch of their
     * {@link Link} targets.
     *
     * @param reader the {@link Reader} that records the reads
     * @param clazz the target {@link Record} class (used to scope the lookup to
     *            instances of exactly this class)
     * @param order the {@link Order} to apply to the result set, or
     *            {@code null} for unsorted results
     * @param page the {@link Page} that limits the result set, or {@code null}
     *            for the full result set
     * @param realms the {@link Realms} that scope the lookup
     * @param <T> the {@link Record} type
     * @return a {@link Read} pairing the records' data with the navigate
     *         pre-fetch of their {@link Link} targets
     */
    private <T extends Record> Read $load(Reader reader, Class<T> clazz,
            @Nullable Order order, @Nullable Page page,
            @Nonnull Realms realms) {
        Criteria criteria = $Criteria.amongRealms(realms,
                $Criteria.forClass(clazz));
        Set<String> paths = getPathsForClassIfSupported(clazz);
        Set<String> navigatePaths = getNavigatePathsForClassIfSupported(clazz);
        Pending<Map<Long, Map<String, Set<Object>>>> data = read(reader, paths,
                criteria, order, page);
        Pending<Map<Long, Map<String, Set<Object>>>> navigated = prefetchNavigate(
                reader, navigatePaths, criteria, page, data);
        return new Read(data, navigated);
    }

    /**
     * Record on {@code reader} a read for every {@link Record} in
     * {@code clazz}'s hierarchy, scoped to {@code realms} and shaped by
     * {@code order} and {@code page}, together with the {@code navigate()}
     * pre-fetch of their {@link Link} targets.
     *
     * @param reader the {@link Reader} that records the reads
     * @param clazz the {@link Record} class whose hierarchy is queried
     * @param order the {@link Order} to apply to the result set, or
     *            {@code null} for unsorted results
     * @param page the {@link Page} that limits the result set, or {@code null}
     *            for the full result set
     * @param realms the {@link Realms} that scope the lookup
     * @param <T> the {@link Record} type
     * @return a {@link Read} pairing the records' data with the navigate
     *         pre-fetch of their {@link Link} targets
     */
    private <T extends Record> Read $loadAny(Reader reader, Class<T> clazz,
            @Nullable Order order, @Nullable Page page, Realms realms) {
        Criteria criteria = $Criteria.amongRealms(realms,
                $Criteria.forClassHierarchy(clazz));
        Set<String> paths = getPathsForClassHierarchyIfSupported(clazz);
        Set<String> navigatePaths = getNavigatePathsForClassHierarchyIfSupported(
                clazz);
        Pending<Map<Long, Map<String, Set<Object>>>> data = read(reader, paths,
                criteria, order, page);
        Pending<Map<Long, Map<String, Set<Object>>>> navigated = prefetchNavigate(
                reader, navigatePaths, criteria, page, data);
        return new Read(data, navigated);
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
     * Resolve {@code selection} against the database (or any matching
     * {@link AdHocDataSource AdHocDataSources}), recording any required reads
     * on {@code reader} and mutating {@code selection} with its result.
     * Equivalent to invoking
     * {@link #$selectWithPossibleSources(Reader, DatabaseSelection, Set)} with
     * no pre-supplied {@link AdHocDataSource} set.
     *
     * @param reader the {@link Reader} that records any required reads
     * @param selection the {@link DatabaseSelection} to resolve
     * @param <T> the {@link Record} type
     */
    private <T extends Record> void $select(Reader reader,
            DatabaseSelection<T> selection) {
        $selectWithPossibleSources(reader, selection, null);
    }

    /**
     * Record on {@code reader} the read required to resolve {@code selection}
     * and return a {@link Pending} of the {@link SelectResult} holding the
     * matching {@link Record Records}.
     *
     * @param reader the {@link Reader} that records the read when the selection
     *            can be resolved with a single recorded query
     * @param selection the {@link LoadClassSelection} to resolve
     * @param <T> the {@link Record} type
     * @return a {@link Pending} of the {@link SelectResult}, whose companion
     *         value (when a filter without pagination is applied) carries the
     *         unfiltered records
     */
    private <T extends Record> Pending<SelectResult<Set<T>>> $selectClass(
            Reader reader, LoadClassSelection<T> selection) {
        Class<T> clazz = selection.clazz;
        boolean any = selection.any;
        Order order = selection.order;
        Page page = selection.page;
        Realms realms = selection.realms;
        Predicate<T> filter = selection.filter;
        boolean hasFilter = !DatabaseSelection.isNoFilter(filter);
        if(hasNativeSortingAndPagination
                || doesNotRequireSortingOrPagination(order, page)) {
            // When native sorting/pagination is supported OR no
            // sorting/pagination is requested, the database can handle the
            // query directly without client-side stream manipulation.
            if(hasFilter && page != null) {
                try (Reader sharedReader = new IncrementalReader(connections)) {
                    Function<Page, Set<T>> retriever = $page -> {
                        Read read = any
                                ? $loadAny(sharedReader, clazz, order, $page,
                                        realms)
                                : $load(sharedReader, clazz, order, $page,
                                        realms);
                        AtomicReference<Set<T>> records = new AtomicReference<>();
                        read.data
                                .then($data -> read.navigated
                                        .then($navigated -> resolveLinkTargets(
                                                sharedReader, $data,
                                                $navigated))
                                        .map($targets -> instantiateAll(clazz,
                                                any, $data, $targets)))
                                .onResolve(records::set);
                        sharedReader.drain();
                        return records.get();
                    };
                    return Pending.of(new SelectResult<>(Pagination
                            .applyFilterAndPage(retriever, filter, page)));
                }
            }
            else {
                Read read = any ? $loadAny(reader, clazz, order, page, realms)
                        : $load(reader, clazz, order, page, realms);
                return read.data.then($data -> read.navigated
                        .then($navigated -> resolveLinkTargets(reader, $data,
                                $navigated))
                        .map($targets -> finalizeSet(clazz, any, $data,
                                $targets, hasFilter, filter)));
            }
        }
        else {
            // Legacy servers lack native sorting/pagination, so results must
            // be fetched and processed client-side.
            Set<T> records = fetch(Selection.of(clazz).any(any).realms(realms));
            if(order != null) {
                records = DatabaseInterface.sort(records,
                        backwardsCompatible(order));
            }
            Stream<T> stream = records.stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty());
            if(page != null) {
                stream = stream.skip(page.skip()).limit(page.limit());
            }
            return Pending.of(new SelectResult<>(stream
                    .collect(Collectors.toCollection(LinkedHashSet::new))));
        }
    }

    /**
     * Record on {@code reader} the count required to resolve {@code selection}
     * and return a {@link Pending} of the {@link SelectResult} holding the
     * matching count.
     *
     * @param reader the {@link Reader} that records the count when the
     *            selection can be resolved with a single recorded query
     * @param selection the {@link CountSelection} to resolve
     * @param <T> the {@link Record} type
     * @return a {@link Pending} of the {@link SelectResult}; no companion value
     *         is carried because the underlying {@link #fetch(Selection)} (when
     *         used) covers the unfiltered set under its own {@link Reservation}
     */
    private <T extends Record> Pending<SelectResult<Integer>> $selectCount(
            Reader reader, CountSelection<T> selection) {
        Class<T> clazz = selection.clazz;
        boolean any = selection.any;
        Criteria criteria = selection.criteria;
        Predicate<T> filter = selection.filter;
        boolean hasFilter = !DatabaseSelection.isNoFilter(filter);
        Realms realms = selection.realms;
        if(hasFilter) {
            // Fetch unfiltered so the inner selection caches reusable data,
            // then count the filtered stream.
            Set<T> records = fetch(Selection.of(clazz).any(any).where(criteria)
                    .realms(realms));
            return Pending.of(new SelectResult<>(
                    (int) records.stream().filter(filter).count()));
        }
        else if(criteria == null) {
            // No criteria means count all records of this class
            return $count(reader, any
                    ? $Criteria.amongRealms(realms,
                            $Criteria.forClassHierarchy(clazz))
                    : $Criteria.amongRealms(realms, $Criteria.forClass(clazz)))
                            .map(SelectResult::new);
        }
        else if(Record.isDatabaseResolvableCondition(clazz, criteria)) {
            return $count(
                    reader, any
                            ? $Criteria.amongRealms(realms,
                                    $Criteria.accrossClassHierachy(clazz,
                                            criteria))
                            : $Criteria.amongRealms(realms,
                                    $Criteria.withinClass(clazz, criteria)))
                                            .map(SelectResult::new);
        }
        else {
            return Pending.of(new SelectResult<>(any
                    ? filterAny(clazz, criteria, NO_ORDER, NO_PAGINATION,
                            realms).size()
                    : filter(clazz, criteria, NO_ORDER, NO_PAGINATION, realms)
                            .size()));
        }
    }

    /**
     * Record on {@code reader} the read required to resolve {@code selection}
     * and return a {@link Pending} of the {@link SelectResult} holding the
     * matching {@link Record Records}.
     *
     * @param reader the {@link Reader} that records the read when the selection
     *            can be resolved with a single recorded query
     * @param selection the {@link FindSelection} to resolve
     * @param <T> the {@link Record} type
     * @return a {@link Pending} of the {@link SelectResult}, whose companion
     *         value (when a filter without pagination is applied) carries the
     *         unfiltered records
     */
    private <T extends Record> Pending<SelectResult<Set<T>>> $selectCriteria(
            Reader reader, FindSelection<T> selection) {
        Class<T> clazz = selection.clazz;
        boolean any = selection.any;
        Criteria criteria = selection.criteria;
        Order order = selection.order;
        Page page = selection.page;
        Realms realms = selection.realms;
        Predicate<T> filter = selection.filter;
        boolean hasFilter = !DatabaseSelection.isNoFilter(filter);
        if(hasNativeSortingAndPagination
                || doesNotRequireSortingOrPagination(order, page)) {
            // When native sorting/pagination is supported OR no
            // sorting/pagination is requested, the database can handle the
            // query directly without client-side stream manipulation.
            boolean dbResolvable = Record.isDatabaseResolvableCondition(clazz,
                    criteria);
            if(hasFilter && page != null) {
                try (Reader sharedReader = new IncrementalReader(connections)) {
                    Function<Page, Set<T>> retriever = $page -> {
                        if(dbResolvable) {
                            Read read = any
                                    ? $findAny(sharedReader, clazz, criteria,
                                            order, $page, realms)
                                    : $find(sharedReader, clazz, criteria,
                                            order, $page, realms);
                            AtomicReference<Set<T>> records = new AtomicReference<>();
                            read.data.then($data -> read.navigated
                                    .then($navigated -> resolveLinkTargets(
                                            sharedReader, $data, $navigated))
                                    .map($targets -> instantiateAll(clazz, any,
                                            $data, $targets)))
                                    .onResolve(records::set);
                            sharedReader.drain();
                            return records.get();
                        }
                        else {
                            return any
                                    ? filterAny(clazz, criteria, order, $page,
                                            realms)
                                    : filter(clazz, criteria, order, $page,
                                            realms);
                        }
                    };
                    return Pending.of(new SelectResult<>(Pagination
                            .applyFilterAndPage(retriever, filter, page)));
                }
            }
            else if(dbResolvable) {
                Read read = any
                        ? $findAny(reader, clazz, criteria, order, page, realms)
                        : $find(reader, clazz, criteria, order, page, realms);
                return read.data.then($data -> read.navigated
                        .then($navigated -> resolveLinkTargets(reader, $data,
                                $navigated))
                        .map($targets -> finalizeSet(clazz, any, $data,
                                $targets, hasFilter, filter)));
            }
            else {
                Set<T> records = any
                        ? filterAny(clazz, criteria, order, page, realms)
                        : filter(clazz, criteria, order, page, realms);
                if(hasFilter) {
                    return Pending
                            .of(new SelectResult<>(
                                    records.stream().filter(filter)
                                            .collect(Collectors.toCollection(
                                                    LinkedHashSet::new)),
                                    records));
                }
                else {
                    return Pending.of(new SelectResult<>(records));
                }
            }
        }
        else {
            // Legacy servers lack native sorting/pagination, so results must
            // be fetched and processed client-side.
            Set<T> records = fetch(
                    Selection.of(clazz).any(any).where(criteria));
            if(order != null) {
                records = DatabaseInterface.sort(records,
                        backwardsCompatible(order));
            }
            Stream<T> stream = records.stream()
                    .filter(record -> realms.names().isEmpty() || !Sets
                            .intersection(record.realms(), realms.names())
                            .isEmpty());
            if(page != null) {
                stream = stream.skip(page.skip()).limit(page.limit());
            }
            return Pending.of(new SelectResult<>(stream
                    .collect(Collectors.toCollection(LinkedHashSet::new))));
        }
    }

    /**
     * Resolve {@code selection} against the supplied {@code sources} by
     * dispatching {@link AdHocDataSource#fetch(DatabaseSelection)} to each
     * source and combining the results.
     * <p>
     * For multi-source selections, results are combined per
     * {@link DatabaseSelection} subtype: counts are summed,
     * {@link LoadRecordSelection} returns the first non-{@code null} record,
     * {@link UniqueSelection} enforces at-most-one across sources, and
     * {@link SetBasedSelection} flattens, sorts, and pages the union.
     *
     * @param selection the {@link DatabaseSelection} to resolve
     * @param sources the non-empty {@link AdHocDataSource AdHocDataSources}
     *            against which {@code selection} is dispatched
     * @param <T> the {@link Record} type
     * @param <R> the {@link DatabaseSelection} result type
     * @return the resolved result combined from {@code sources}
     */
    @SuppressWarnings("unchecked")
    private <T extends Record, R> R $selectFromSources(
            DatabaseSelection<T> selection, Set<AdHocDataSource<?>> sources) {
        if(sources.size() == 1) {
            return (R) sources.iterator().next().fetch(selection.duplicate());
        }
        if(selection instanceof CountSelection) {
            Integer count = 0;
            for (AdHocDataSource<?> source : sources) {
                count += (int) source.fetch(selection.duplicate());
            }
            return (R) count;
        }
        else if(selection instanceof LoadRecordSelection) {
            T loaded = null;
            for (AdHocDataSource<?> source : sources) {
                loaded = source.fetch(selection.duplicate());
                if(loaded != null) {
                    break;
                }
            }
            return (R) loaded;
        }
        else if(selection instanceof UniqueSelection) {
            T found = null;
            for (AdHocDataSource<?> source : sources) {
                T candidate = source.fetch(selection.duplicate());
                if(candidate != null && found != null) {
                    // Enforce uniqueness across AdHocDataSources
                    UniqueSelection<T> us = (UniqueSelection<T>) selection;
                    throw duplicateEntryException(
                            "Multiple records match {} in {}{}", us.criteria,
                            us.any ? "the hierarchy of " : "", selection.clazz);
                }
                else if(candidate != null) {
                    found = candidate;
                }
            }
            return (R) found;
        }
        else if(selection instanceof SetBasedSelection) {
            Order order = ((SetBasedSelection<?>) selection).order;
            Page page = ((SetBasedSelection<?>) selection).page;
            Set<T> results = new LinkedHashSet<>();
            for (AdHocDataSource<?> source : sources) {
                SetBasedSelection<?> dupe = (SetBasedSelection<?>) selection
                        .duplicate();
                Reflection.set("order", null, dupe);
                Reflection.set("page", null, dupe);
                results.addAll(source.fetch(dupe));
            }
            if(order != null) {
                results = DatabaseInterface.sort(results,
                        backwardsCompatible(order));
            }
            if(page != null) {
                results = results.stream().skip(page.skip()).limit(page.limit())
                        .collect(Collectors.toCollection(LinkedHashSet::new));
            }
            return (R) results;
        }
        else {
            throw new IllegalStateException(
                    "Unsupported Selection type " + selection.getClass());
        }
    }

    /**
     * Record on {@code reader} the read required to resolve {@code selection}
     * and return a {@link Pending} of the {@link SelectResult} holding the
     * loaded {@link Record} (or {@code null} when no record matches).
     *
     * @param reader the {@link Reader} that records the read
     * @param selection the {@link LoadRecordSelection} to resolve
     * @param <T> the {@link Record} type
     * @return a {@link Pending} of the {@link SelectResult}, whose companion
     *         value (when a filter is applied) carries the unfiltered loaded
     *         {@link Record}
     */
    @SuppressWarnings("unchecked")
    private <T extends Record> Pending<SelectResult<T>> $selectRecord(
            Reader reader, LoadRecordSelection<T> selection) {
        Class<T> initialClazz = selection.clazz;
        long id = selection.id;
        Realms realms = selection.realms;
        Predicate<T> filter = selection.filter;
        boolean hasFilter = !DatabaseSelection.isNoFilter(filter);
        // The provided clazz has descendants, so it is possible that the
        // Record with the #id is actually a member of a subclass.
        boolean needsSectionLookup = StaticAnalysis.instance()
                .getClassHierarchy(initialClazz).size() > 1;
        Set<String> paths = needsSectionLookup ? null
                : getPathsForClassIfSupported(initialClazz);
        Pending<Map<String, Set<Object>>> data = paths != null
                ? reader.select(paths, id)
                : reader.select(id);
        // Record any NAVIGATE prefetch onto the same batch as the main read.
        // When the class is known up front the class-specific navigate paths
        // apply; otherwise the hierarchy-wide paths cover every possible
        // subclass at the cost of fetching a few paths the actual subclass
        // does not use.
        Set<String> navigatePaths = needsSectionLookup
                ? getNavigatePathsForClassHierarchyIfSupported(initialClazz)
                : getNavigatePathsForClassIfSupported(initialClazz);
        Pending<Map<Long, Map<String, Set<Object>>>> navigated = navigatePaths != null
                ? reader.navigate(navigatePaths, id)
                : Pending.of(ImmutableMap.of());
        return data.then($data -> {
            if($data == null || $data.isEmpty()) {
                return Pending.of(new SelectResult<>(null));
            }
            Class<T> clazz = initialClazz;
            if(needsSectionLookup) {
                Set<Object> sections = $data.get(Record.SECTION_KEY);
                if(sections != null && !sections.isEmpty()) {
                    String section = (String) Iterables.getLast(sections);
                    clazz = Reflection.getClassCasted(section);
                }
            }
            if(!realms.names().isEmpty()) {
                Set<Object> $realmsRaw = $data.getOrDefault(Record.REALMS_KEY,
                        ImmutableSet.of());
                Set<String> $realms = (Set<String>) (Set<?>) $realmsRaw;
                if(Sets.intersection($realms, realms.names()).isEmpty()) {
                    return Pending.of(new SelectResult<>(null));
                }
            }
            Class<T> resolvedClazz = clazz;
            Pending<Map<Long, Map<String, Set<Object>>>> targets = navigated
                    .then($navigated -> resolveLinkTargets(reader,
                            ImmutableMap.of(id, $data), $navigated));
            return targets.map($targets -> {
                T record = instantiate(resolvedClazz, id, $data, $targets);
                if(record != null && hasFilter) {
                    return new SelectResult<>(
                            filter.test(record) ? record : null, record);
                }
                return new SelectResult<>(record);
            });
        });
    }

    /**
     * Record on {@code reader} the read required to resolve {@code selection}
     * and return a {@link Pending} of the {@link SelectResult} holding the
     * unique matching {@link Record} (or {@code null} when no record matches).
     *
     * @param reader the {@link Reader} that records the underlying read
     * @param selection the {@link UniqueSelection} to resolve
     * @param <T> the {@link Record} type
     * @return a {@link Pending} of the {@link SelectResult}, whose companion
     *         value carries any companion value produced by the inner query
     * @throws DuplicateEntryException from the returned {@link Pending} when
     *             more than one {@link Record} matches
     */
    private <T extends Record> Pending<SelectResult<T>> $selectUnique(
            Reader reader, UniqueSelection<T> selection) {
        DatabaseSelection.BuilderState<T> state = new DatabaseSelection.BuilderState<>(
                selection.clazz, selection.any);
        state.criteria = selection.criteria;
        state.page = DatabaseInterface.UNIQUE_PAGINATION;
        state.filter = selection.filter;
        state.realms = selection.realms;
        Pending<SelectResult<Set<T>>> inner = selection.criteria != null
                ? $selectCriteria(reader, new FindSelection<>(state))
                : $selectClass(reader, new LoadClassSelection<>(state));
        return inner.map(selected -> {
            Set<T> results = selected.result;
            T result;
            if(results.isEmpty()) {
                result = null;
            }
            else if(results.size() == 1) {
                result = results.iterator().next();
            }
            else {
                throw duplicateEntryException(
                        "Multiple records match {} in {}{}", selection.criteria,
                        selection.any ? "the hierarchy of " : "",
                        selection.clazz);
            }
            return new SelectResult<>(result, selected.cacheValue);
        });
    }

    /**
     * Resolve {@code selection}, recording any required reads on {@code reader}
     * and mutating {@code selection} with its result. Database-bound resolution
     * is deferred to {@link Reader#drain() reader.drain()}; all other paths
     * mutate {@code selection} immediately.
     *
     * @param reader the {@link Reader} that records any required reads
     * @param selection the {@link DatabaseSelection} to resolve
     * @param sources a pre-resolved {@link Set} of {@link AdHocDataSource
     *            AdHocDataSources} for {@code selection}'s class, or
     *            {@code null} to have this method look them up
     * @param <T> the {@link Record} type
     * @param <R> the result type
     */
    @SuppressWarnings({ "rawtypes" })
    private <T extends Record, R> void $selectWithPossibleSources(Reader reader,
            DatabaseSelection<T> selection,
            @Nullable Set<AdHocDataSource<?>> sources) {
        Preconditions.checkState(selection.state == Selection.State.PENDING,
                "Selection must be PENDING; pre-resolved selections must be "
                        + "handled by the caller before dispatch");
        selection.setState(Selection.State.SUBMITTED);
        R cached = recallAndPossiblyFilter(selection);
        if(cached != null) {
            ((DatabaseSelection) selection).setResult(cached);
            selection.setState(Selection.State.FINISHED);
        }
        else {
            Set<AdHocDataSource<?>> relevantSources = sources == null
                    ? (selection.any
                            ? getAttachedSourcesForHierarchy(selection.clazz)
                            : getAttachedSources(selection.clazz))
                    : sources;
            if(!relevantSources.isEmpty()) {
                R result = $selectFromSources(selection, relevantSources);
                ((DatabaseSelection) selection).setResult(result);
                selection.setState(Selection.State.FINISHED);
            }
            else {
                Pending<? extends SelectResult<?>> pending;
                if(selection instanceof CountSelection) {
                    pending = $selectCount(reader,
                            (CountSelection<T>) selection);
                }
                else if(selection instanceof LoadRecordSelection) {
                    pending = $selectRecord(reader,
                            (LoadRecordSelection<T>) selection);
                }
                else if(selection instanceof LoadClassSelection) {
                    pending = $selectClass(reader,
                            (LoadClassSelection<T>) selection);
                }
                else if(selection instanceof FindSelection) {
                    pending = $selectCriteria(reader,
                            (FindSelection<T>) selection);
                }
                else if(selection instanceof UniqueSelection) {
                    pending = $selectUnique(reader,
                            (UniqueSelection<T>) selection);
                }
                else {
                    throw new IllegalStateException(
                            "Unsupported Selection type "
                                    + selection.getClass());
                }
                pending.onResolve(res -> {
                    ((DatabaseSelection) selection).setResult(res.result);
                    selection.cacheValue = res.cacheValue;
                    selection.setState(Selection.State.FINISHED);
                });
            }
        }
    }

    /**
     * Partition the results of a combined multi-select query back into a single
     * {@link Selection} and populate its result, recording the {@link Link}
     * pre-fetch for the partition on {@code reader}.
     *
     * @param reader the {@link Reader} that records the {@link Link} pre-fetch
     * @param selection the {@link Selection} to populate
     * @param data the combined query results
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void demux(Reader reader, DatabaseSelection<?> selection,
            Map<Long, Map<String, Set<Object>>> data) {
        Object result = null;
        if(selection instanceof LoadRecordSelection) {
            long id = ((LoadRecordSelection<?>) selection).id;
            Map<String, Set<Object>> recordData = data.get(id);
            if(recordData != null) {
                Set<Object> sections = recordData.get(Record.SECTION_KEY);
                if(sections != null) {
                    String section = (String) Iterables.getLast(sections);
                    Class actualClass = Reflection.getClassCasted(section);
                    if(selection.clazz.isAssignableFrom(actualClass)) {
                        result = instantiate(actualClass, id, recordData, null);
                    }
                }
            }
        }
        else {
            Map<Long, Map<String, Set<Object>>> filtered = Maps
                    .newLinkedHashMap();
            Set<String> classNames;
            if(selection.any) {
                classNames = StaticAnalysis.instance()
                        .getClassHierarchy(selection.clazz).stream()
                        .map(Class::getName).collect(Collectors.toSet());
            }
            else {
                classNames = ImmutableSet.of(selection.clazz.getName());
            }
            for (Entry<Long, Map<String, Set<Object>>> entry : data
                    .entrySet()) {
                Set<Object> sections = entry.getValue().get(Record.SECTION_KEY);
                if(sections != null) {
                    String section = (String) Iterables.getLast(sections);
                    if(classNames.contains(section)) {
                        filtered.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            Set<String> navigatePaths = selection.any
                    ? getNavigatePathsForClassHierarchyIfSupported(
                            selection.clazz)
                    : getNavigatePathsForClassIfSupported(selection.clazz);
            Pending<Map<Long, Map<String, Set<Object>>>> navigated = navigatePaths != null
                    ? reader.navigate(navigatePaths, filtered.keySet())
                    : Pending.of(ImmutableMap.of());
            AtomicReference<Object> resolved = new AtomicReference<>();
            navigated
                    .then($navigated -> resolveLinkTargets(reader, filtered,
                            $navigated))
                    .map($targets -> instantiateAll((Class) selection.clazz,
                            selection.any, filtered, $targets))
                    .onResolve(resolved::set);
            reader.drain();
            result = resolved.get();
        }
        selection.setResult(result);
        selection.setState(Selection.State.FINISHED);
        reserve(selection);
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
        return fetch(Selection.of(clazz).order(order).page(page)
                .filter(record -> record
                        .matches($Criteria.amongRealms(realms, criteria))));
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
        return fetch(Selection.ofAny(clazz).order(order).page(page)
                .filter(record -> record
                        .matches($Criteria.amongRealms(realms, criteria))));
    }

    /**
     * Build a {@link SelectResult} of {@link Record Records} from {@code data}
     * and pre-fetched {@code targets}, applying {@code filter} when
     * {@code hasFilter} is {@code true}.
     *
     * @param clazz the target {@link Record} class
     * @param any whether to query across the class hierarchy
     * @param data the matching record data
     * @param targets the pre-fetched {@link Link} targets keyed by destination
     *            record id
     * @param hasFilter whether {@code filter} is non-trivial
     * @param filter the client-side filter
     * @param <T> the {@link Record} type
     * @return the {@link SelectResult}
     */
    private <T extends Record> SelectResult<Set<T>> finalizeSet(Class<T> clazz,
            boolean any, Map<Long, Map<String, Set<Object>>> data,
            Map<Long, Map<String, Set<Object>>> targets, boolean hasFilter,
            Predicate<T> filter) {
        Set<T> records = instantiateAll(clazz, any, data, targets);
        if(hasFilter) {
            return new SelectResult<>(
                    records.stream().filter(filter).collect(
                            Collectors.toCollection(LinkedHashSet::new)),
                    records);
        }
        return new SelectResult<>(records);
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
     * descendants) for each entry in {@code data}.
     *
     * @param clazz the target {@link Record} class
     * @param data the record data keyed by record id
     * @param targets the pre-fetched {@link Link} targets keyed by destination
     *            record id
     * @return the instantiated {@link Record Records}
     */
    private <T extends Record> Set<T> instantiateAll(Class<T> clazz,
            Map<Long, Map<String, Set<Object>>> data,
            Map<Long, Map<String, Set<Object>>> targets) {
        ConcurrentMap<Long, Record> loaded = new ConcurrentHashMap<>();
        return LazyTransformSet.of(data.entrySet(), entry -> instantiate(clazz,
                entry.getKey(), loaded, entry.getValue(), targets));
    }

    /**
     * Create a {@link Record} instance for each entry in {@code data}, with
     * each record's class determined by its stored section key.
     *
     * @param data the record data keyed by record id
     * @param targets the pre-fetched {@link Link} targets keyed by destination
     *            record id
     * @return the instantiated {@link Record Records}
     */
    private <T extends Record> Set<T> instantiateAll(
            Map<Long, Map<String, Set<Object>>> data,
            Map<Long, Map<String, Set<Object>>> targets) {
        ConcurrentMap<Long, Record> loaded = new ConcurrentHashMap<>();
        return LazyTransformSet.of(data.entrySet(),
                entry -> instantiate(entry.getKey(), loaded, entry.getValue(),
                        targets));
    }

    /**
     * Create a {@link Record} instance for each entry in {@code data}, either
     * across {@code clazz}'s hierarchy when {@code any} is {@code true} or as
     * exact instances of {@code clazz} otherwise.
     *
     * @param clazz the target {@link Record} class
     * @param any whether to instantiate across the class hierarchy
     * @param data the record data keyed by record id
     * @param targets the pre-fetched {@link Link} targets keyed by destination
     *            record id
     * @return the instantiated {@link Record Records}
     */
    private <T extends Record> Set<T> instantiateAll(Class<T> clazz,
            boolean any, Map<Long, Map<String, Set<Object>>> data,
            Map<Long, Map<String, Set<Object>>> targets) {
        return any ? instantiateAll(data, targets)
                : instantiateAll(clazz, data, targets);
    }

    /**
     * Recursively resolve link-graph targets through {@code reader}, recording
     * one {@link Reader#select(Collection)} per BFS frontier so each depth is
     * shared across sibling {@link Pending Pendings} that drain together.
     *
     * @param reader the {@link Reader} that records each frontier's select
     * @param pool the link-graph data accumulated so far; mutated in place as
     *            new frontiers are loaded
     * @param fetched the record ids already loaded into {@code pool}; mutated
     *            in place
     * @param frontier the record ids to load next
     * @return a {@link Pending} of the fully resolved {@code pool}
     */
    private Pending<Map<Long, Map<String, Set<Object>>>> prefetchLinks(
            Reader reader, Map<Long, Map<String, Set<Object>>> pool,
            Set<Long> fetched, Set<Long> frontier) {
        // BFS over the link graph: each recursive call fetches one depth
        // level. #fetched tracks visited IDs so cycles in the link graph
        // terminate naturally.
        if(frontier.isEmpty()) {
            return Pending.of(pool);
        }
        return reader.select(frontier).then(batch -> {
            pool.putAll(batch);
            fetched.addAll(frontier);
            return prefetchLinks(reader, pool, fetched,
                    extractLinkTargets(batch, fetched));
        });
    }

    /**
     * Record a read on {@code reader} for every record matching
     * {@code criteria}, restricted to {@code paths} when non-{@code null} and
     * shaped by the configured {@link #readStrategy}.
     *
     * @param reader the {@link Reader} on which to record the read
     * @param paths the field names whose values should be returned, or
     *            {@code null} to return every field
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the sort {@link Order} to apply, or {@code null} for an
     *            unsorted result
     * @param page the {@link Page} that limits the result set, or {@code null}
     *            for the full result set
     * @return a {@link Pending} of the matching record data
     */
    private Pending<Map<Long, Map<String, Set<Object>>>> read(Reader reader,
            @Nullable Set<String> paths, Criteria criteria,
            @Nullable Order order, @Nullable Page page) {
        if(readStrategy == ReadStrategy.BULK) {
            if(order != null && page != null) {
                return paths != null
                        ? reader.select(paths, criteria, order, page)
                        : reader.select(criteria, order, page);
            }
            else if(order != null) {
                return paths != null ? reader.select(paths, criteria, order)
                        : reader.select(criteria, order);
            }
            else if(page != null) {
                return paths != null ? reader.select(paths, criteria, page)
                        : reader.select(criteria, page);
            }
            else {
                return paths != null ? reader.select(paths, criteria)
                        : reader.select(criteria);
            }
        }
        else { // STREAM
            Pending<Set<Long>> ids;
            if(order != null && page != null) {
                ids = reader.find(criteria, order, page);
            }
            else if(order != null) {
                ids = reader.find(criteria, order);
            }
            else if(page != null) {
                ids = reader.find(criteria, page);
            }
            else {
                ids = reader.find(criteria);
            }
            return ids.map($ids -> stream(paths, $ids));
        }
    }

    /**
     * Look up a previously reserved result by query signature.
     *
     * @param reservation the query signature
     * @return the reserved result, or {@code null} on miss
     */
    @SuppressWarnings("unchecked")
    private <T> T recall(Reservation reservation) {
        Map<Reservation, Object> reservations = this.reservations.get();
        return reservations != null ? (T) reservations.get(reservation) : null;
    }

    /**
     * Look up a previously reserved result for the {@code selection} and apply
     * client-side filtering if necessary.
     * <p>
     * The reservation cache is keyed by database query parameters (class,
     * criteria, order, page, realms) but not by client-side filter, so a cache
     * hit may contain unfiltered results. This method applies the
     * {@link DatabaseSelection#filter} on hit &mdash; streaming
     * {@link Collection Collections} and testing single {@link Record Records}
     * &mdash; so that callers always receive correctly filtered data.
     * <p>
     * When both a filter and a page are present the cache is skipped entirely,
     * because filtering a fixed page can produce a short result. The
     * type-specific {@code $select} methods handle that case correctly via
     * {@code Pagination.applyFilterAndPage}.
     *
     * @param selection the {@link DatabaseSelection} whose reservation to look
     *            up
     * @param <T> the {@link Record} type
     * @param <R> the result type
     * @return the filtered cached result, or {@code null} on miss or when the
     *         cache must be bypassed
     */
    @SuppressWarnings("unchecked")
    private <T extends Record, R> R recallAndPossiblyFilter(
            DatabaseSelection<T> selection) {
        Object cached = recall(selection.reservation());
        boolean hasFilter = selection.filter != null
                && !DatabaseSelection.isNoFilter(selection.filter);
        boolean hasPagination = selection instanceof SetBasedSelection
                && ((SetBasedSelection<?>) selection).page != null;
        if(cached != null && !(hasFilter && hasPagination)) {
            if(hasFilter) {
                if(cached instanceof Collection) {
                    cached = ((Collection<Record>) cached).stream().filter(
                            (Predicate<? super Record>) selection.filter)
                            .collect(Collectors
                                    .toCollection(LinkedHashSet::new));
                }
                else if(cached instanceof Record) {
                    if(!selection.filter.test((T) cached)) {
                        cached = null;
                    }
                }
                else {
                    // The cached value is not a type that can be filtered
                    // (e.g., an Integer from a count query). Force a cache miss
                    // so the selection re-executes with its own filter logic.
                    return null;
                }
            }
            return (R) cached;
        }
        else {
            return null;
        }
    }

    /**
     * If it exists, store a {@link Selection Selection's} cacheable result in
     * the thread-local reserve.
     * <p>
     * This is a no-op if {@link #reserve()} has not been called on the current
     * thread.
     * </p>
     *
     * @param selection the {@link Selection} to reserve
     */
    private void reserve(DatabaseSelection<?> selection) {
        Preconditions.checkState(selection.state == Selection.State.FINISHED);
        Map<Reservation, Object> reservations = this.reservations.get();
        if(reservations != null) {
            boolean hasFilter = selection.filter != null
                    && !DatabaseSelection.isNoFilter(selection.filter);
            if(selection.cacheValue != null) {
                reservations.put(selection.reservation(), selection.cacheValue);
            }
            else if(!hasFilter) {
                reservations.put(selection.reservation(), selection.result);
            }
        }
    }

    /**
     * Record on {@code reader} the cleanup BFS that closes any {@link Link}
     * targets the navigate phase did not reach, returning a {@link Pending} of
     * the complete target pool.
     *
     * @param reader the {@link Reader} that records each cleanup select
     * @param sources the source records whose {@link Link Links} seed the
     *            traversal
     * @param navigated the navigate phase result
     * @return a {@link Pending} of the target pool keyed by destination record
     *         id
     */
    private Pending<Map<Long, Map<String, Set<Object>>>> resolveLinkTargets(
            Reader reader, Map<Long, Map<String, Set<Object>>> sources,
            Map<Long, Map<String, Set<Object>>> navigated) {
        Map<Long, Map<String, Set<Object>>> pool = new HashMap<>(navigated);
        Set<Long> covered = new HashSet<>(pool.keySet());
        covered.addAll(sources.keySet());
        Set<Long> frontier = extractLinkTargets(sources, covered);
        frontier.addAll(extractLinkTargets(pool, covered));
        return prefetchLinks(reader, pool, covered, frontier);
    }

    /**
     * Record on {@code reader} the {@code navigate()} that pre-fetches the
     * {@link Link} targets reachable from a criteria query.
     * <p>
     * An unpaginated query navigates from the {@code criteria} itself, so the
     * navigate can share a batch with the query's own read; a paginated query
     * navigates from the resolved page's record ids, since only the page
     * &mdash; not the full criteria result &mdash; should be traversed.
     * </p>
     *
     * @param reader the {@link Reader} that records the navigate
     * @param navigatePaths the navigate paths, or {@code null} when navigation
     *            is unsupported for the query's class
     * @param criteria the {@link Criteria} that identifies the starting records
     * @param page the {@link Page} that limits the query, or {@code null} when
     *            unpaginated
     * @param data a {@link Pending} of the query's record data, used to source
     *            the starting ids of a paginated query
     * @return a {@link Pending} of the navigate result keyed by destination
     *         record id
     */
    private Pending<Map<Long, Map<String, Set<Object>>>> prefetchNavigate(
            Reader reader, @Nullable Set<String> navigatePaths,
            Criteria criteria, @Nullable Page page,
            Pending<Map<Long, Map<String, Set<Object>>>> data) {
        if(navigatePaths == null) {
            return Pending.of(ImmutableMap.of());
        }
        else if(page == null) {
            return reader.navigate(navigatePaths, criteria);
        }
        else {
            return data.then(
                    $data -> reader.navigate(navigatePaths, $data.keySet()));
        }
    }

    /**
     * Pre-fetch, through a dedicated {@link Reader}, the {@link Link} targets
     * reachable from {@code data}.
     *
     * @param navigatePaths the navigate paths, or {@code null} when navigation
     *            is unsupported for the records' class
     * @param ids the record ids to navigate from
     * @param data the source records' data, scanned while the cleanup traversal
     *            closes targets the navigate did not reach
     * @return the pre-fetched targets keyed by destination record id
     */
    private Map<Long, Map<String, Set<Object>>> prefetchLinkTargets(
            @Nullable Set<String> navigatePaths, Set<Long> ids,
            Map<Long, Map<String, Set<Object>>> data) {
        if(ids.isEmpty()) {
            return ImmutableMap.of();
        }
        else {
            try (Reader reader = supportsBulkCommands
                    ? new BatchReader(connections)
                    : new IncrementalReader(connections)) {
                Pending<Map<Long, Map<String, Set<Object>>>> navigated = navigatePaths != null
                        ? reader.navigate(navigatePaths, ids)
                        : Pending.of(ImmutableMap.of());
                AtomicReference<Map<Long, Map<String, Set<Object>>>> targets = new AtomicReference<>();
                navigated.then($navigated -> resolveLinkTargets(reader, data,
                        $navigated)).onResolve(targets::set);
                reader.drain();
                return targets.get();
            }
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

        private String environment = "";
        private String host = "localhost";
        private TriConsumer<Class<? extends Record>, Long, Throwable> onLoadFailureHandler = null;
        private String password = "admin";
        private int port = 1717;
        private ReadStrategy readStrategy = null;
        private int streamingReadBufferSize = 100;
        private String username = "admin";
        private List<Entry<Class<? extends Record>, Consumer<? extends Record>>> saveListeners = new ArrayList<>();
        private SpuriousSaveFailureStrategy spuriousSaveFailureStrategy = SpuriousSaveFailureStrategy.FAIL_FAST;

        /**
         * Build the configured {@link Runway} and return the instance.
         *
         * @return a {@link Runway} instance
         */
        @SuppressWarnings("unchecked")
        public Runway build() {
            ConnectionPool connections = ConnectionPool.newCachedConnectionPool(
                    host, port, username, password, environment);
            Runway db = new Runway(connections);
            db.streamingReadBufferSize = streamingReadBufferSize;
            db.readStrategy = MoreObjects.firstNonNull(readStrategy,
                    ReadStrategy.BULK);
            db.spuriousSaveFailureStrategy = spuriousSaveFailureStrategy;
            if(onLoadFailureHandler != null) {
                db.onLoadFailureHandler = onLoadFailureHandler;
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

    }

    /**
     * Properties about this {@link Runway} instance that support post-build
     * configuration and inspection.
     *
     * @author Jeff Nelson
     */
    public class Properties {

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
     * The {@link ReadStrategy} determines how {@link Runway} reads data from
     * Concourse in response to a request.
     *
     * @author Jeff Nelson
     */
    public enum ReadStrategy {
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

    /**
     * A dedup key for {@link DatabaseSelection DatabaseSelections} in a batch
     * {@link #select} call. Two {@link SelectionKey SelectionKeys} are equal
     * when they have the same {@link Reservation} and the same filter instance.
     * This ensures that unfiltered selections with identical query parameters
     * are deduped (since they share the {@link DatabaseSelection#NO_FILTER}
     * singleton), while filtered selections with different predicates are
     * always treated as distinct.
     *
     * @author Jeff Nelson
     */
    private static final class SelectionKey {

        /**
         * The {@link DatabaseSelection} this key represents.
         */
        final DatabaseSelection<?> selection;

        /**
         * The {@link Reservation} derived from the {@link #selection}.
         */
        private final Reservation reservation;

        /**
         * The filter predicate, compared by identity.
         */
        private final Predicate<?> filter;

        /**
         * Construct a new {@link SelectionKey}.
         *
         * @param selection the {@link DatabaseSelection}
         */
        SelectionKey(DatabaseSelection<?> selection) {
            this.selection = selection;
            this.reservation = selection.reservation();
            this.filter = selection.filter;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof SelectionKey) {
                SelectionKey other = (SelectionKey) obj;
                return reservation.equals(other.reservation)
                        && filter == other.filter;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(reservation, System.identityHashCode(filter));
        }

    }

    /**
     * The pair of reads recorded for a class or criteria query: the matching
     * {@link Record Records'} own data, and the {@code navigate()} pre-fetch of
     * the {@link Link} targets reachable from them.
     *
     * @author Jeff Nelson
     */
    private static final class Read {

        /**
         * A {@link Pending} of the matching records' data.
         */
        final Pending<Map<Long, Map<String, Set<Object>>>> data;

        /**
         * A {@link Pending} of the {@code navigate()} pre-fetch of the
         * {@link Link} targets reachable from the matching records.
         */
        final Pending<Map<Long, Map<String, Set<Object>>>> navigated;

        /**
         * Construct a new {@link Read}.
         *
         * @param data a {@link Pending} of the matching records' data
         * @param navigated a {@link Pending} of the {@code navigate()}
         *            pre-fetch
         */
        Read(Pending<Map<Long, Map<String, Set<Object>>>> data,
                Pending<Map<Long, Map<String, Set<Object>>>> navigated) {
            this.data = data;
            this.navigated = navigated;
        }

    }
}
