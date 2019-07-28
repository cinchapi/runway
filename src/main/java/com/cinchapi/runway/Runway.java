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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.server.plugin.util.Versions;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logging;
import com.cinchapi.runway.cache.NoOpCache;
import com.github.zafarkhaja.semver.Version;
import com.google.common.cache.Cache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
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
     * Utility method do ensure that the {@code criteria} is limited to querying
     * objects that belong to a specific {@code clazz}.
     * 
     * @param criteria
     * @param clazz
     * @return the updated {@code criteria}
     */
    private static <T> Criteria ensureClassSpecificCriteria(Criteria criteria,
            Class<T> clazz) {
        return Criteria.where().key(Record.SECTION_KEY)
                .operator(Operator.EQUALS).value(clazz.getName()).and()
                .group(criteria).build();
    }

    /**
     * A connection pool to the underlying Concourse database.
     */
    /* package */ final ConnectionPool connections;

    /**
     * A pluggable {@link Cache} that can be used to make repeated record
     * {@link #load(Class, long, TLongObjectHashMap)} more efficient.
     */
    private final Cache<Long, Record> cache;

    /**
     * A mapping from a transaction id to the set of records that are waiting to
     * be saved within that transaction. We use this collection to ensure that a
     * record being saved only links to an existing record in the database or a
     * record that will later exist (e.g. waiting to be saved).
     */
    private final TLongObjectMap<Set<Record>> waitingToBeSaved = new TLongObjectHashMap<Set<Record>>();

    /**
     * A flag that indicates whether the connected server supports result set
     * sorting.
     */
    private final boolean sortable;

    /**
     * Construct a new instance.
     * 
     * @param connections a Concourse {@link ConnectionPool}
     */
    private Runway(ConnectionPool connections, Cache<Long, Record> cache) {
        this.connections = connections;
        instances.add(this);
        if(instances.size() > 1) {
            Record.PINNED_RUNWAY_INSTANCE = null;
        }
        else {
            Record.PINNED_RUNWAY_INSTANCE = this;
        }
        this.cache = cache;
        Concourse concourse = connections.request();
        try {
            Version target = Version.forIntegers(0, 10);
            Version actual = Versions
                    .parseSemanticVersion(concourse.getServerVersion());
            sortable = actual.greaterThanOrEqualTo(target);
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
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria) {
        Concourse concourse = connections.request();
        try {
            Set<Long> ids = $find(concourse, clazz, criteria);
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<Record>();
            AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
            Set<T> records = LazyTransformSet.of(ids, id -> {
                if(data.get() == null) {
                    data.set($select(ids));
                }
                return load(clazz, id, existing, data.get().get(id));
            });
            return records;
        }
        finally {
            connections.release(concourse);
        }
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria) {
        Concourse concourse = connections.request();
        try {
            Collection<Class<?>> hierarchy = hierarchies.get(clazz);
            Map<Long, Class<T>> ids = Maps.newLinkedHashMap();
            for (Class cls : hierarchy) {
                Set<Long> $ids = $find(concourse, cls, criteria);
                $ids.forEach(id -> ids.put(id, cls));
            }
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<Record>();
            AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
            Set<T> found = LazyTransformSet.of(ids.keySet(), id -> {
                if(data.get() == null) {
                    data.set($select(ids.keySet()));
                }
                return load(ids.get(id), id, existing, data.get().get(id));
            });
            return found;
        }
        finally {
            connections.release(concourse);
        }

    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T extends Record> T findAnyUnique(Class<T> clazz,
            Criteria criteria) {
        Collection<Class<?>> hierarchy = hierarchies.get(clazz);
        Set<T> found = Sets.newLinkedHashSet();
        for (Class cls : hierarchy) {
            T $found = (T) findUnique(cls, criteria);
            if($found != null) {
                found.add($found);
            }
        }
        if(found.size() == 0) {
            return null;
        }
        else if(found.size() == 1) {
            return found.iterator().next();
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
    public <T extends Record> T findUnique(Class<T> clazz, Criteria criteria) {
        Concourse concourse = connections.request();
        try {
            criteria = ensureClassSpecificCriteria(criteria, clazz);
            Set<Long> ids = concourse.find(criteria);
            if(ids.isEmpty()) {
                return null;
            }
            else if(ids.size() > 1) {
                throw new DuplicateEntryException(
                        new com.cinchapi.concourse.thrift.DuplicateEntryException(
                                AnyStrings.format(
                                        "There are more than one records that match {} in {}",
                                        criteria, clazz)));
            }
            else {
                long id = Iterables.getOnlyElement(ids);
                return load(clazz, id);
            }
        }
        finally {
            connections.release(concourse);
        }
    }

    @Override
    public <T extends Record> Set<T> load(Class<T> clazz) {
        Concourse concourse = connections.request();
        try {
            Set<Long> ids = $load(concourse, clazz);
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<>();
            AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
            Set<T> records = LazyTransformSet.of(ids, id -> {
                if(data.get() == null) {
                    data.set($select(ids));
                }
                return load(clazz, id, existing, data.get().get(id));
            });
            return records;
        }
        finally {
            connections.release(concourse);
        }
    }

    @Override
    public <T extends Record> T load(Class<T> clazz, long id) {
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
        return load(clazz, id, new TLongObjectHashMap<Record>(), null);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Record> Set<T> loadAny(Class<T> clazz) {
        Concourse concourse = connections.request();
        try {
            Collection<Class<?>> hierarchy = hierarchies.get(clazz);
            Map<Long, Class<T>> ids = Maps.newLinkedHashMap();
            for (Class cls : hierarchy) {
                Set<Long> $ids = $load(concourse, cls);
                $ids.forEach(id -> ids.put(id, cls));
            }
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<Record>();
            AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
            Set<T> loaded = LazyTransformSet.of(ids.keySet(), id -> {
                if(data.get() == null) {
                    data.set($select(ids.keySet()));
                }
                return load(ids.get(id), id, existing, data.get().get(id));
            });
            return loaded;
        }
        finally {
            connections.release(concourse);
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
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<>();
            Set<Long> ids = $search(concourse, clazz, query, keys);
            AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
            Set<T> records = LazyTransformSet.of(ids, id -> {
                if(data.get() == null) {
                    data.set($select(ids));
                }
                return load(clazz, id, existing, data.get().get(id));
            });
            return records;
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Record> Set<T> searchAny(Class<T> clazz, String query,
            String... keys) {
        Concourse concourse = connections.request();
        try {
            Collection<Class<?>> hierarchy = hierarchies.get(clazz);
            Map<Long, Class<T>> ids = Maps.newLinkedHashMap();
            for (Class cls : hierarchy) {
                Set<Long> $ids = $search(concourse, cls, query, keys);
                $ids.forEach(id -> ids.put(id, cls));
            }
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<Record>();
            AtomicReference<Map<Long, Map<String, Set<Object>>>> data = new AtomicReference<>();
            Set<T> loaded = LazyTransformSet.of(ids.keySet(), id -> {
                if(data.get() == null) {
                    data.set($select(ids.keySet()));
                }
                return load(ids.get(id), id, existing, data.get().get(id));
            });
            return loaded;
        }
        finally {
            connections.release(concourse);
        }

    }

    // TODO: what about loading a specific record using a parent class?

    /**
     * Perform the find operation using the {@code concourse} handler.
     * 
     * @param concourse
     * @param clazz
     * @param criteria
     * @return the result set
     */
    private <T> Set<Long> $find(Concourse concourse, Class<T> clazz,
            Criteria criteria) {
        criteria = ensureClassSpecificCriteria(criteria, clazz);
        return concourse.find(criteria);
    }

    /**
     * Return the ids of all the {@code Record}s in the {@code clazz}, using the
     * provided {@code concourse} connection.
     * 
     * @param concourse
     * @param clazz
     * @return the records in the class
     */
    private <T> Set<Long> $load(Concourse concourse, Class<T> clazz) {
        Criteria criteria = Criteria.where().key(Record.SECTION_KEY)
                .operator(Operator.EQUALS).value(clazz.getName()).build();
        return concourse.find(criteria);
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
    private <T> Set<Long> $search(Concourse concourse, Class<T> clazz,
            String query, String... keys) {
        return Arrays.stream(keys).map(key -> concourse.search(key, query))
                .flatMap(Set::stream)
                .filter(record -> concourse.get(Record.SECTION_KEY, record)
                        .equals(clazz.getName()))
                .collect(Collectors.toSet());
    }

    /**
     * Intelligently select all the data for the {@code ids} from
     * {@code concourse}.
     * 
     * @param concourse
     * @param ids
     * @return the selected data
     */
    private Map<Long, Map<String, Set<Object>>> $select(Set<Long> ids) {
        Concourse concourse = connections.request();
        try {
            // TODO: stagger/stream/buffer the load if there are a lot of ids
            // (possibly using a continuation) so that a group of records are
            // fetched on the fly only when necessary so as to prevent holding
            // so much data in memory
            return concourse.select(ids);
        }
        finally {
            connections.release(concourse);
        }
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
     * @return the loaded {@link Record} instance
     */
    @SuppressWarnings("unchecked")
    private <T extends Record> T load(Class<T> clazz, long id,
            TLongObjectHashMap<Record> existing,
            @Nullable Map<String, Set<Object>> data) {
        Record record;
        try {
            record = cache.get(id, () -> {
                return Record.load(clazz, id, existing, connections, this,
                        data);
            });
        }
        catch (ExecutionException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        return (T) record;
    }

    /**
     * Builder for {@link Runway} connections. This is returned from
     * {@link #builder()}.
     *
     * @author Jeff Nelson
     */
    public static class Builder {

        private Cache<Long, Record> cache = new NoOpCache<>();
        private String environment = "";
        private String host = "localhost";
        private String password = "admin";
        private int port = 1717;
        private String username = "admin";

        /**
         * Build the configured {@link Runway} and return the instance.
         * 
         * @return a {@link Runway} instance
         */
        public Runway build() {
            return new Runway(ConnectionPool.newCachedConnectionPool(host, port,
                    username, password, environment), cache);
        }

        /**
         * Set the connection's cache.
         * 
         * @param cache
         * @return this builder
         */
        public Builder cache(Cache<Long, Record> cache) {
            this.cache = cache;
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

}
