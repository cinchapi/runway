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
import java.util.Set;
import java.util.stream.Collectors;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Logging;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
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

    /**
     * Return a {@link Runway} instance that is connected to Concourse using the
     * default connection parameters.
     * 
     * @return a {@link Runway} instance
     */
    public static Runway connect() {
        return new Runway(ConnectionPool.newCachedConnectionPool());
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
     */
    public static Runway connect(String host, int port, String username,
            String password) {
        return connect(host, port, username, password, "");
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
     */
    public static Runway connect(String host, int port, String username,
            String password, String environment) {
        return new Runway(ConnectionPool.newCachedConnectionPool(host, port,
                username, password, environment));
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
     * A connection pool to the underlying Concourse database.
     */
    /* package */ final ConnectionPool connections;

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
    public <T extends Record> Set<T> find(Class<T> clazz,
            BuildableState criteria) {
        return find(clazz, criteria.build());
    }

    @Override
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria) {
        Concourse concourse = connections.request();
        try {
            criteria = ensureClassSpecificCriteria(criteria, clazz);
            Set<Long> ids = concourse.find(criteria);
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<Record>();
            Set<T> records = LazyTransformSet.of(ids,
                    id -> load(clazz, id, existing));
            return records;
        }
        finally {
            connections.release(concourse);
        }
    }

    @Override
    public <T extends Record> Set<T> findAny(Class<T> clazz,
            BuildableState criteria) {
        return findAny(clazz, criteria.build());
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria) {
        Collection<Class<?>> hierarchy = hierarchies.get(clazz);
        Set<T> found = Sets.newLinkedHashSet();
        for (Class cls : hierarchy) {
            found.addAll(find(cls, criteria));
        }
        return found;
    }

    @Override
    public <T extends Record> T findAnyUnique(Class<T> clazz,
            BuildableState criteria) {
        return findAnyUnique(clazz, criteria.build());
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
    public <T extends Record> T findUnique(Class<T> clazz,
            BuildableState criteria) {
        return findUnique(clazz, criteria.build());
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
            Criteria criteria = Criteria.where().key(Record.SECTION_KEY)
                    .operator(Operator.EQUALS).value(clazz.getName()).build();
            Set<Long> ids = concourse.find(criteria);
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<>();
            Set<T> records = LazyTransformSet.of(ids,
                    id -> load(clazz, id, existing));
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
        return load(clazz, id, new TLongObjectHashMap<Record>());
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Record> Set<T> loadAny(Class<T> clazz) {
        Collection<Class<?>> hierarchy = hierarchies.get(clazz);
        Set<T> loaded = Sets.newLinkedHashSet();
        for (Class cls : hierarchy) {
            loaded.addAll(load(cls));
        }
        return loaded;
    }

    // TODO: what about loading a specific record using a parent class?

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
                    current.errors.add(Throwables.getStackTraceAsString(t));
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
        concourse.stage();
        try {
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<>();
            Set<Long> ids = Arrays.stream(keys)
                    .map(key -> concourse.search(key, query))
                    .flatMap(Set::stream)
                    .filter(record -> concourse.get(Record.SECTION_KEY, record)
                            .equals(clazz.getName()))
                    .collect(Collectors.toSet());
            Set<T> records = LazyTransformSet.of(ids,
                    id -> load(clazz, id, existing));
            if(concourse.commit()) {
                return records;
            }
            else {
                throw new TransactionException();
            }
        }
        catch (TransactionException e) {
            concourse.abort();
            return search(clazz, query, keys);
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
        Collection<Class<?>> hierarchy = hierarchies.get(clazz);
        Set<T> loaded = Sets.newLinkedHashSet();
        for (Class cls : hierarchy) {
            loaded.addAll(search(cls, query, keys));
        }
        return loaded;
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and
     * has the specified {@code id}.
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param id
     * @param existing
     * @return
     */
    private <T extends Record> T load(Class<T> clazz, long id,
            TLongObjectHashMap<Record> existing) {
        return Record.load(clazz, id, existing, connections, this);
    }

}
