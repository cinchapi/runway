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

import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
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
public final class Runway implements AutoCloseable {

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

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the matching records
     */
    public <T extends Record> Set<T> find(Class<T> clazz,
            BuildableState criteria) {
        return find(clazz, criteria.build());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the matching records
     */
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria) {
        Concourse concourse = connections.request();
        try {
            Set<T> records = Sets.newLinkedHashSet();
            criteria = ensureClassSpecificCriteria(criteria, clazz);
            Set<Long> ids = concourse.find(criteria);
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<Record>();
            ids.forEach((id) -> {
                T record = load(clazz, id, existing);
                records.add(record);
            });
            return records;
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
     */
    public <T extends Record> T findOne(Class<T> clazz,
            BuildableState criteria) {
        return findOne(clazz, criteria.build());
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
     */
    public <T extends Record> T findOne(Class<T> clazz, Criteria criteria) {
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
                                Strings.format(
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

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> load(Class<T> clazz) {
        Concourse concourse = connections.request();
        try {
            Set<T> records = Sets.newLinkedHashSet();
            Criteria criteria = Criteria.where().key(Record.SECTION_KEY)
                    .operator(Operator.EQUALS).value(clazz.getName()).build();
            Set<Long> ids = concourse.find(criteria);
            TLongObjectHashMap<Record> existing = new TLongObjectHashMap<>();
            ids.forEach(id -> records.add(load(clazz, id, existing)));
            return records;
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Load the Record that is contained within the specified {@code clazz} and
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
     * @return the existing Record
     */
    public <T extends Record> T load(Class<T> clazz, long id) {
        return load(clazz, id, new TLongObjectHashMap<Record>());
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
                return records[0].save(concourse, Sets.newHashSet());
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
        return Record.load(clazz, id, existing, connections);
    }

}
