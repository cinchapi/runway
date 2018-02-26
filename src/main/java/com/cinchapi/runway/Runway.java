/*
 * Cinchapi Inc. CONFIDENTIAL
 * Copyright (c) 2017 Cinchapi Inc. All Rights Reserved.
 *
 * All information contained herein is, and remains the property of Cinchapi.
 * The intellectual and technical concepts contained herein are proprietary to
 * Cinchapi and may be covered by U.S. and Foreign Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained from Cinchapi. Access to the source code
 * contained herein is hereby forbidden to anyone except current Cinchapi
 * employees, managers or contractors who have executed Confidentiality and
 * Non-disclosure agreements explicitly covering such access.
 *
 * The copyright notice above does not evidence any actual or intended
 * publication or disclosure of this source code, which includes information
 * that is confidential and/or proprietary, and is a trade secret, of Cinchapi.
 *
 * ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC PERFORMANCE, OR PUBLIC
 * DISPLAY OF OR THROUGH USE OF THIS SOURCE CODE WITHOUT THE EXPRESS WRITTEN
 * CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
 * LAWS AND INTERNATIONAL TREATIES. THE RECEIPT OR POSSESSION OF THIS SOURCE
 * CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS TO
 * REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR
 * SELL ANYTHING THAT IT MAY DESCRIBE, IN WHOLE OR IN PART.
 */
package com.cinchapi.runway;

import java.util.Set;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.DuplicateEntryException;
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
 *
 *
 * @author jeff
 */
public final class Runway implements AutoCloseable {

    /**
     * The record where metadata is stored. We typically store some transient
     * metadata for transaction routing within this record (so its only visible
     * within the specific transaction) and we clear it before commit time.
     */
    private static long METADATA_RECORD = -1;

    private static Set<Runway> instances = Sets.newHashSet();

    public static Runway connect() {
        return null;
    }

    public static Runway connect(String host, int port, String username,
            String password) {
        return connect(host, port, username, password, "");
    }

    public static Runway connect(String host, int port, String username,
            String password, String environment) {
        return new Runway(host, port, username, password, environment);
    }

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
     * @param host
     * @param port
     * @param username
     * @param password
     * @param environment
     */
    private Runway(String host, int port, String username, String password,
            String environment) {
        this.connections = ConnectionPool.newCachedConnectionPool(host, port,
                username, password, environment);
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
                return records[0].save(concourse);
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
                waitingToBeSaved.put(transactionId, waiting);
                for (Record record : records) {
                    current = record;
                    record.saveWithinTransaction(concourse);
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
