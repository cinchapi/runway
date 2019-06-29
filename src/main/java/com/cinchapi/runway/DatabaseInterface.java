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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;

/**
 * A {@link DatabaseInterface} provides methods for interacting with a database
 * backend.
 *
 * @author Jeff Nelson
 */
public interface DatabaseInterface {

    /*
     * IMPLEMENTATION NOTE
     * -------------------
     * Sorting functionality currently uses the local Record#compareTo(Record,
     * <String|List>) functionality because Concourse (as of Version 0.9.Z) does
     * not natively support result sorting. This is expected to change in a
     * future release. When that happens, Runway must seamlessly leverage the
     * database's native sort when available while also falling back to the
     * current implementation when it is not supported.
     * 
     * To do so, do the following:
     * 1) Add #find, #findAny, #load and #loadAny methods that take Concourse's
     * sort construct (e.g. Order)
     * 2) In the implementation of those new methods, check the version of
     * Concourse. If native sort is supported, use it. Otherwise, convert the
     * Order to Runway's String/List order instruction and call the appropriate
     * method.
     */

    /**
     * Return the {@code records} in sorted {@code order}.
     * 
     * @param records
     * @param order
     * @return the sorted records
     */
    public static <T extends Record> Set<T> sort(Set<T> records,
            List<String> order) {
        return order != null
                ? records.stream().sorted((r1, r2) -> r1.compareTo(r2, order))
                        .collect(Collectors.toCollection(LinkedHashSet::new))
                : records;
    }

    /**
     * Return the {@code records} in sorted {@code order}.
     * 
     * @param records
     * @param order
     * @return the sorted records
     */
    public static <T extends Record> Set<T> sort(Set<T> records, String order) {
        return order != null
                ? records.stream().sorted((r1, r2) -> r1.compareTo(r2, order))
                        .collect(Collectors.toCollection(LinkedHashSet::new))
                : records;
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            BuildableState criteria) {
        return find(clazz, criteria.build());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            BuildableState criteria, String order) {
        Set<T> records = find(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, String order) {
        Set<T> records = find(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            BuildableState criteria, List<String> order) {
        Set<T> records = find(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, List<String> order) {
        Set<T> records = find(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the matching records
     */
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria);

    /**
     * Execute the {@link #find(Class, BuildableState)} query for {@code clazz}
     * and all of its descendants.
     * 
     * @param clazz
     * @param criteria
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            BuildableState criteria) {
        return findAny(clazz, criteria.build());
    }

    /**
     * Execute the {@link #find(Class, BuildableState)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            BuildableState criteria, String order) {
        Set<T> records = findAny(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Execute the {@link #find(Class, BuildableState)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            BuildableState criteria, List<String> order) {
        Set<T> records = findAny(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Execute the {@link #find(Class, BuildableState)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, String order) {
        Set<T> records = findAny(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Execute the {@link #find(Class, BuildableState)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, List<String> order) {
        Set<T> records = findAny(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants.
     * 
     * @param clazz
     * @param criteria
     * @return the matching records
     */
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria);

    /**
     * Execute the {@link #findUnique(Class, BuildableState)} query for
     * {@code clazz} and all of its descendants.
     * 
     * @param clazz
     * @param criteria
     * @return the one matching record
     */
    public default <T extends Record> T findAnyUnique(Class<T> clazz,
            BuildableState criteria) {
        return findAnyUnique(clazz, criteria.build());
    }

    /**
     * Execute the {@link #findUnique(Class, Criteria)} query for {@code clazz}
     * and
     * all of its descendants.
     * 
     * @param clazz
     * @param criteria
     * @return the one matching record
     */
    public <T extends Record> T findAnyUnique(Class<T> clazz,
            Criteria criteria);

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
    public default <T extends Record> T findUnique(Class<T> clazz,
            BuildableState criteria) {
        return findUnique(clazz, criteria.build());
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
    public <T extends Record> T findUnique(Class<T> clazz, Criteria criteria);

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
    public <T extends Record> Set<T> load(Class<T> clazz);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order}.
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
    public default <T extends Record> Set<T> load(Class<T> clazz,
            List<String> order) {
        Set<T> records = load(clazz);
        return sort(records, order);
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
    public <T extends Record> T load(Class<T> clazz, long id);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order}.
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
    public default <T extends Record> Set<T> load(Class<T> clazz,
            String order) {
        Set<T> records = load(clazz);
        return sort(records, order);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants.
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
    public <T extends Record> Set<T> loadAny(Class<T> clazz);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order}.
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
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            String order) {
        Set<T> records = loadAny(clazz);
        return sort(records, order);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order}.
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
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            List<String> order) {
        Set<T> records = loadAny(clazz);
        return sort(records, order);
    }

}