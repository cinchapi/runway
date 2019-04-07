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

}
