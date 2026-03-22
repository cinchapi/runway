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

import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Gateway} provides a unified entry point for database access. Use a
 * {@link Gateway} when invoking operations without needing to manually account
 * for optional arguments or conditional logic.
 * <p>
 * A {@link Gateway} interprets the intent of the caller from the arguments
 * supplied, including those that may be null or empty, and ensures that the
 * request is fulfilled through the most appropriate underlying database
 * operation.
 * </p>
 * <p>
 * This design allows client code to remain concise and expressive, while
 * ensuring that optional input such as {@link Criteria}, {@link Order} or
 * {@link Page} are honored consistently and efficiently.
 * </p>
 *
 * @deprecated Use {@link Selection#of(Class, Criteria, Order, Page, Realms)} or
 *             {@link Selection#ofAny(Class, Criteria, Order, Page, Realms)}
 *             with {@link DatabaseInterface#select(Selection...)} instead. The
 *             {@link Selection} API provides the same null-tolerant routing
 *             with support for batching multiple operations into fewer database
 *             calls.
 * @author Jeff Nelson
 */
@Deprecated
public final class Gateway {

    /**
     * Create a new {@link Gateway} instance that provides access to the
     * specified database.
     *
     * @param db the database interface to use for operations
     * @return a new gateway instance
     */
    static Gateway to(DatabaseInterface db) {
        return new Gateway(db);
    }

    /**
     * The database interface used by this gateway for all operations.
     */
    private final DatabaseInterface db;

    /**
     * Create a new gateway instance with the specified database interface.
     *
     * @param db the database interface to use for operations
     */
    private Gateway(DatabaseInterface db) {
        this.db = db;
    }

    /**
     * Fetch records from the specified class, possibly filtered by
     * {@code criteria}, possibly sorted by {@code order}, and possibly
     * paginated by {@code page}. This method intelligently routes to the
     * appropriate database operation ({@link DatabaseInterface#find} or
     * {@link DatabaseInterface#load}) based on the parameters provided. When
     * any of these elements are null, they are ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param order the sort order, possibly null
     * @param page the pagination details, possibly null
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#of(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetch(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Order order,
            @Nullable Page page) {
        return fetch(clazz, criteria, order, page, Realms.any(), false);
    }

    /**
     * Fetch records from the specified class, possibly filtered by
     * {@code criteria}, possibly sorted by {@code order}, and possibly
     * paginated by {@code page}, within the specified {@code realms}. This
     * method intelligently routes to the appropriate database operation
     * ({@link DatabaseInterface#find} or {@link DatabaseInterface#load}) based
     * on the parameters provided. When any of these elements are null, they are
     * ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param order the sort order, possibly null
     * @param page the pagination details, possibly null
     * @param realms the realms to search within
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#of(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetch(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Order order,
            @Nullable Page page, Realms realms) {
        return fetch(clazz, criteria, order, page, realms, false);
    }

    /**
     * Fetch records from the specified class, possibly filtered by
     * {@code criteria}, possibly sorted by {@code order}, and possibly
     * paginated by {@code page}. This method intelligently routes to the
     * appropriate database operation ({@link DatabaseInterface#find} or
     * {@link DatabaseInterface#load}) based on the parameters provided. When
     * any of these elements are null, they are ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param page the pagination details, possibly null
     * @param order the sort order, possibly null
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#of(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetch(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Page page,
            @Nullable Order order) {
        return fetch(clazz, criteria, order, page, Realms.any(), false);
    }

    /**
     * Fetch records from the specified class, possibly filtered by
     * {@code criteria}, possibly sorted by {@code order}, and possibly
     * paginated by {@code page}, within the specified {@code realms}. This
     * method intelligently routes to the appropriate database operation
     * ({@link DatabaseInterface#find} or {@link DatabaseInterface#load}) based
     * on the parameters provided. When any of these elements are null, they are
     * ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param page the pagination details, possibly null
     * @param order the sort order, possibly null
     * @param realms the realms to search within, possibly null
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#of(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetch(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Page page,
            @Nullable Order order, @Nullable Realms realms) {
        return fetch(clazz, criteria, order, page, realms, false);
    }

    /**
     * Fetch records from the specified class or its descendants, possibly
     * filtered by {@code criteria}, possibly sorted by {@code order}, and
     * possibly paginated by {@code page}. This method intelligently routes to
     * the appropriate database operation ({@link DatabaseInterface#findAny} or
     * {@link DatabaseInterface#loadAny}) based on the parameters provided. When
     * any of these elements are null, they are ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param order the sort order, possibly null
     * @param page the pagination details, possibly null
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#ofAny(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetchAny(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Order order,
            @Nullable Page page) {
        return fetch(clazz, criteria, order, page, Realms.any(), true);
    }

    /**
     * Fetch records from the specified class or its descendants, possibly
     * filtered by {@code criteria}, possibly sorted by {@code order}, and
     * possibly paginated by {@code page}, within the specified {@code realms}.
     * This method intelligently routes to the appropriate database operation
     * ({@link DatabaseInterface#findAny} or {@link DatabaseInterface#loadAny})
     * based on the parameters provided. When any of these elements are null,
     * they are ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param order the sort order, possibly null
     * @param page the pagination details, possibly null
     * @param realms the realms to search within
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#ofAny(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetchAny(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Order order,
            @Nullable Page page, Realms realms) {
        return fetch(clazz, criteria, order, page, realms, true);
    }

    /**
     * Fetch records from the specified class or its descendants, possibly
     * filtered by {@code criteria}, possibly sorted by {@code order}, and
     * possibly paginated by {@code page}. This method intelligently routes to
     * the appropriate database operation ({@link DatabaseInterface#findAny} or
     * {@link DatabaseInterface#loadAny}) based on the parameters provided. When
     * any of these elements are null, they are ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param page the pagination details, possibly null
     * @param order the sort order, possibly null
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#ofAny(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetchAny(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Page page,
            @Nullable Order order) {
        return fetch(clazz, criteria, order, page, Realms.any(), true);
    }

    /**
     * Fetch records from the specified class or its descendants, possibly
     * filtered by {@code criteria}, possibly sorted by {@code order}, and
     * possibly paginated by {@code page}, within the specified {@code realms}.
     * This method intelligently routes to the appropriate database operation
     * ({@link DatabaseInterface#findAny} or {@link DatabaseInterface#loadAny})
     * based on the parameters provided. When any of these elements are null,
     * they are ignored in the fetch process.
     *
     * @param clazz the class of records to fetch
     * @param criteria the selection criteria, possibly null
     * @param page the pagination details, possibly null
     * @param order the sort order, possibly null
     * @param realms the realms to search within, possibly null
     * @return the fetched records
     * @deprecated Use
     *             {@link Selection#ofAny(Class, Criteria, Order, Page, Realms)}
     */
    @Deprecated
    public <T extends Record> Set<T> fetchAny(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Page page,
            @Nullable Order order, @Nullable Realms realms) {
        return fetch(clazz, criteria, order, page, realms, true);
    }

    /**
     * Route the database fetch calls based on the provided parameters.
     * Different combinations of non-null parameters route to different database
     * operations
     * ({@link DatabaseInterface#find}/{@link DatabaseInterface#findAny} or
     * {@link DatabaseInterface#load}/{@link DatabaseInterface#loadAny}).
     *
     * @param <T> the type of the records to fetch
     * @param clazz the class of the records
     * @param criteria the conditions that the records must meet, possibly null
     * @param order the order in which to sort the records, possibly null
     * @param page the pagination details, possibly null
     * @param realms the realms to look into when fetching records
     * @param any flag to indicate whether to fetch from {@code clazz} and its
     *            descendants
     * @return a set of records meeting the criteria, sorted, paginated and
     *         limited to realms as specified
     */
    private <T extends Record> Set<T> fetch(Class<T> clazz,
            @Nullable Criteria criteria, @Nullable Order order,
            @Nullable Page page, Realms realms, boolean any) {
        Set<T> records;
        if(criteria != null && order != null && page != null) {
            records = any ? db.findAny(clazz, criteria, order, page)
                    : db.find(clazz, criteria, order, page);
        }
        else if(criteria != null && order != null) {
            records = any ? db.findAny(clazz, criteria, order)
                    : db.find(clazz, criteria, order);
        }
        else if(criteria != null && page != null) {
            records = any ? db.findAny(clazz, criteria, page)
                    : db.find(clazz, criteria, page);
        }
        else if(order != null && page != null) {
            records = any ? db.loadAny(clazz, order, page)
                    : db.load(clazz, order, page);
        }
        else if(criteria != null) {
            records = any ? db.findAny(clazz, criteria)
                    : db.find(clazz, criteria);
        }
        else if(order != null) {
            records = any ? db.loadAny(clazz, order) : db.load(clazz, order);
        }
        else if(page != null) {
            records = any ? db.loadAny(clazz, page) : db.load(clazz, page);
        }
        else {
            records = any ? db.loadAny(clazz) : db.load(clazz);
        }
        return records;
    }

}
