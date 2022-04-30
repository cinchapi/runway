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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.runway.util.Pagination;
import com.google.common.collect.Sets;

/**
 * A {@link DatabaseInterface} provides methods for interacting with a database
 * backend.
 *
 * @author Jeff Nelson
 */
public interface DatabaseInterface {

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
     * Return the number of {@link Records} in the {@code clazz}.
     * 
     * @param clazz
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int count(Class<T> clazz) {
        return count(clazz, Realms.any());
    }

    /**
     * Return the number of {@link Records} in the {@code clazz} among the
     * provided {@code realms}.
     * 
     * @param clazz
     * @param realms
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int count(Class<T> clazz, Realms realms) {
        return load(clazz, realms).size();
    }

    /**
     * Return the number of {@link Records} in the {@code clazz} that match the
     * {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int count(Class<T> clazz,
            Criteria criteria) {
        return count(clazz, criteria, Realms.any());
    }

    /**
     * Return the number of {@link Records} in the {@code clazz} that match the
     * {@code criteria} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param realms
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int count(Class<T> clazz,
            Criteria criteria, Realms realms) {
        return find(clazz, criteria, realms).size();
    }

    /**
     * Return the number of {@link Records} in the {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}
     * 
     * @param clazz
     * @param filter
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int count(Class<T> clazz,
            Criteria criteria, Predicate<T> filter) {
        return count(clazz, criteria, filter, Realms.any());
    }

    /**
     * Return the number of {@link Records} in the {@code clazz} that match the
     * {@code criteria} and pass the {@code filter} among the provided
     * {@code realms}.
     * 
     * @param clazz
     * @param filter
     * @param realms
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int count(Class<T> clazz,
            Criteria criteria, Predicate<T> filter, Realms realms) {
        return find(clazz, criteria, filter, realms).size();
    }

    /**
     * Return the number of {@link Records} in the {@code clazz} that pass the
     * {@code filter}.
     * 
     * @param clazz
     * @param filter
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int count(Class<T> clazz,
            Predicate<T> filter) {
        return count(clazz, filter, Realms.any());
    }

    /**
     * Return the number of {@link Records} in the {@code clazz} that pass the
     * {@code filter} among the provided {@code realms}.
     * 
     * @param clazz
     * @param filter
     * @param realms
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int count(Class<T> clazz,
            Predicate<T> filter, Realms realms) {
        return load(clazz, filter, realms).size();
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz}.
     * 
     * @param clazz
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int countAny(Class<T> clazz) {
        return countAny(clazz, Realms.any());
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz} among the provided {@code realms}.
     * 
     * @param clazz
     * @param realms
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int countAny(Class<T> clazz,
            Realms realms) {
        return loadAny(clazz, realms).size();
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz} that match the {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int countAny(Class<T> clazz,
            Criteria criteria) {
        return countAny(clazz, criteria, Realms.any());
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz} that match the {@code criteria} among the provided
     * {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param realms
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int countAny(Class<T> clazz,
            Criteria criteria, Realms realms) {
        return findAny(clazz, criteria, realms).size();
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz} that match the {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @param filter
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int countAny(Class<T> clazz,
            Criteria criteria, Predicate<T> filter) {
        return countAny(clazz, criteria, filter, Realms.any());
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz} that match the {@code criteria} among the provided
     * {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param filter
     * @param realms
     * @return the number of {@link Records} in {@code clazz} that match the
     *         {@code criteria}.
     */
    public default <T extends Record> int countAny(Class<T> clazz,
            Criteria criteria, Predicate<T> filter, Realms realms) {
        return findAny(clazz, criteria, filter, realms).size();
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz} that pass the {@code filter}
     * 
     * @param clazz
     * @param filter
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int countAny(Class<T> clazz,
            Predicate<T> filter) {
        return countAny(clazz, filter, Realms.any());
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz} that pass the {@code filter} among the provided
     * {@code realms}.
     * 
     * @param clazz
     * @param filter
     * @param realms
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int countAny(Class<T> clazz,
            Predicate<T> filter, Realms realms) {
        return loadAny(clazz, filter, realms).size();
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
            Criteria criteria) {
        return find(clazz, criteria, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Realms realms);

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     * @deprecated Use {@link #find(Class, Criteria, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, List<String> order) {
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
            Criteria criteria, Order order) {
        return find(clazz, criteria, order, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order} among the provided
     * {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Realms realms);

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order} and limited to the
     * specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Order order, Page page) {
        return find(clazz, criteria, order, page, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order} and limited to the
     * specified {@code page} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms);

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}, sorted by the specified
     * {@code order} and limited to the
     * specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Order order, Page page, Predicate<T> filter) {
        return find(clazz, criteria, order, page, filter, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}, sorted by the specified
     * {@code order} and limited to the
     * specified {@code page} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Order order, Page page, Predicate<T> filter,
            Realms realms) {
        return Pagination.filterAndPaginate(
                $page -> find(clazz, criteria, order, $page, realms), filter,
                page);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}, sorted by the specified
     * {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Order order, Predicate<T> filter) {
        return find(clazz, criteria, order, filter, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}, sorted by the specified
     * {@code order} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Order order, Predicate<T> filter,
            Realms realms) {
        Set<T> unfiltered = find(clazz, criteria, order, realms);
        return Sets.filter(unfiltered, filter::test);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} limited to the specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page) {
        return find(clazz, criteria, page, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} limited to the specified {@code page} among the provided
     * {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Page page, Realms realms);

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order} and limited to the
     * specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page, Order order) {
        return find(clazz, criteria, page, order, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order} and limited to the
     * specified {@code page} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page, Order order, Realms realms) {
        return find(clazz, criteria, order, page, realms);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}, sorted by the specified
     * {@code order} and limited to the specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page, Order order, Predicate<T> filter) {
        return find(clazz, criteria, page, order, filter, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}, sorted by the specified
     * {@code order} and limited to the specified {@code page} among the
     * provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page, Order order, Predicate<T> filter,
            Realms realms) {
        return find(clazz, criteria, order, page, filter, realms);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter} limited to the specified
     * {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page, Predicate<T> filter) {
        return find(clazz, criteria, page, filter, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter} limited to the specified
     * {@code page} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page, Predicate<T> filter, Realms realms) {
        return Pagination.filterAndPaginate(
                $page -> find(clazz, criteria, $page, realms), filter, page);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter}.
     * 
     * @param clazz
     * @param criteria
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Predicate<T> filter) {
        return find(clazz, criteria, filter, Realms.any());
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} and pass the {@code filter} among the provided
     * {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Predicate<T> filter, Realms realms) {
        Set<T> unfiltered = find(clazz, criteria, realms);
        return Sets.filter(unfiltered, filter::test);
    }

    /**
     * Find and return all the records of type {@code clazz} that match the
     * {@code criteria} sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     * @deprecated Use {@link #find(Class, Criteria, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, String order) {
        Set<T> records = find(clazz, criteria);
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
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria) {
        return findAny(clazz, criteria, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Realms realms);

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     * @deprecated Use {@link #findAny(Class, Criteria, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, List<String> order) {
        Set<T> records = findAny(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order) {
        return findAny(clazz, criteria, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order} among
     * the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Realms realms);

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order} and
     * limited to the specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Page page) {
        return findAny(clazz, criteria, order, page, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order} and
     * limited to the specified {@code page} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms);

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, sorted by the
     * specified {@code order} and limited to the specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Page page, Predicate<T> filter) {
        return findAny(clazz, criteria, order, page, filter, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, sorted by the
     * specified {@code order} and limited to the specified {@code page} among
     * the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Page page, Predicate<T> filter,
            Realms realms) {
        return Pagination.filterAndPaginate(
                $page -> findAny(clazz, criteria, order, $page, realms), filter,
                page);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, sorted by the
     * specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Predicate<T> filter) {
        return findAny(clazz, criteria, order, filter, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, sorted by the
     * specified {@code order} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Predicate<T> filter,
            Realms realms) {
        Set<T> unfiltered = findAny(clazz, criteria, order, realms);
        return Sets.filter(unfiltered, filter::test);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants limited to the specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page) {
        return findAny(clazz, criteria, page, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants limited to the specified {@code page} among
     * the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param realms
     * @return the matching records
     */
    public <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Page page, Realms realms);

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order} and
     * limited to the specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Order order) {
        return findAny(clazz, criteria, page, order, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order} and
     * limited to the specified {@code page} among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param order
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Order order, Realms realms) {
        return findAny(clazz, criteria, order, page, realms);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, sorted by the
     * specified {@code order} and limited to the specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param order
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Order order, Predicate<T> filter) {
        return findAny(clazz, criteria, page, order, filter, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, sorted by the
     * specified {@code order} and limited to the specified {@code page} among
     * the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param order
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Order order, Predicate<T> filter,
            Realms realms) {
        return findAny(clazz, criteria, order, page, filter, realms);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, limited to the
     * specified {@code page}.
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Predicate<T> filter) {
        return findAny(clazz, criteria, page, filter, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants that pass the {@code filter}, limited to the
     * specified {@code page} among the provided {@code realms}
     * 
     * @param clazz
     * @param criteria
     * @param page
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Predicate<T> filter, Realms realms) {
        return Pagination.filterAndPaginate(
                $page -> findAny(clazz, criteria, $page, realms), filter, page);

    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants and return those that pass the {@code filter}.
     * 
     * @param clazz
     * @param criteria
     * @param filter
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Predicate<T> filter) {
        return findAny(clazz, criteria, filter, Realms.any());
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants and return those that pass the {@code filter}
     * among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param filter
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Predicate<T> filter, Realms realms) {
        Set<T> unfiltered = findAny(clazz, criteria, realms);
        return Sets.filter(unfiltered, filter::test);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz}
     * and all of its descendants sorted by the specified {@code order}.
     * 
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     * @deprecated Use {@link #findAny(Class, Criteria, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, String order) {
        Set<T> records = findAny(clazz, criteria);
        return sort(records, order);
    }

    /**
     * Execute the {@link #findUnique(Class, Criteria)} query for {@code clazz}
     * and all of its descendants.
     * 
     * @param clazz
     * @param criteria
     * @return the one matching record
     */
    public default <T extends Record> T findAnyUnique(Class<T> clazz,
            Criteria criteria) {
        return findAnyUnique(clazz, criteria, Realms.any());
    }

    /**
     * Execute the {@link #findUnique(Class, Criteria)} query for {@code clazz}
     * and all of its descendants among the provided {@code realms}.
     * 
     * @param clazz
     * @param criteria
     * @param realms
     * @return the one matching record
     */
    public <T extends Record> T findAnyUnique(Class<T> clazz, Criteria criteria,
            Realms realms);

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
            Criteria criteria) {
        return findUnique(clazz, criteria, Realms.any());
    }

    /**
     * Find the one record of type {@code clazz} that matches the
     * {@code criteria} among the provided {@code realms}. If more than one
     * record matches, throw a {@link DuplicateEntryException}.
     * 
     * @param clazz
     * @param criteria
     * @param realms
     * @return the one matching record
     * @throws DuplicateEntryException
     */
    public <T extends Record> T findUnique(Class<T> clazz, Criteria criteria,
            Realms realms);

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
    public default <T extends Record> Set<T> load(Class<T> clazz) {
        return load(clazz, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> load(Class<T> clazz, Realms realms);

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
     * @deprecated Use {@link #load(Class, Order)}
     */
    @Deprecated
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
    public default <T extends Record> T load(Class<T> clazz, long id) {
        return load(clazz, id, Realms.any());
    }

    /**
     * Load the Record that is contained within the specified {@code clazz} and
     * has the specified {@code id} if it exist in any of the {@code realms}.
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
    public <T extends Record> T load(Class<T> clazz, long id, Realms realms);

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
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order) {
        return load(clazz, order, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} among the
     * {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Realms realms);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} and limited to
     * the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page) {
        return load(clazz, order, page, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} and limited to
     * the specified {@code page} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Realms realms);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Predicate<T> filter) {
        return load(clazz, order, page, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Predicate<T> filter, Realms realms) {
        return Pagination.filterAndPaginate(
                $page -> load(clazz, order, $page, realms), filter, page);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
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
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Predicate<T> filter) {
        return load(clazz, order, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Predicate<T> filter, Realms realms) {
        Set<T> unfiltered = load(clazz, order, realms);
        return Sets.filter(unfiltered, filter::test);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page) {
        return load(clazz, page, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page} among the
     * {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Realms realms);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} and limited to
     * the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Order order) {
        return load(clazz, order, page);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Order order, Predicate<T> filter) {
        return load(clazz, page, order, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param order
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Order order, Predicate<T> filter, Realms realms) {
        return load(clazz, order, page, filter, realms);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Predicate<T> filter) {
        return load(clazz, page, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Predicate<T> filter, Realms realms) {
        return Pagination.filterAndPaginate($page -> load(clazz, $page, realms),
                filter, page);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and pass the {@code filter}.
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
            Predicate<T> filter) {
        return load(clazz, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and pass the {@code filter} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz,
            Predicate<T> filter, Realms realms) {
        Set<T> unfiltered = load(clazz, realms);
        return Sets.filter(unfiltered, filter::test);
    }

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
     * @deprecated Use {@link #load(Class, Order)} instead
     */
    @Deprecated
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
    public default <T extends Record> Set<T> loadAny(Class<T> clazz) {
        return loadAny(clazz, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Realms realms);

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
     * @deprecated Use {@link #loadAny(Class, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            List<String> order) {
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
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order) {
        return loadAny(clazz, order, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} among {@code realms}
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Realms realms);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Page page) {
        return loadAny(clazz, order, page, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Page page, Realms realms);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Page page, Predicate<T> filter) {
        return loadAny(clazz, order, page, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param page
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Page page, Predicate<T> filter, Realms realms) {
        return Pagination.filterAndPaginate(
                $page -> loadAny(clazz, order, $page, realms), filter, page);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Predicate<T> filter) {
        return loadAny(clazz, order, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param order
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Predicate<T> filter, Realms realms) {
        Set<T> unfiltered = loadAny(clazz, order, realms);
        return Sets.filter(unfiltered, filter::test);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Page page) {
        return loadAny(clazz, page, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page} among the
     * {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Realms realms);

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Order order) {
        return loadAny(clazz, order, page, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param order
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Order order, Realms realms) {
        return loadAny(clazz, page, order, realms);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Order order, Predicate<T> filter) {
        return loadAny(clazz, page, order, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param order
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Order order, Predicate<T> filter, Realms realms) {
        return loadAny(clazz, order, page, filter, realms);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Predicate<T> filter) {
        return loadAny(clazz, page, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page} among the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param page
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Predicate<T> filter, Realms realms) {
        return Pagination.filterAndPaginate($page -> loadAny(clazz, $page, realms),
                filter, page);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and pass the {@code filter}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Predicate<T> filter) {
        return loadAny(clazz, filter, Realms.any());
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and pass the {@code filter} amomg
     * the {@code realms}.
     * 
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Predicate<T> filter, Realms realms) {
        Set<T> unfiltered = loadAny(clazz, realms);
        return Sets.filter(unfiltered, filter::test);
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
     * @deprecated Use {@link #loadAny(Class, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            String order) {
        Set<T> records = loadAny(clazz);
        return sort(records, order);
    }

}