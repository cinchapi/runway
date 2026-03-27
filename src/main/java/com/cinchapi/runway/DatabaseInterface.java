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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link DatabaseInterface} provides methods for interacting with a database
 * backend.
 * <p>
 * All retrieval methods delegate to {@link #fetch(Selection...)}, which is the
 * single dispatch point that implementations must provide. The convenience
 * methods on this interface build the appropriate {@link Selection} and route
 * through {@code select}.
 *
 * @author Jeff Nelson
 */
public interface DatabaseInterface {

    /**
     * A {@link Page} that retrieves at most two results, used by unique-result
     * queries to detect duplicates without fetching the entire result set.
     */
    static Page UNIQUE_PAGINATION = Page.sized(2);

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
        return fetch(Selection.of(clazz).count());
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
        return fetch(Selection.of(clazz).where(criteria).count());
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
        return fetch(
                Selection.of(clazz).where(criteria).count().filter(filter));
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
        return fetch(Selection.of(clazz).where(criteria).count().filter(filter)
                .realms(realms));
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
        return fetch(
                Selection.of(clazz).where(criteria).count().realms(realms));
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
        return fetch(Selection.of(clazz).count().filter(filter));
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
        return fetch(Selection.of(clazz).count().filter(filter).realms(realms));
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
        return fetch(Selection.of(clazz).count().realms(realms));
    }

    /**
     * Return the number of {@link Records} across the hierarchy of
     * {@code clazz}.
     *
     * @param clazz
     * @return the number of {@link Records} in {@code clazz}.
     */
    public default <T extends Record> int countAny(Class<T> clazz) {
        return fetch(Selection.ofAny(clazz).count());
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
        return fetch(Selection.ofAny(clazz).where(criteria).count());
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
        return fetch(
                Selection.ofAny(clazz).where(criteria).count().filter(filter));
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
        return fetch(Selection.ofAny(clazz).where(criteria).count()
                .filter(filter).realms(realms));
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
        return fetch(
                Selection.ofAny(clazz).where(criteria).count().realms(realms));
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
        return fetch(Selection.ofAny(clazz).count().filter(filter));
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
        return fetch(
                Selection.ofAny(clazz).count().filter(filter).realms(realms));
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
        return fetch(Selection.ofAny(clazz).count().realms(realms));
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
        return fetch(Selection.of(clazz).where(criteria));
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
            Criteria criteria, List<String> order) {
        Set<T> records = fetch(Selection.of(clazz).where(criteria));
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
        return fetch(Selection.of(clazz).where(criteria).order(order));
    }

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
        return fetch(
                Selection.of(clazz).where(criteria).order(order).page(page));
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
            Criteria criteria, Order order, Page page, Predicate<T> filter) {
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .order(order).page(page));
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
            Criteria criteria, Order order, Page page, Predicate<T> filter,
            Realms realms) {
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .order(order).page(page).realms(realms));
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
            Criteria criteria, Order order, Page page, Realms realms) {
        return fetch(Selection.of(clazz).where(criteria).order(order).page(page)
                .realms(realms));
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
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .order(order));
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
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .order(order).realms(realms));
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
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Order order, Realms realms) {
        return fetch(Selection.of(clazz).where(criteria).order(order)
                .realms(realms));
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
        return fetch(Selection.of(clazz).where(criteria).page(page));
    }

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
        return fetch(
                Selection.of(clazz).where(criteria).order(order).page(page));
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
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .order(order).page(page));
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
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .order(order).page(page).realms(realms));
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
        return fetch(Selection.of(clazz).where(criteria).order(order).page(page)
                .realms(realms));
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
        return fetch(
                Selection.of(clazz).where(criteria).filter(filter).page(page));
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
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .page(page).realms(realms));
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
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Page page, Realms realms) {
        return fetch(
                Selection.of(clazz).where(criteria).page(page).realms(realms));
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
        return fetch(Selection.of(clazz).where(criteria).filter(filter));
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
        return fetch(Selection.of(clazz).where(criteria).filter(filter)
                .realms(realms));
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
    public default <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria, Realms realms) {
        return fetch(Selection.of(clazz).where(criteria).realms(realms));
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
        Set<T> records = fetch(Selection.of(clazz).where(criteria));
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
        return fetch(Selection.ofAny(clazz).where(criteria));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order}.
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
        Set<T> records = fetch(Selection.ofAny(clazz).where(criteria));
        return sort(records, order);
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order}.
     *
     * @param clazz
     * @param criteria
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order) {
        return fetch(Selection.ofAny(clazz).where(criteria).order(order));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order} and limited
     * to the specified {@code page}.
     *
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Page page) {
        return fetch(
                Selection.ofAny(clazz).where(criteria).order(order).page(page));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, sorted by the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .order(order).page(page));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, sorted by the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .order(order).page(page).realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order} and limited
     * to the specified {@code page} among the provided {@code realms}.
     *
     * @param clazz
     * @param criteria
     * @param order
     * @param page
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Page page, Realms realms) {
        return fetch(Selection.ofAny(clazz).where(criteria).order(order)
                .page(page).realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, sorted by the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .order(order));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, sorted by the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .order(order).realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order} among the
     * provided {@code realms}.
     *
     * @param clazz
     * @param criteria
     * @param order
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Order order, Realms realms) {
        return fetch(Selection.ofAny(clazz).where(criteria).order(order)
                .realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants limited to the specified {@code page}.
     *
     * @param clazz
     * @param criteria
     * @param page
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page) {
        return fetch(Selection.ofAny(clazz).where(criteria).page(page));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order} and limited
     * to the specified {@code page}.
     *
     * @param clazz
     * @param criteria
     * @param page
     * @param order
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Order order) {
        return fetch(
                Selection.ofAny(clazz).where(criteria).order(order).page(page));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, sorted by the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .order(order).page(page));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, sorted by the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .order(order).page(page).realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order} and limited
     * to the specified {@code page} among the provided {@code realms}.
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
        return fetch(Selection.ofAny(clazz).where(criteria).order(order)
                .page(page).realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, limited to the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .page(page));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants that pass the {@code filter}, limited to the
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .page(page).realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants limited to the specified {@code page} among the
     * provided {@code realms}.
     *
     * @param clazz
     * @param criteria
     * @param page
     * @param realms
     * @return the matching records
     */
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Page page, Realms realms) {
        return fetch(Selection.ofAny(clazz).where(criteria).page(page)
                .realms(realms));
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter));
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
        return fetch(Selection.ofAny(clazz).where(criteria).filter(filter)
                .realms(realms));
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
    public default <T extends Record> Set<T> findAny(Class<T> clazz,
            Criteria criteria, Realms realms) {
        return fetch(Selection.ofAny(clazz).where(criteria).realms(realms));
    }

    /**
     * Execute the {@link #find(Class, Criteria)} query for {@code clazz} and
     * all of its descendants sorted by the specified {@code order}.
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
        Set<T> records = fetch(Selection.ofAny(clazz).where(criteria));
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
    public default <T extends Record> T findAnyUnique(Class<T> clazz,
            Criteria criteria, Realms realms) {
        Set<T> results = fetch(Selection.ofAny(clazz).where(criteria)
                .page(UNIQUE_PAGINATION).realms(realms));
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
    public default <T extends Record> T findUnique(Class<T> clazz,
            Criteria criteria, Realms realms) {
        Set<T> results = fetch(Selection.of(clazz).where(criteria)
                .page(UNIQUE_PAGINATION).realms(realms));
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

    /**
     * Create a {@link Gateway} instance that provides intelligent routing to
     * the appropriate database operations based on the parameters provided. The
     * gateway simplifies database access by automatically choosing between
     * {@link #find} and {@link #load} operations.
     *
     * @return a new gateway instance for this database interface
     * @deprecated Use
     *             {@link Selection#of(Class, Criteria, Order, Page, Realms)} or
     *             {@link Selection#ofAny(Class, Criteria, Order, Page, Realms)}
     *             with {@link #fetch(Selection...)} instead.
     */
    @Deprecated
    public default Gateway gateway() {
        return Gateway.to(this);
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
    public default <T extends Record> Set<T> load(Class<T> clazz) {
        return fetch(Selection.of(clazz));
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
     * @deprecated Use {@link #load(Class, Order)}
     */
    @Deprecated
    public default <T extends Record> Set<T> load(Class<T> clazz,
            List<String> order) {
        Set<T> records = fetch(Selection.of(clazz));
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
        return fetch(Selection.of(clazz).id(id));
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
    public default <T extends Record> T load(Class<T> clazz, long id,
            Realms realms) {
        return fetch(Selection.of(clazz).id(id).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order}.
     *
     * @param clazz
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order) {
        return fetch(Selection.of(clazz).order(order));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} and limited to
     * the specified {@code page}.
     *
     * @param clazz
     * @param order
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page) {
        return fetch(Selection.of(clazz).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     *
     * @param clazz
     * @param order
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Predicate<T> filter) {
        return fetch(
                Selection.of(clazz).filter(filter).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
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
        return fetch(Selection.of(clazz).filter(filter).order(order).page(page)
                .realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} and limited to
     * the specified {@code page} among the {@code realms}.
     *
     * @param clazz
     * @param order
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Realms realms) {
        return fetch(
                Selection.of(clazz).order(order).page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order}.
     *
     * @param clazz
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Predicate<T> filter) {
        return fetch(Selection.of(clazz).filter(filter).order(order));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} among the {@code realms}.
     *
     * @param clazz
     * @param order
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Predicate<T> filter, Realms realms) {
        return fetch(
                Selection.of(clazz).filter(filter).order(order).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} among the
     * {@code realms}.
     *
     * @param clazz
     * @param order
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Realms realms) {
        return fetch(Selection.of(clazz).order(order).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page}.
     *
     * @param clazz
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page) {
        return fetch(Selection.of(clazz).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order} and limited to
     * the specified {@code page}.
     *
     * @param clazz
     * @param page
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Order order) {
        return fetch(Selection.of(clazz).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     *
     * @param clazz
     * @param page
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Order order, Predicate<T> filter) {
        return fetch(
                Selection.of(clazz).filter(filter).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
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
        return fetch(Selection.of(clazz).filter(filter).order(order).page(page)
                .realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page}.
     *
     * @param clazz
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Predicate<T> filter) {
        return fetch(Selection.of(clazz).filter(filter).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page} among the {@code realms}.
     *
     * @param clazz
     * @param page
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Predicate<T> filter, Realms realms) {
        return fetch(
                Selection.of(clazz).filter(filter).page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page} among the
     * {@code realms}.
     *
     * @param clazz
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Realms realms) {
        return fetch(Selection.of(clazz).page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and pass the {@code filter}.
     *
     * @param clazz
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz,
            Predicate<T> filter) {
        return fetch(Selection.of(clazz).filter(filter));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and pass the {@code filter} among the {@code realms}.
     *
     * @param clazz
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz,
            Predicate<T> filter, Realms realms) {
        return fetch(Selection.of(clazz).filter(filter).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} among the {@code realms}.
     *
     * @param clazz
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> load(Class<T> clazz,
            Realms realms) {
        return fetch(Selection.of(clazz).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and sorted using the specified {@code order}.
     *
     * @param clazz
     * @return a {@link Set set} of {@link Record} objects
     * @deprecated Use {@link #load(Class, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> load(Class<T> clazz,
            String order) {
        Set<T> records = fetch(Selection.of(clazz));
        return sort(records, order);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants.
     *
     * @param clazz
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz) {
        return fetch(Selection.ofAny(clazz));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order}.
     *
     * @param clazz
     * @return a {@link Set set} of {@link Record} objects
     * @deprecated Use {@link #loadAny(Class, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            List<String> order) {
        Set<T> records = fetch(Selection.ofAny(clazz));
        return sort(records, order);
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order}.
     *
     * @param clazz
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order) {
        return fetch(Selection.ofAny(clazz).order(order));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     *
     * @param clazz
     * @param order
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Page page) {
        return fetch(Selection.ofAny(clazz).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page}.
     *
     * @param clazz
     * @param order
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Page page, Predicate<T> filter) {
        return fetch(
                Selection.ofAny(clazz).filter(filter).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page} among the {@code realms}.
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
        return fetch(Selection.ofAny(clazz).filter(filter).order(order)
                .page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
     *
     * @param clazz
     * @param order
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Page page, Realms realms) {
        return fetch(
                Selection.ofAny(clazz).order(order).page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order}.
     *
     * @param clazz
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Predicate<T> filter) {
        return fetch(Selection.ofAny(clazz).filter(filter).order(order));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} among the {@code realms}.
     *
     * @param clazz
     * @param order
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Predicate<T> filter, Realms realms) {
        return fetch(Selection.ofAny(clazz).filter(filter).order(order)
                .realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} among {@code realms}
     *
     * @param clazz
     * @param order
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Order order, Realms realms) {
        return fetch(Selection.ofAny(clazz).order(order).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page}.
     *
     * @param clazz
     * @param page
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Page page) {
        return fetch(Selection.ofAny(clazz).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page}.
     *
     * @param clazz
     * @param page
     * @param order
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Order order) {
        return fetch(Selection.ofAny(clazz).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page}.
     *
     * @param clazz
     * @param page
     * @param order
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Order order, Predicate<T> filter) {
        return fetch(
                Selection.ofAny(clazz).filter(filter).order(order).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants that pass the {@code filter},
     * sorted using the specified {@code order} and limited to the specified
     * {@code page}.
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
        return fetch(Selection.ofAny(clazz).filter(filter).order(order)
                .page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order} and limited to the specified {@code page} among the
     * {@code realms}.
     *
     * @param clazz
     * @param page
     * @param order
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Order order, Realms realms) {
        return fetch(
                Selection.ofAny(clazz).order(order).page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page}.
     *
     * @param clazz
     * @param page
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Predicate<T> filter) {
        return fetch(Selection.ofAny(clazz).filter(filter).page(page));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} that pass the {@code filter}, limited to the specified
     * {@code page} among the {@code realms}.
     *
     * @param clazz
     * @param page
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Predicate<T> filter, Realms realms) {
        return fetch(Selection.ofAny(clazz).filter(filter).page(page)
                .realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} and limited to the specified {@code page} among the
     * {@code realms}.
     *
     * @param clazz
     * @param page
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Realms realms) {
        return fetch(Selection.ofAny(clazz).page(page).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and pass the {@code filter}.
     *
     * @param clazz
     * @param filter
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Predicate<T> filter) {
        return fetch(Selection.ofAny(clazz).filter(filter));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and pass the {@code filter} amomg
     * the {@code realms}.
     *
     * @param clazz
     * @param filter
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Predicate<T> filter, Realms realms) {
        return fetch(Selection.ofAny(clazz).filter(filter).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants among the {@code realms}.
     *
     * @param clazz
     * @param realms
     * @return a {@link Set set} of {@link Record} objects
     */
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Realms realms) {
        return fetch(Selection.ofAny(clazz).realms(realms));
    }

    /**
     * Load all the Records that are contained within the specified
     * {@code clazz} or any of its descendants and sorted using the specified
     * {@code order}.
     *
     * @param clazz
     * @return a {@link Set set} of {@link Record} objects
     * @deprecated Use {@link #loadAny(Class, Order)} instead
     */
    @Deprecated
    public default <T extends Record> Set<T> loadAny(Class<T> clazz,
            String order) {
        Set<T> records = fetch(Selection.ofAny(clazz));
        return sort(records, order);
    }

    /**
     * Load the {@link Record} that is contained within the specified
     * {@code clazz} and has the specified {@code id}, throwing an
     * {@link IllegalStateException} if no such {@link Record} exists.
     * <p>
     * This method provides a fail-fast alternative to
     * {@link #load(Class, long)} for cases where the caller expects the
     * {@link Record} to exist and considers its absence to be an error
     * condition.
     * </p>
     *
     * @param clazz the {@link Record} type
     * @param id the {@link Record} id
     * @param <T> the {@link Record} type
     * @return the existing {@link Record}
     * @throws IllegalStateException if no {@link Record} with the specified
     *             {@code id} exists
     */
    public default <T extends Record> T loadNullSafe(Class<T> clazz, long id) {
        return loadNullSafe(clazz, id, Realms.any());
    }

    /**
     * Load the {@link Record} that is contained within the specified
     * {@code clazz} and has the specified {@code id} if it exists in any of the
     * {@code realms}, throwing an {@link IllegalStateException} if no such
     * {@link Record} exists.
     * <p>
     * This method provides a fail-fast alternative to
     * {@link #load(Class, long, Realms)} for cases where the caller expects the
     * {@link Record} to exist and considers its absence to be an error
     * condition.
     * </p>
     *
     * @param clazz the {@link Record} type
     * @param id the {@link Record} id
     * @param realms the {@link Realms} to search
     * @param <T> the {@link Record} type
     * @return the existing {@link Record}
     * @throws IllegalStateException if no {@link Record} with the specified
     *             {@code id} exists in any of the {@code realms}
     */
    public default <T extends Record> T loadNullSafe(Class<T> clazz, long id,
            Realms realms) {
        T record = fetch(Selection.of(clazz).id(id).realms(realms));
        if(record != null) {
            return record;
        }
        else {
            throw new IllegalStateException();
        }
    }

    /**
     * Execute a single {@link Selection} and return the result directly, cast
     * to the appropriate type.
     * <p>
     * The return type is inferred from the calling context &mdash; a
     * {@link java.util.Set Set} for find and load queries, a single
     * {@link Record} for load-by-id queries, or an {@link Integer} for count
     * queries.
     *
     * @param selection the {@link Selection} to execute
     * @param <R> the expected result type
     * @return the result of the {@link Selection}
     * @throws IllegalStateException if the {@link Selection} has already been
     *             submitted
     */
    public default <R> R fetch(Selection<?> selection) {
        return select(selection).next();
    }

    /**
     * Execute one or more {@link Selection Selections} and return a
     * {@link Selections} wrapper for positional access to the results.
     * <p>
     * This is the single dispatch point for all retrieval operations. Every
     * other read method on this interface delegates here. Subclasses provide
     * the actual query execution, caching, and optimization.
     * </p>
     *
     * @param selections the {@link Selection Selections} to execute
     * @return a {@link Selections} wrapper for positional access
     * @throws IllegalArgumentException if the input array is empty
     * @throws IllegalStateException if any {@link Selection} has already been
     *             submitted
     */
    public Selections select(Selection<?>... selections);

}
