/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.cinchapi.ccl.syntax.ConditionTree;
import com.cinchapi.common.base.Array;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * A {@link DatabaseInterface} that serves a single {@link AdHocRecord} type
 * from an in-memory data source.
 * <p>
 * An {@link AdHocDatabase} bridges programmatic data sources with Runway's
 * query interface. Data is supplied via a {@link Supplier} that is evaluated
 * on each query, allowing for dynamic or computed data.
 * </p>
 * <p>
 * Queries for the registered {@link AdHocRecord} type are resolved in-memory
 * against the supplied collection. Queries for any other type return empty
 * results.
 * </p>
 * <p>
 * To combine multiple {@link AdHocDatabase AdHocDatabases} or integrate with
 * a persistent database, use {@link FederatedRunway}.
 * </p>
 *
 * @param <T> the type of {@link AdHocRecord} served by this database
 * @author Jeff Nelson
 */
public class AdHocDatabase<T extends AdHocRecord> implements DatabaseInterface {

    /**
     * The class of {@link AdHocRecord} served by this database.
     */
    private final Class<T> clazz;

    /**
     * The data source for this database.
     */
    private final Supplier<Collection<T>> supplier;

    /**
     * Construct a new instance.
     *
     * @param clazz the {@link AdHocRecord} class this database serves
     * @param supplier the data source; invoked on each query
     */
    public AdHocDatabase(Class<T> clazz, Supplier<Collection<T>> supplier) {
        this.clazz = clazz;
        this.supplier = supplier;
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        if(handles(clazz)) {
            Set<R> filtered = doFind(criteria);
            filtered = sort(filtered, order);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Order order, Realms realms) {
        if(handles(clazz)) {
            Set<R> filtered = doFind(criteria);
            return sort(filtered, order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Page page, Realms realms) {
        if(handles(clazz)) {
            Set<R> filtered = doFind(criteria);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Realms realms) {
        if(handles(clazz)) {
            return doFind(criteria);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> filtered = doFindAny(clazz, criteria);
            filtered = sort(filtered, order);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Order order, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> filtered = doFindAny(clazz, criteria);
            return sort(filtered, order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Page page, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> filtered = doFindAny(clazz, criteria);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Realms realms) {
        if(handlesHierarchy(clazz)) {
            return doFindAny(clazz, criteria);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> R findAnyUnique(Class<R> clazz, Criteria criteria,
            Realms realms) {
        Set<R> results = findAny(clazz, criteria, realms);
        return unique(results, clazz, criteria);
    }

    @Override
    public <R extends Record> R findUnique(Class<R> clazz, Criteria criteria,
            Realms realms) {
        Set<R> results = find(clazz, criteria, realms);
        return unique(results, clazz, criteria);
    }

    @Override
    public <R extends Record> R load(Class<R> clazz, long id, Realms realms) {
        if(handles(clazz)) {
            return doLoad(id);
        }
        else {
            return null;
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Order order,
            Page page, Realms realms) {
        if(handles(clazz)) {
            Set<R> all = doLoad();
            all = sort(all, order);
            return paginate(all, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Order order,
            Realms realms) {
        if(handles(clazz)) {
            return sort(doLoad(), order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Page page,
            Realms realms) {
        if(handles(clazz)) {
            return paginate(doLoad(), page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Realms realms) {
        if(handles(clazz)) {
            return doLoad();
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz, Order order,
            Page page, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> all = doLoadAny(clazz);
            all = sort(all, order);
            return paginate(all, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz, Order order,
            Realms realms) {
        if(handlesHierarchy(clazz)) {
            return sort(doLoadAny(clazz), order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz, Page page,
            Realms realms) {
        if(handlesHierarchy(clazz)) {
            return paginate(doLoadAny(clazz), page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz, Realms realms) {
        if(handlesHierarchy(clazz)) {
            return doLoadAny(clazz);
        }
        else {
            return ImmutableSet.of();
        }
    }

    /**
     * Return the class of {@link AdHocRecord} served by this database.
     *
     * @return the registered class
     */
    public Class<T> registeredClass() {
        return clazz;
    }

    /**
     * Find all records matching the given criteria.
     *
     * @param criteria the filter criteria
     * @return matching records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doFind(Criteria criteria) {
        Predicate<T> filter = createFilter(criteria);
        return supplier.get().stream()
                .filter(filter)
                .map(record -> (R) record)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Find all records in the class hierarchy matching the given criteria.
     *
     * @param requestedClass the requested class (may be a supertype)
     * @param criteria the filter criteria
     * @return matching records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doFindAny(Class<R> requestedClass,
            Criteria criteria) {
        Predicate<T> filter = createFilter(criteria);
        return supplier.get().stream()
                .filter(record -> requestedClass.isInstance(record))
                .filter(filter)
                .map(record -> (R) record)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Load all records from the supplier.
     *
     * @return all records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doLoad() {
        return supplier.get().stream()
                .map(record -> (R) record)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Load a specific record by id.
     *
     * @param id the record id
     * @return the record, or {@code null} if not found
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> R doLoad(long id) {
        return supplier.get().stream()
                .filter(record -> record.id() == id)
                .map(record -> (R) record)
                .findFirst()
                .orElse(null);
    }

    /**
     * Load all records assignable to the requested class.
     *
     * @param requestedClass the requested class (may be a supertype)
     * @return matching records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doLoadAny(Class<R> requestedClass) {
        return supplier.get().stream()
                .filter(record -> requestedClass.isInstance(record))
                .map(record -> (R) record)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Create a filter predicate from the given criteria.
     *
     * @param criteria the filter criteria
     * @return a predicate that tests records against the criteria
     */
    private Predicate<T> createFilter(Criteria criteria) {
        ConcourseCompiler compiler = ConcourseCompiler.get();
        ConditionTree ast = (ConditionTree) compiler.parse(criteria);
        String[] keys = compiler.analyze(ast).keys()
                .toArray(Array.containing());
        return record -> compiler.evaluate(ast, record.mmap(keys));
    }

    /**
     * Return {@code true} if this database handles the exact class.
     *
     * @param requestedClass the class being queried
     * @return {@code true} if this database serves the class
     */
    private boolean handles(Class<?> requestedClass) {
        return clazz.equals(requestedClass);
    }

    /**
     * Return {@code true} if this database handles the class hierarchy.
     *
     * @param requestedClass the class being queried
     * @return {@code true} if this database serves the class or a subclass
     */
    private boolean handlesHierarchy(Class<?> requestedClass) {
        return requestedClass.isAssignableFrom(clazz);
    }

    /**
     * Apply pagination to a set of records.
     *
     * @param records the records to paginate
     * @param page the pagination parameters, or {@code null} for no pagination
     * @return the paginated records
     */
    private <R extends Record> Set<R> paginate(Set<R> records,
            @Nullable Page page) {
        if(page == null) {
            return records;
        }
        return records.stream()
                .skip(page.skip())
                .limit(page.limit())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Sort a set of records.
     *
     * @param records the records to sort
     * @param order the sort order, or {@code null} for no sorting
     * @return the sorted records
     */
    private <R extends Record> Set<R> sort(Set<R> records,
            @Nullable Order order) {
        if(order == null) {
            return records;
        }
        List<String> orderSpec = toOrderSpec(order);
        return DatabaseInterface.sort(records, orderSpec);
    }

    /**
     * Convert an {@link Order} to a list-based order specification.
     *
     * @param order the order
     * @return the list-based order specification
     */
    private static List<String> toOrderSpec(Order order) {
        List<String> components = Lists.newArrayList();
        for (OrderComponent component : order.spec()) {
            String prefix = component.direction() == Direction.ASCENDING
                    ? Record.SORT_DIRECTION_ASCENDING_PREFIX
                    : Record.SORT_DIRECTION_DESCENDING_PREFIX;
            components.add(prefix + component.key());
        }
        return components;
    }

    /**
     * Verify that a result set contains exactly one record.
     *
     * @param results the result set
     * @param clazz the queried class
     * @param criteria the query criteria
     * @return the single result, or {@code null} if empty
     * @throws DuplicateEntryException if more than one result exists
     */
    private <R extends Record> R unique(Set<R> results, Class<R> clazz,
            Criteria criteria) {
        if(results.isEmpty()) {
            return null;
        }
        else if(results.size() == 1) {
            return results.iterator().next();
        }
        else {
            throw new DuplicateEntryException(
                    new com.cinchapi.concourse.thrift.DuplicateEntryException(
                            "Multiple records match " + criteria + " in "
                                    + clazz));
        }
    }

}

