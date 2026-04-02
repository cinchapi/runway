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

import static com.cinchapi.runway.DatabaseInterface.duplicateEntryException;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Direction;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.lang.sort.OrderComponent;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * A {@link DatabaseInterface} that serves a single {@link AdHocRecord} type
 * from an in-memory data source.
 * <p>
 * An {@link AdHocDataSource} bridges programmatic data sources with Runway's
 * query interface. Data is supplied via a {@link Supplier} that is evaluated on
 * each query, allowing for dynamic or computed data.
 * </p>
 * <p>
 * Queries for the registered {@link AdHocRecord} type are resolved in-memory
 * against the supplied collection. Queries for any other type return empty
 * results.
 * </p>
 * <p>
 * <strong>Note on Realms:</strong> {@link AdHocRecord AdHocRecords} do not have
 * realm associations. While query methods accept {@link Realms} parameters for
 * API compatibility, the realm constraints are not applied to ad-hoc data. All
 * records from the supplier are considered visible regardless of the specified
 * realms.
 * </p>
 * <p>
 * To use this source with a {@link Runway} instance, attach it using
 * {@link Runway#attach(AdHocDataSource...)}:
 * </p>
 *
 * <pre>
 * {
 *     &#64;code AdHocDataSource<ReportRecord> reports = new AdHocDataSource<>(
 *             ReportRecord.class, () -> generateReports());
 *
 *     try (DatabaseInterface db = runway.attach(reports)) {
 *         db.load(ReportRecord.class); // Served from reports source
 *     }
 * }
 * </pre>
 *
 * @param <T> the type of {@link AdHocRecord} served by this source
 * @author Jeff Nelson
 */
public class AdHocDataSource<T extends AdHocRecord> implements
        DatabaseInterface {

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
     * The class of {@link AdHocRecord} served by this source.
     */
    private final Class<T> clazz;

    /**
     * The data source for this database.
     */
    private final Supplier<Collection<T>> supplier;

    /**
     * Construct a new instance.
     *
     * @param clazz the {@link AdHocRecord} class this source serves
     * @param supplier the data source; invoked on each query
     */
    public AdHocDataSource(Class<T> clazz, Supplier<Collection<T>> supplier) {
        this.clazz = clazz;
        this.supplier = supplier;
    }

    @Override
    public <R extends Record> int count(Class<R> clazz, Criteria criteria,
            Predicate<R> filter, Realms realms) {
        return find(clazz, criteria, filter, realms).size();
    }

    @Override
    public <R extends Record> int count(Class<R> clazz, Predicate<R> filter,
            Realms realms) {
        return load(clazz, filter, realms).size();
    }

    @Override
    public <R extends Record> int countAny(Class<R> clazz, Criteria criteria,
            Predicate<R> filter, Realms realms) {
        return findAny(clazz, criteria, filter, realms).size();
    }

    @Override
    public <R extends Record> int countAny(Class<R> clazz, Predicate<R> filter,
            Realms realms) {
        return loadAny(clazz, filter, realms).size();
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Order order, Page page, Predicate<R> filter, Realms realms) {
        if(handles(clazz)) {
            Set<R> filtered = doFind(criteria, filter);
            filtered = sort(filtered, order);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Order order, Predicate<R> filter, Realms realms) {
        if(handles(clazz)) {
            Set<R> filtered = doFind(criteria, filter);
            return sort(filtered, order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Page page, Predicate<R> filter, Realms realms) {
        if(handles(clazz)) {
            Set<R> filtered = doFind(criteria, filter);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> find(Class<R> clazz, Criteria criteria,
            Predicate<R> filter, Realms realms) {
        if(handles(clazz)) {
            return doFind(criteria, filter);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Order order, Page page, Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> filtered = doFindAny(clazz, criteria, filter);
            filtered = sort(filtered, order);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Order order, Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> filtered = doFindAny(clazz, criteria, filter);
            return sort(filtered, order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Page page, Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> filtered = doFindAny(clazz, criteria, filter);
            return paginate(filtered, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> findAny(Class<R> clazz, Criteria criteria,
            Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            return doFindAny(clazz, criteria, filter);
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
            return doLoad(id, r -> true);
        }
        else {
            return null;
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Order order,
            Page page, Predicate<R> filter, Realms realms) {
        if(handles(clazz)) {
            Set<R> all = doLoad(filter);
            all = sort(all, order);
            return paginate(all, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Order order,
            Predicate<R> filter, Realms realms) {
        if(handles(clazz)) {
            return sort(doLoad(filter), order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Page page,
            Predicate<R> filter, Realms realms) {
        if(handles(clazz)) {
            return paginate(doLoad(filter), page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> load(Class<R> clazz, Predicate<R> filter,
            Realms realms) {
        if(handles(clazz)) {
            return doLoad(filter);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz, Order order,
            Page page, Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            Set<R> all = doLoadAny(clazz, filter);
            all = sort(all, order);
            return paginate(all, page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz, Order order,
            Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            return sort(doLoadAny(clazz, filter), order);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz, Page page,
            Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            return paginate(doLoadAny(clazz, filter), page);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    public <R extends Record> Set<R> loadAny(Class<R> clazz,
            Predicate<R> filter, Realms realms) {
        if(handlesHierarchy(clazz)) {
            return doLoadAny(clazz, filter);
        }
        else {
            return ImmutableSet.of();
        }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Selections select(Selection<?>... options) {
        Preconditions.checkArgument(options.length > 0);
        DatabaseSelection<?>[] selections = Arrays.stream(options)
                .map(DatabaseSelection::resolve)
                .toArray(DatabaseSelection[]::new);
        for (DatabaseSelection<?> selection : selections) {
            if(selection.state == Selection.State.RESOLVED) {
                selection.state = Selection.State.FINISHED;
                continue; /* (authorized short circuit) */
            }
            selection.ensurePending();
            selection.state = Selection.State.SUBMITTED;
            if(selection instanceof CountSelection) {
                // NOTE: This path isn't consolidated because #count has
                // different codepaths with vs without a Criteria
                CountSelection<?> s = (CountSelection<?>) selection;
                Predicate filter = s.filter;
                if(s.criteria != null) {
                    s.result = s.any
                            ? countAny(s.clazz, s.criteria, filter, s.realms)
                            : count(s.clazz, s.criteria, filter, s.realms);
                }
                else {
                    s.result = s.any ? countAny(s.clazz, filter, s.realms)
                            : count(s.clazz, filter, s.realms);
                }
            }
            else if(selection instanceof LoadRecordSelection) {
                LoadRecordSelection<?> s = (LoadRecordSelection<?>) selection;
                s.result = load(s.clazz, s.id, s.realms);
            }
            else if(selection instanceof FindSelection) {
                FindSelection<?> s = (FindSelection<?>) selection;
                Predicate filter = s.filter;
                s.result = s.any
                        ? findAny(s.clazz, s.criteria, s.order, s.page, filter,
                                s.realms)
                        : find(s.clazz, s.criteria, s.order, s.page, filter,
                                s.realms);
            }
            else if(selection instanceof LoadClassSelection) {
                LoadClassSelection<?> s = (LoadClassSelection<?>) selection;
                Predicate filter = s.filter;
                s.result = s.any
                        ? loadAny(s.clazz, s.order, s.page, filter, s.realms)
                        : load(s.clazz, s.order, s.page, filter, s.realms);
            }
            else if(selection instanceof UniqueSelection) {
                UniqueSelection<?> s = (UniqueSelection<?>) selection;
                Predicate filter = s.filter;
                Set results;
                if(s.criteria != null) {
                    results = s.any
                            ? findAny(s.clazz, s.criteria, filter, s.realms)
                            : find(s.clazz, s.criteria, filter, s.realms);
                }
                else {
                    results = s.any ? loadAny(s.clazz, filter, s.realms)
                            : load(s.clazz, filter, s.realms);
                }
                s.result = unique(results, s.clazz, s.criteria);
            }
            else {
                throw new UnsupportedOperationException(
                        "Unsupported Selection type " + selection.getClass());
            }
            selection.state = Selection.State.FINISHED;
        }
        return new Selections(selections);
    }

    /**
     * Return the {@link AdHocRecord} {@link Class} served by this data source.
     *
     * @return the {@link AdHocRecord} class this source serves
     */
    public Class<T> type() {
        return clazz;
    }

    /**
     * Create a filter predicate from the given criteria.
     *
     * @param criteria the filter criteria
     * @return a predicate that tests records against the criteria
     */
    private Predicate<T> createFilter(Criteria criteria) {
        return record -> record.matches(criteria);
    }

    /**
     * Find all {@link Record Records} matching the given criteria and passing
     * the {@code inputFilter}.
     *
     * @param criteria the filter criteria
     * @param inputFilter an additional predicate applied after the criteria
     *            match
     * @return matching records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doFind(Criteria criteria,
            Predicate<R> inputFilter) {
        Predicate<T> filter = createFilter(criteria);
        return supplier.get().stream().filter(filter).map(record -> (R) record)
                .filter(inputFilter)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Find all {@link Record Records} in the class hierarchy matching the given
     * criteria and passing the {@code inputFilter}.
     *
     * @param requestedClass the requested class (may be a supertype)
     * @param criteria the filter criteria
     * @param inputFilter an additional predicate applied after the criteria
     *            match
     * @return matching records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doFindAny(Class<R> requestedClass,
            Criteria criteria, Predicate<R> inputFilter) {
        Predicate<T> filter = createFilter(criteria);
        return supplier.get().stream()
                .filter(record -> requestedClass.isInstance(record))
                .filter(filter).map(record -> (R) record).filter(inputFilter)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Load all {@link Record Records} from the supplier that pass the
     * {@code inputFilter}.
     *
     * @param inputFilter a predicate that each record must satisfy to be
     *            included
     * @return matching records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doLoad(Predicate<R> inputFilter) {
        return supplier.get().stream().map(record -> (R) record)
                .filter(inputFilter)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Load a specific {@link Record} by id that passes the {@code inputFilter}.
     *
     * @param id the record id
     * @param inputFilter a predicate that the record must satisfy to be
     *            returned
     * @return the record, or {@code null} if not found or filtered out
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> R doLoad(long id, Predicate<R> inputFilter) {
        return supplier.get().stream().filter(record -> record.id() == id)
                .map(record -> (R) record).filter(inputFilter).findFirst()
                .orElse(null);
    }

    /**
     * Load all {@link Record Records} assignable to the requested class that
     * pass the {@code inputFilter}.
     *
     * @param requestedClass the requested class (may be a supertype)
     * @param inputFilter a predicate that each record must satisfy to be
     *            included
     * @return matching records
     */
    @SuppressWarnings("unchecked")
    private <R extends Record> Set<R> doLoadAny(Class<R> requestedClass,
            Predicate<R> inputFilter) {
        return supplier.get().stream()
                .filter(record -> requestedClass.isInstance(record))
                .map(record -> (R) record).filter(inputFilter)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Return {@code true} if this source handles the exact class.
     *
     * @param requestedClass the class being queried
     * @return {@code true} if this source serves the class
     */
    private boolean handles(Class<?> requestedClass) {
        return clazz.equals(requestedClass);
    }

    /**
     * Return {@code true} if this source handles the class hierarchy.
     *
     * @param requestedClass the class being queried
     * @return {@code true} if this source serves the class or a subclass
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
        return records.stream().skip(page.skip()).limit(page.limit())
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
            throw duplicateEntryException("Multiple records match {} in {} ",
                    criteria, clazz);
        }
    }

}
