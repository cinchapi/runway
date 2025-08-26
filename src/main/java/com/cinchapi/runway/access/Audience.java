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
package com.cinchapi.runway.access;

import static com.cinchapi.runway.access.AccessControl.ALL_KEYS;
import static com.cinchapi.runway.access.AccessControl.NO_KEYS;
import static com.cinchapi.runway.access.AccessControlSupport.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.collect.Association;
import com.cinchapi.common.collect.MergeStrategies;
import com.cinchapi.common.collect.Sequences;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.runway.DatabaseInterface;
import com.cinchapi.runway.Realms;
import com.cinchapi.runway.Record;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;

/**
 * A {@link Record} that can "perform" database operations on other records
 * (e.g., a user) and is therefore subject to permissions and rules with respect
 * to {@link AccessControl access controlled} records. {@link Framing} are a
 * key component of the access control paradigm within the framework where
 * granular access rules for various operations can be defined.
 * <p>
 * This interface extends {@link DatabaseInterface}, enabling idiomatic and
 * semantic database operations that are being "performed" by the
 * {@link Audience}. When these operations are executed through an
 * {@link Audience} instance, the framework automatically applies and respects
 * the access rules defined for that {@link Audience} on every
 * {@link AccessControl access controlled} record it touches.
 * </p>
 * <p>
 * For example, instead of using {@code runway.load(Movie.class, 1)}, which
 * bypasses access controls, one would use
 * {@code user.load(Movie.class, 1)} where {@code user} is an
 * {@link Audience}. This ensures that the framework only returns the movie if
 * the {@code user} is permitted to see it.
 * </p>
 * <h2>Specialized CRUD Methods</h2>
 * As part of the access control framework, {@link Audience} defines specialized
 * CRUD methods that enforce access controls:
 * <ul>
 * <li>{@link #create(Class, Object...)} &mdash; vs. using a constructor
 * directly</li>
 * <li>{@link #read(String, Record)} &mdash; vs. {@link Record#get(String)}</li>
 * <li>{@link #write(String, Object, Record)} &mdash; vs.
 * {@link Record#set(String, Object)}</li>
 * <li>{@link #delete(Record)} &mdash; vs. {@link Record#delete()}</li>
 * <li>{@link #frame(Record)} &mdash; a variation of {@code read} that filters
 * out inaccessible data instead of throwing a
 * {@link RestrictedAccessException}</li>
 * </ul>
 * <h2>Anonymous Access</h2>
 * <p>
 * In contexts where the current {@link Audience} is unknown (e.g., an API
 * request without a logged-in user), the {@link #anonymous()} method provides a
 * default {@link Audience} that can be used to interact with the access
 * control system consistently.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Audience extends DatabaseInterface {

    /**
     * Return a singleton {@link Audience} that represents an unauthenticated or
     * unknown user.
     * <p>
     * In a context where there is no known audience (e.g., an API request is
     * made without a logged-in user session), this method should be called to
     * get an {@link Audience} that is interoperable with the rest of the access
     * control framework.
     * </p>
     *
     * @return the anonymous {@link Audience}
     */
    public static Audience anonymous() {
        return Anonymous.get();
    }

    /**
     * Return a {@link Predicate} that tests whether a {@link Record} is visible
     * to this {@link Audience}.
     * <p>
     * This is a framework-private method and should not be called directly.
     * </p>
     *
     * @return a {@link Predicate} to filter for visible records
     */
    @SuppressWarnings("unlikely-arg-type")
    public default <T extends Record> Predicate<T> $checkIfVisible() {
        // TODO: make private in Java 9+
        return record -> {
            if(record instanceof AccessControl) {
                AccessControl subject = (AccessControl) record;
                if(subject.equals(this)) {
                    return true; // By convention, an Audience always has access
                                 // to itself
                }
                else if(this instanceof Anonymous) {
                    return subject.$isDiscoverableByAnonymous();
                }
                else {
                    // It is assumed that a record that can be read or written
                    // is implicitly discoverable. So, this cascading check
                    // protects against cases where the lower "discover"
                    // visibility isn't explicitly marked for an Audience
                    // because the implementing class assumes that specifying
                    // read/write visibility is enough.
                    return subject.$isDiscoverableBy(this)
                            || subject.$readableBy(this) != NO_KEYS
                            || subject.$writableBy(this) != NO_KEYS;
                }
            }
            else {
                return true;
            }
        };
    }

    /**
     * Return the appropriate {@link DatabaseInterface} to which database
     * operations should be delegated.
     * <p>
     * This is a framework-private method and should not be called directly.
     * </p>
     *
     * @return the {@link DatabaseInterface}
     */
    public default DatabaseInterface $db() {
        // TODO: make private in Java 9+
        if(this instanceof Record) {
            return Reflection.get("db", this);
        }
        else {
            throw new IllegalStateException(
                    "Illegal attempt to apply the Audience interface to a non-Record type: "
                            + this.getClass());
        }
    }

    /**
     * Create a new {@link Record} of the specified {@code clazz} on behalf of
     * this {@link Audience}.
     * <p>
     * This method verifies that this {@link Audience} is permitted to create
     * the {@link Record} before instantiation. The returned {@link Record} is
     * not saved to the database until {@link Record#save()} is called.
     * </p>
     *
     * @param clazz the type of {@link Record} to create
     * @param args constructor arguments for the {@link Record}
     * @param <T> the type of {@link Record}
     * @return the newly created {@link Record}, not yet saved
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to create the {@link Record}
     */
    public default <T extends Record> T create(Class<T> clazz, Object... args)
            throws RestrictedAccessException {
        T record = Reflection.newInstance(clazz, args);
        if(record instanceof AccessControl) {
            AccessControl subject = (AccessControl) record;
            if((this instanceof Anonymous && !subject.$isCreatableByAnonymous())
                    || (!(this instanceof Anonymous)
                            && !subject.$isCreatableBy(this))) {
                throw new RestrictedAccessException();
            }
        }
        if(this instanceof Record) {
            Reflection.set("_author", (Record) this, record);
        }
        return record;
    }

    /**
     * Delete the {@code record} on behalf of this {@link Audience}.
     * <p>
     * This method verifies that this {@link Audience} is permitted to delete
     * the {@code record} before marking it for deletion. The {@code record} is
     * not deleted from the database until {@link Record#save()} is called.
     * </p>
     *
     * @param record the {@link Record} to delete
     * @param <T> the type of the {@link Record}
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to delete the {@code record}
     */
    public default <T extends Record> void delete(T record)
            throws RestrictedAccessException {
        if(record instanceof AccessControl) {
            if(!((AccessControl) record).$isDeletableBy(this)) {
                throw new RestrictedAccessException();
            }
        }
        record.deleteOnSave();
    }

    /**
     * Read a "frame" of data from the {@code record} containing only the
     * information that is visible to this {@link Audience}.
     * <p>
     * Unlike {@link #read(Collection, Record)}, this method does not throw a
     * {@link RestrictedAccessException}. Instead, it filters out any data that
     * this {@link Audience} is not permitted to see.
     * </p>
     * <p>
     * If this {@link Audience} is not permitted to discover the {@code record}
     * at all, this method returns {@code null}. Otherwise, it returns a map
     * that contains data for the subset of {@code keys} that are readable. An
     * empty map return value indicates that while the {@code record} is
     * visible, none of the requested keys are.
     * </p>
     * <h3>Nested Field Resolution</h3>
     * <p>
     * This method supports nested field access using dot notation (e.g.,
     * {@code "user.profile.name"}). When accessing nested fields, the method
     * recursively applies access control rules at each level:
     * </p>
     * <ul>
     * <li>Root fields are evaluated against this {@link Record}'s access
     * rules</li>
     * <li>Nested fields are evaluated against the target {@link Record}'s
     * access rules if it implements {@link AccessControl}</li>
     * <li>Circular references are detected and rendered as
     * {@code "id (recursive link)"}</li>
     * <li>Non-navigable values return {@code null} when nested access is
     * attempted</li>
     * </ul>
     *
     * @param keys the fields to read from
     * @param record the {@link Record} to read from
     * @param <T> the type of the {@link Record}
     * @return a map of visible data or {@code null} if the {@code record} is
     *         not discoverable at all by this {@link Audience}
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public default <T extends Record> Map<String, Object> frame(
            Collection<String> keys, T subject) {
        Preconditions.checkNotNull(keys, "keys cannot be null");
        Map<String, Object> data;
        if(!$checkIfVisible().test(subject)) {
            return null;
        }
        else if(subject instanceof AccessControl) {
            AccessControl gated = (AccessControl) subject;
            // Break up navigation keys into root components that must be
            // resolved in this Record to their subsequent stops that must
            // be resolved in linked Records
            Map<String, Set<String>> roots = new HashMap<>();
            for (String key : keys) {
                String[] toks = key.split("\\.");
                // Handle negative rules: negative keys apply only to the
                // root level and are not distributed to nested paths
                String root = toks[0];
                String next = Stream.of(toks).skip(1)
                        .collect(Collectors.joining("."));
                Set<String> nexts = roots.computeIfAbsent(root,
                        $ -> new HashSet<>());
                if(!next.isEmpty()) {
                    nexts.add(next);
                }
            }
            /*
             * Determine the keys to map based on what is requested and what is
             * readable for the #audience.
             *
             * @formatter:off
             * |-----------|----------|-----------------------------------------------------------|
             * | Requested | Readable | Result                                                    |
             * |-----------|----------|-----------------------------------------------------------|
             * | All       | All      | data = Record.map()                                       |
             * | All       | None     | data = an empty map                                       |
             * | All       | Some     | data = Record.map(keys) with the keys that are readable   |
             * | Some      | All      | data = Record.map(keys) with the keys that are requested  |
             * | Some      | None     | data = an empty Map                                       |
             * | Some      | Some     | data = Record.map(keys) with the intersection of the keys |
             * |                      | that are readable and requested                           |
             * |-----------|----------|-----------------------------------------------------------|
             * @formatter:on
             */
            Set<String> requested = roots.keySet();
            Set<String> readable = this instanceof Anonymous
                    ? gated.$readableByAnonymous()
                    : gated.$readableBy(this);
            if(readable == NO_KEYS) {
                RESTRICTED_ACCESS_DETECTED.set(true);
                data = ImmutableMap.of();
            }
            else if(requested.equals(ALL_KEYS) && readable.equals(ALL_KEYS)) {
                data = subject.map();
            }
            else {
                if(requested.equals(ALL_KEYS) && !readable.equals(ALL_KEYS)) {
                    String[] visible = readable.toArray(Array.containing());
                    data = subject.map(visible);
                }
                else if(!requested.equals(ALL_KEYS)
                        && readable.equals(ALL_KEYS)) {
                    String[] visible = requested.toArray(Array.containing());
                    data = subject.map(visible);
                }
                else {
                    Set<String> allowed = new HashSet<>();
                    Set<String> denied = new HashSet<>();
                    for (String key : readable) {
                        if(key.startsWith("-")) {
                            denied.add(key.substring(1));
                        }
                        else {
                            allowed.add(key);
                        }
                    }
                    String[] visible = requested.stream().filter(
                            key -> allowed.isEmpty() || allowed.contains(key))
                            .filter(key -> !denied.contains(key))
                            .toArray(String[]::new);
                    if(visible.length < requested.size()) {
                        RESTRICTED_ACCESS_DETECTED.set(true);
                    }
                    if(visible.length == 0) {
                        // No keys are visible, but don't call Record#map
                        // with an empty array because doing so will return
                        // all data
                        data = ImmutableMap.of();
                    }
                    else {
                        data = subject.map(visible);
                    }
                }
            }
            // Go through each value in the data and replace it with a
            // subsequent call to #frame (via the Audience, if possible)
            // using the next stops from the root. We use a ThreadLocal to
            // keep track of records we've already seen so we don't have to
            // have an overloaded method that takes #seen as a recursive
            // parameter. In Java 9+ we could probably switch to that by
            // using a private interface method.
            Multiset<Record> seen = PREVIOUSLY_FRAMED_RECORDS.get();
            seen.add(subject);
            data = data.entrySet().stream().map(e -> {
                String key = e.getKey();
                Object value = e.getValue();
                Set<String> nexts = roots.get(key);
                if(nexts != null && nexts.isEmpty()) {
                    // This is a terminal value
                    return e;
                }
                else {
                    String[] remaining = nexts != null
                            ? nexts.toArray(Array.containing())
                            : Array.containing();
                    if(seen.contains(value)) {
                        value = ((Record) value).id() + " (recursive link)";
                    }
                    else if(value instanceof AccessControl) {
                        Record record = (Record) value;
                        seen.add(record);
                        value = frame(ImmutableSet.copyOf(remaining),
                                (T) record);
                        seen.remove(record);
                    }
                    else if(value instanceof Record) {
                        Record record = (Record) value;
                        seen.add(record);
                        value = record.map(remaining);
                        seen.remove(record);
                    }
                    else if(Sequences.isSequence(value)) {
                        value = Sequences.stream(value).map(item -> {
                            if(seen.contains(item)) {
                                item = ((Record) item).id()
                                        + " (recursive link)";
                            }
                            else {
                                if(item instanceof AccessControl) {
                                    Record record = (Record) item;
                                    seen.add(record);
                                    item = frame(ImmutableSet.copyOf(remaining),
                                            (T) record);
                                    seen.remove(record);
                                }
                                else if(item instanceof Record) {
                                    Record record = (Record) item;
                                    seen.add(record);
                                    item = record.map(remaining);
                                    seen.remove(record);
                                }
                            }
                            return item;
                        }).collect(Collectors.toList());
                    }
                    else if(nexts != null) {
                        // This is an attempt to navigate a non-navigable
                        // value
                        value = null;
                    }
                    return new SimpleEntry<>(key, value);
                }
            }).collect(Association::of, (map, entry) -> {
                String k = entry.getKey();
                Object v = entry.getValue();
                if(v != null) {
                    map.merge(k, v, MergeStrategies::upsert);
                }
                else {
                    map.put(k, v);
                }
            }, MergeStrategies::upsert);
            seen.remove(subject);
            if(seen.isEmpty()) {
                PREVIOUSLY_FRAMED_RECORDS.remove();
            }
        }
        else {
            data = subject.map(keys.toArray(Array.containing()));
        }
        // By convention, the subject's id should always be included when
        // framing.
        if(!data.containsKey("id")) {
            data.put("id", subject.get("id"));
        }
        return data;
    }

    /**
     * Read a "frame" of data from the {@code record} containing only the
     * information that is visible to this {@link Audience}.
     * <p>
     * This is a convenience method that is equivalent to calling
     * {@link #frame(Collection, Record)} with all of the keys in the
     * {@code record}.
     * </p>
     *
     * @param record the {@link Record} to read from
     * @param <T> the type of the {@link Record}
     * @return a map of visible data or {@code null} if the {@code record} is
     *         not discoverable at all by this {@link Audience}
     * @see #frame(Collection, Record)
     */
    public default <T extends Record> Map<String, Object> frame(T record) {
        return frame(ALL_KEYS, record);
    }

    /**
     * Read the values from the specified {@code keys} in the {@code record} on
     * behalf of this {@link Audience}.
     * <p>
     * This method verifies that this {@link Audience} is permitted to read all
     * the specified {@code keys} before returning their values.
     * </p>
     *
     * @param keys the fields to read from
     * @param record the {@link Record} to read from
     * @param <T> the type of the {@link Record}
     * @return a map from each key to its value
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to read one or more of the {@code keys}
     */
    public default <T extends Record> Map<String, Object> read(
            Collection<String> keys, T record)
            throws RestrictedAccessException {
        try {
            Map<String, Object> data = frame(keys, record);
            if(RESTRICTED_ACCESS_DETECTED.get()) {
                throw new RestrictedAccessException();
            }
            else {
                return data;
            }
        }
        finally {
            RESTRICTED_ACCESS_DETECTED.remove();
        }
    }

    /**
     * Read the value from the {@code key} in the {@code record} on behalf of
     * this {@link Audience}.
     * <p>
     * This method verifies that this {@link Audience} is permitted to read the
     * specified {@code key} before returning the value.
     * </p>
     *
     * @param key the field to read from
     * @param record the {@link Record} to read from
     * @param <T> the type of the {@link Record}
     * @return the value of the {@code key}
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to read the {@code key}
     */
    public default <T extends Record> Object read(String key, T record)
            throws RestrictedAccessException {
        Map<String, Object> data = frame(ImmutableSet.of(key), record);
        return data.getOrDefault(key, null);
    }

    /**
     * Write the {@code data} to the {@code record} on behalf of this
     * {@link Audience}.
     * <p>
     * This method verifies that this {@link Audience} is permitted to write to
     * all the keys in the {@code data} map before making the changes.
     * </p>
     *
     * @param data a map from keys to the values to write
     * @param record the {@link Record} to modify
     * @param <T> the type of the {@link Record}
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to write to one or more of the keys in the
     *             {@code data}
     */
    public default <T extends Record> void write(Map<String, Object> data,
            T record) throws RestrictedAccessException {
        if(record instanceof AccessControl) {
            AccessControl subject = (AccessControl) record;
            Set<String> rules = this instanceof Anonymous
                    ? subject.$writableByAnonymous()
                    : subject.$writableBy(this);
            if(!isPermittedAccess(data.keySet(), rules)) {
                throw new RestrictedAccessException();
            }
        }
        if(this instanceof Record) {
            Reflection.set("_author", (Record) this, record);
        }
        record.set(data);
    }

    /**
     * Write the {@code value} to the {@code key} in the {@code record} on
     * behalf of this {@link Audience}.
     * <p>
     * This method verifies that this {@link Audience} is permitted to write to
     * the specified {@code key} before making the change.
     * </p>
     *
     * @param key the field to write to
     * @param value the data to write
     * @param record the {@link Record} to modify
     * @param <T> the type of the {@link Record}
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to write to the {@code key}
     */
    public default <T extends Record> void write(String key, Object value,
            T record) throws RestrictedAccessException {
        if(record instanceof AccessControl) {
            AccessControl subject = (AccessControl) record;
            Set<String> rules = this instanceof Anonymous
                    ? subject.$writableByAnonymous()
                    : subject.$writableBy(this);
            if(!isPermittedAccess(ImmutableSet.of(key), rules)) {
                throw new RestrictedAccessException();
            }
        }
        if(this instanceof Record) {
            Reflection.set("_author", (Record) this, record);
        }
        record.set(key, value);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Page page, Predicate<T> filter, Realms realms) {
        return $db().find(clazz, criteria, order, page,
                filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        return $db().find(clazz, criteria, order, page, $checkIfVisible(),
                realms);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Predicate<T> filter, Realms realms) {
        return $db().find(clazz, criteria, order, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return $db().find(clazz, criteria, order, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Page page, Predicate<T> filter, Realms realms) {
        return $db().find(clazz, criteria, page, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return $db().find(clazz, criteria, page, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Predicate<T> filter, Realms realms) {
        return $db().find(clazz, criteria, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return $db().find(clazz, criteria, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Page page, Predicate<T> filter, Realms realms) {
        return $db().findAny(clazz, criteria, order, page,
                filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Page page, Realms realms) {
        return $db().findAny(clazz, criteria, order, page, $checkIfVisible(),
                realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Predicate<T> filter, Realms realms) {
        return $db().findAny(clazz, criteria, order,
                filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Order order, Realms realms) {
        return $db().findAny(clazz, criteria, order, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Page page, Predicate<T> filter, Realms realms) {
        return $db().findAny(clazz, criteria, page,
                filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Page page, Realms realms) {
        return $db().findAny(clazz, criteria, page, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Predicate<T> filter, Realms realms) {
        return $db().findAny(clazz, criteria, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> findAny(Class<T> clazz, Criteria criteria,
            Realms realms) {
        return $db().findAny(clazz, criteria, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> T findAnyUnique(Class<T> clazz,
            Criteria criteria, Realms realms) {
        T record = $db().findAnyUnique(clazz, criteria, realms);
        return $checkIfVisible().test(record) ? record : null;
    }

    @Override
    default <T extends Record> T findUnique(Class<T> clazz, Criteria criteria,
            Realms realms) {
        T record = $db().findUnique(clazz, criteria, realms);
        return $checkIfVisible().test(record) ? record : null;
    }

    @Override
    default <T extends Record> T load(Class<T> clazz, long id, Realms realms) {
        T record = $db().load(clazz, id, realms);
        return $checkIfVisible().test(record) ? record : null;
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Predicate<T> filter, Realms realms) {
        return $db().load(clazz, order, page, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Page page, Realms realms) {
        return $db().load(clazz, order, page, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Predicate<T> filter, Realms realms) {
        return $db().load(clazz, order, filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Order order,
            Realms realms) {
        return $db().load(clazz, order, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Predicate<T> filter, Realms realms) {
        return $db().load(clazz, page, filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Page page,
            Realms realms) {
        return $db().load(clazz, page, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Predicate<T> filter,
            Realms realms) {
        return $db().load(clazz, filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> load(Class<T> clazz, Realms realms) {
        return $db().load(clazz, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Page page, Predicate<T> filter, Realms realms) {
        return $db().loadAny(clazz, order, page, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Page page, Realms realms) {
        return $db().loadAny(clazz, order, page, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Predicate<T> filter, Realms realms) {
        return $db().loadAny(clazz, order, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
            Realms realms) {
        return $db().loadAny(clazz, order, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Predicate<T> filter, Realms realms) {
        return $db().loadAny(clazz, page, filter.and($checkIfVisible()),
                realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
            Realms realms) {
        return $db().loadAny(clazz, page, $checkIfVisible(), realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz,
            Predicate<T> filter, Realms realms) {
        return $db().loadAny(clazz, filter.and($checkIfVisible()), realms);
    }

    @Override
    default <T extends Record> Set<T> loadAny(Class<T> clazz, Realms realms) {
        return $db().loadAny(clazz, $checkIfVisible(), realms);
    }

}
