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
package com.cinchapi.runway.access;

import static com.cinchapi.runway.access.AccessControl.ALL_KEYS;
import static com.cinchapi.runway.access.AccessControl.NO_KEYS;
import static com.cinchapi.runway.access.AccessControlSupport.*;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.collect.Association;
import com.cinchapi.common.collect.MergeStrategies;
import com.cinchapi.common.collect.Sequences;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.runway.Computed;
import com.cinchapi.runway.DatabaseInterface;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Runway;
import com.cinchapi.runway.Selection;
import com.cinchapi.runway.Selections;
import com.cinchapi.runway.SerializationOptions;
import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.TransactionInterface;
import com.cinchapi.runway.Transactional;
import com.cinchapi.runway.Unique;
import com.cinchapi.runway.util.KeySelection;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

/**
 * A {@link Record} that can "perform" database operations on other records
 * (e.g., a user) and is therefore subject to permissions and rules with respect
 * to {@link AccessControl access controlled} records. {@link Audience
 * Audiences} are a key component of the access control paradigm within the
 * framework, where granular access rules for various operations can be defined.
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
 * bypasses access controls, one would use {@code user.load(Movie.class, 1)}
 * where {@code user} is an {@link Audience}. This ensures that the framework
 * only returns the movie if the {@code user} is permitted to see it.
 * </p>
 * <h2>Specialized CRUD Methods</h2> As part of the access control framework,
 * {@link Audience} defines specialized CRUD methods that enforce access
 * controls:
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
 * default {@link Audience} that can be used to interact with the access control
 * system consistently.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Audience extends DatabaseInterface, Transactional {

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
     * to this {@link Audience}, honoring any applicable {@link Scope} for the
     * {@link Record Record's} class.
     * <p>
     * This is a framework-private method and should not be called directly.
     * </p>
     *
     * @return a {@link Predicate} to filter for visible records
     */
    public default <T extends Record> Predicate<T> $checkIfInScopeOrVisible() {
        // TODO: make private in Java 9+
        return record -> {
            if(record instanceof AccessControl) {
                Scope scope = AccessControl
                        .resolveVisibilityScope(record.getClass(), this);
                if(scope != null && scope.isApplicable()) {
                    return scope.test(record);
                }
            }
            return $checkIfVisible().test(record);
        };
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
                // It is assumed that a record that can be read or written is
                // implicitly discoverable. So, this cascading check protects
                // against cases where the lower "discover" visibility isn't
                // explicitly marked for an Audience because the implementing
                // class assumes that specifying read/write visibility is
                // enough.
                else if(this instanceof Anonymous) {
                    return subject.$isDiscoverableByAnonymous()
                            || subject.$readableByAnonymous() != NO_KEYS
                            || subject.$writableByAnonymous() != NO_KEYS;
                }
                else {
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
     * the {@link Record} before it is returned, and the check resolves within
     * the same database context that this {@link Audience} operates against.
     * The returned {@link Record} is not saved to the database until
     * {@link Record#save()} is called, and it is bound to that same context, so
     * a direct {@link Record#save() save} persists within it (e.g., within a
     * {@link com.cinchapi.runway.Transaction Transaction}).
     * </p>
     *
     * @param clazz the type of {@link Record} to create
     * @param args constructor arguments for the {@link Record}
     * @param <T> the type of {@link Record}
     * @return the newly created {@link Record}, not yet saved
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to create the {@link Record}
     * @throws IllegalStateException if a {@link Record} reachable from the
     *             {@code args} is bound to a different open
     *             {@link com.cinchapi.runway.Transaction Transaction}
     */
    public default <T extends Record> T create(Class<T> clazz, Object... args)
            throws RestrictedAccessException {
        T record = Reflection.newInstance(clazz, args);
        if(this instanceof Record) {
            // Bind the new record, and its reachable graph, to the same
            // database interface that this audience operates against before
            // the permission check runs, so the check and a later save both
            // resolve within that context (e.g., within a Transaction).
            Object binding = Reflection.get("binding", this);
            if(binding != null) {
                Reflection.call(record, "bindGraph", binding,
                        Reflection.get("connections", this),
                        Sets.newIdentityHashSet());
            }
        }
        verifyIsCreatableByAudience(this, record);
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
     * Atomically find the first {@link Record} in the hierarchy of
     * {@code clazz} that is visible to this {@link Audience} and matches the
     * {@code criteria} under the supplied {@code order}, and update the value
     * of {@code key} on behalf of this {@link Audience}.
     * <p>
     * The lookup only considers records that are visible to this
     * {@link Audience}, so the first match is the first visible one under
     * {@code order}. The update proceeds only if {@code key} is writable by
     * this {@link Audience} on the match; otherwise the result is {@code null}
     * and nothing is updated. The lookup, the access checks and the update run
     * in this {@link Audience Audience's} transactional scope: within an open
     * {@link TransactionInterface} they stage and commit with it; otherwise,
     * they commit together in their own transaction. The write stages as a save
     * of the match, so save-time validation applies to the whole record, and
     * within an open transaction a failed save poisons it. The field
     * eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}, and the {@code update} operator may run
     * more than once, so it must be free of side effects.
     * </p>
     *
     * @param clazz the {@link Record} type whose hierarchy is searched
     * @param criteria the {@link Criteria} the record must match
     * @param order the {@link Order} that defines "first"
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if there is no
     *         visible match that this {@link Audience} can update
     * @throws IllegalArgumentException if {@code order} is {@code null}, if
     *             {@code key} is not eligible for atomic operations, or if
     *             {@code update} returns {@code null} or a value that is not an
     *             instance of the field's type
     * @throws UnsupportedOperationException if this {@link Audience} has no
     *             transactional scope
     * @throws IllegalStateException if this {@link Audience} has no binding, or
     *             if it is bound to an open transaction that another thread
     *             owns or that a failed save poisoned
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findAnyFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findAnyFirstAndUpdate requires an Order");
        return supplyAndUpdate(this, () -> findAnyFirst(clazz, criteria, order),
                key, update);
    }

    /**
     * Atomically find the one {@link Record} in the hierarchy of {@code clazz}
     * that is visible to this {@link Audience} and matches the
     * {@code criteria}, and update the value of {@code key} on behalf of this
     * {@link Audience}.
     * <p>
     * The lookup only considers records that are visible to this
     * {@link Audience}, so a hidden record neither matches nor makes the result
     * ambiguous. The update proceeds only if {@code key} is writable by this
     * {@link Audience} on the match; otherwise the result is {@code null} and
     * nothing is updated. The lookup, the access checks and the update run in
     * this {@link Audience Audience's} transactional scope: within an open
     * {@link TransactionInterface} they stage and commit with it; otherwise,
     * they commit together in their own transaction. The write stages as a save
     * of the match, so save-time validation applies to the whole record, and
     * within an open transaction a failed save poisons it. The field
     * eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}, and the {@code update} operator may run
     * more than once, so it must be free of side effects.
     * </p>
     *
     * @param clazz the {@link Record} type whose hierarchy is searched
     * @param criteria the {@link Criteria} the record must match
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if there is no
     *         visible match that this {@link Audience} can update
     * @throws DuplicateEntryException if more than one visible record in the
     *             hierarchy matches
     * @throws IllegalArgumentException if {@code key} is not eligible for
     *             atomic operations, or if {@code update} returns {@code null}
     *             or a value that is not an instance of the field's type
     * @throws UnsupportedOperationException if this {@link Audience} has no
     *             transactional scope
     * @throws IllegalStateException if this {@link Audience} has no binding, or
     *             if it is bound to an open transaction that another thread
     *             owns or that a failed save poisoned
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findAnyUniqueAndUpdate(
            Class<T> clazz, Criteria criteria, String key,
            UnaryOperator<V> update) {
        return supplyAndUpdate(this, () -> findAnyUnique(clazz, criteria), key,
                update);
    }

    /**
     * Atomically find the first {@link Record} of type {@code clazz} that is
     * visible to this {@link Audience} and matches the {@code criteria} under
     * the supplied {@code order}, and update the value of {@code key} on behalf
     * of this {@link Audience}.
     * <p>
     * The lookup only considers records that are visible to this
     * {@link Audience}, so the first match is the first visible one under
     * {@code order}. The update proceeds only if {@code key} is writable by
     * this {@link Audience} on the match; otherwise the result is {@code null}
     * and nothing is updated. The lookup, the access checks and the update run
     * in this {@link Audience Audience's} transactional scope: within an open
     * {@link TransactionInterface} they stage and commit with it; otherwise,
     * they commit together in their own transaction. The write stages as a save
     * of the match, so save-time validation applies to the whole record, and
     * within an open transaction a failed save poisons it. The field
     * eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}, and the {@code update} operator may run
     * more than once, so it must be free of side effects.
     * </p>
     *
     * @param clazz the {@link Record} type to find
     * @param criteria the {@link Criteria} the record must match
     * @param order the {@link Order} that defines "first"
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if there is no
     *         visible match that this {@link Audience} can update
     * @throws IllegalArgumentException if {@code order} is {@code null}, if
     *             {@code key} is not eligible for atomic operations, or if
     *             {@code update} returns {@code null} or a value that is not an
     *             instance of the field's type
     * @throws UnsupportedOperationException if this {@link Audience} has no
     *             transactional scope
     * @throws IllegalStateException if this {@link Audience} has no binding, or
     *             if it is bound to an open transaction that another thread
     *             owns or that a failed save poisoned
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findFirstAndUpdate requires an Order");
        return supplyAndUpdate(this, () -> findFirst(clazz, criteria, order),
                key, update);
    }

    /**
     * Atomically find the one {@link Record} of type {@code clazz} that is
     * visible to this {@link Audience} and matches the {@code criteria}, and
     * update the value of {@code key} on behalf of this {@link Audience}.
     * <p>
     * The lookup only considers records that are visible to this
     * {@link Audience}, so a hidden record neither matches nor makes the result
     * ambiguous. The update proceeds only if {@code key} is writable by this
     * {@link Audience} on the match; otherwise the result is {@code null} and
     * nothing is updated. The lookup, the access checks and the update run in
     * this {@link Audience Audience's} transactional scope: within an open
     * {@link TransactionInterface} they stage and commit with it; otherwise,
     * they commit together in their own transaction. The write stages as a save
     * of the match, so save-time validation applies to the whole record, and
     * within an open transaction a failed save poisons it. The field
     * eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}, and the {@code update} operator may run
     * more than once, so it must be free of side effects.
     * </p>
     *
     * @param clazz the {@link Record} type to find
     * @param criteria the {@link Criteria} the record must match
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if there is no
     *         visible match that this {@link Audience} can update
     * @throws DuplicateEntryException if more than one visible record matches
     * @throws IllegalArgumentException if {@code key} is not eligible for
     *             atomic operations, or if {@code update} returns {@code null}
     *             or a value that is not an instance of the field's type
     * @throws UnsupportedOperationException if this {@link Audience} has no
     *             transactional scope
     * @throws IllegalStateException if this {@link Audience} has no binding, or
     *             if it is bound to an open transaction that another thread
     *             owns or that a failed save poisoned
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        return supplyAndUpdate(this, () -> findUnique(clazz, criteria), key,
                update);
    }

    /**
     * Read a "frame" of data from the {@code subject} containing only the
     * information that is visible to this {@link Audience}, using default
     * {@link SerializationOptions}.
     * <p>
     * By default, {@link Computed @Computed} properties are excluded from the
     * framed result unless they are positively named in {@code keys}. To
     * include {@code @Computed} properties without naming them, invoke
     * {@link #frame(SerializationOptions, Collection, Record)} with options
     * built via
     * {@code SerializationOptions.builder().includeComputedValuesByDefault(true)}.
     * </p>
     *
     * @param keys the fields to read from
     * @param subject the {@link Record} to read from
     * @param <T> the type of the {@link Record}
     * @return a map of visible data or {@code null} if the {@code subject} is
     *         not discoverable at all by this {@link Audience}
     * @see #frame(SerializationOptions, Collection, Record)
     */
    public default <T extends Record> Map<String, Object> frame(
            Collection<String> keys, T subject) {
        return frame(SerializationOptions.defaults(), keys, subject);
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
     * <h3>Key Prefix Conventions</h3>
     * <p>
     * Each entry in {@code keys} accepts the same prefix conventions as
     * {@link Record#map(SerializationOptions, String...) Record#map}:
     * </p>
     * <ul>
     * <li><strong>Bare</strong> &mdash; a key with no prefix is a positive
     * request that triggers whitelist mode; the result contains only the named
     * keys (intersected with what this {@link Audience} is permitted to
     * read).</li>
     * <li><strong>Additive ({@code +})</strong> &mdash; a key prefixed with
     * {@code +} is layered on top of the defaults the {@link Audience} is
     * allowed to see, without dropping them. When the call mixes a bare
     * positive with {@code +}-prefixed keys, the bare positive forces whitelist
     * mode and the {@code +} prefix degrades to a redundant whitelist
     * annotation.</li>
     * <li><strong>Exclude ({@code -})</strong> &mdash; a key prefixed with
     * {@code -} is filtered out of the result. Exclusion always wins over
     * addition for the same key.</li>
     * </ul>
     * <p>
     * <strong>NOTE:</strong> A {@code +}-prefixed key cannot bypass access
     * control. If the audience is not permitted to read the underlying field,
     * the key is dropped at the intersection check and
     * {@link RestrictedAccessException} signalling (via
     * {@link #read(Collection, Record) read}) still fires.
     * </p>
     *
     * @param options the {@link SerializationOptions} to apply when
     *            materializing data from the {@code subject} and any linked
     *            {@link Record Records} encountered during recursive framing
     * @param keys the fields to read from; entries may use {@code +} to layer
     *            on top of the audience's defaults or {@code -} to exclude
     * @param subject the {@link Record} to read from
     * @param <T> the type of the {@link Record}
     * @return a map of visible data or {@code null} if the {@code subject} is
     *         not discoverable at all by this {@link Audience}
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public default <T extends Record> Map<String, Object> frame(
            SerializationOptions options, Collection<String> keys, T subject) {
        Preconditions.checkNotNull(keys, "keys cannot be null");
        Map<String, Object> data;
        if(!$checkIfInScopeOrVisible().test(subject)) {
            return null;
        }
        else if(subject instanceof AccessControl) {
            AccessControl gated = (AccessControl) subject;
            // Bucket each requested key by the prefix on its root and
            // track navigation suffixes per bare root for the
            // recursive descent below. KeySelection.partitionByRoot
            // applies the "exclusion wins over addition" precedence
            // so a downstream resolver never fires for a key the
            // caller has also excluded.
            KeySelection.RootedPartition parsed = KeySelection
                    .partitionByRoot(keys);
            Set<String> userBare = parsed.bare();
            Set<String> userAdditive = parsed.additive();
            Set<String> userNegative = parsed.exclude();
            Map<String, Set<String>> roots = parsed.navigation();
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
                data = new HashMap<>();
            }
            else if(requested.equals(ALL_KEYS) && readable.equals(ALL_KEYS)) {
                data = subject.map(options);
            }
            else {
                if(requested.equals(ALL_KEYS) && !readable.equals(ALL_KEYS)) {
                    String[] visible = readable.toArray(Array.containing());
                    data = subject.map(options, visible);
                }
                else if(!requested.equals(ALL_KEYS)
                        && readable.equals(ALL_KEYS)) {
                    // No access restrictions; forward the caller's keys
                    // to subject.map verbatim, stripped only of any
                    // navigation suffix (the recursive descent below
                    // handles those). Walking the caller's input
                    // instead of #requested preserves every prefix
                    // combination the caller wrote on a given root, so
                    // multi-prefix calls like frame({"x", "-x"}) round-
                    // trip through subject.map's own precedence rules
                    // and produce the same result as Record#map.
                    String[] visible = keys.stream().map(k -> {
                        int dot = k.indexOf('.');
                        return dot < 0 ? k : k.substring(0, dot);
                    }).distinct().toArray(String[]::new);
                    data = subject.map(options, visible);
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
                    // A denylist-only readable rule (empty #allowed,
                    // non-empty #denied) permits every subject key
                    // minus the denied entries &mdash; the
                    // empty-allowlist-as-wildcard semantic enforced by
                    // AccessControlSupport#isPermittedAccess. Skip the
                    // allowlist gate in that mode; #denied still
                    // bounds the result.
                    boolean denylistOnly = allowed.isEmpty();
                    int requestedPositives = userBare.size()
                            + userAdditive.size();
                    if(!userBare.isEmpty()) {
                        // Whitelist intent: only the named keys (bare
                        // and `+`-prefixed) appear, gated by the
                        // allowlist (skipped under denylist-only) and
                        // by #denied. Reattach `-` for any selected
                        // key the caller also wrote as a negative so
                        // subject.map's exclude filter applies inside
                        // whitelist mode.
                        Set<String> selected = new HashSet<>(userBare);
                        selected.addAll(userAdditive);
                        selected.removeIf(
                                k -> (!denylistOnly && !allowed.contains(k))
                                        || denied.contains(k));
                        long honored = Stream
                                .concat(userBare.stream(),
                                        userAdditive.stream())
                                .filter(selected::contains).count();
                        if(honored < requestedPositives) {
                            RESTRICTED_ACCESS_DETECTED.set(true);
                        }
                        String[] visible = Stream.concat(selected.stream(),
                                selected.stream().filter(userNegative::contains)
                                        .map(k -> "-" + k))
                                .toArray(String[]::new);
                        if(visible.length == 0) {
                            // No keys are visible, but don't call
                            // Record#map with an empty array because
                            // doing so will return all data
                            data = new HashMap<>();
                        }
                        else {
                            data = subject.map(options, visible);
                        }
                    }
                    else if(denylistOnly) {
                        // No bare positives and no allowlist. The
                        // audience permits every subject key except
                        // #denied, so delegate to subject.map in
                        // defaults/additive mode with the caller's
                        // additives plus the union of user-negatives
                        // and audience-denials reattached as `-`
                        // excludes. An additive on a denied key is
                        // dropped and the call is flagged as
                        // restricted.
                        long honored = Stream
                                .concat(userBare.stream(),
                                        userAdditive.stream())
                                .filter(k -> !denied.contains(k)).count();
                        if(honored < requestedPositives) {
                            RESTRICTED_ACCESS_DETECTED.set(true);
                        }
                        Set<String> excludes = new HashSet<>(userNegative);
                        excludes.addAll(denied);
                        String[] visible = Stream
                                .concat(userAdditive.stream()
                                        .filter(k -> !denied.contains(k))
                                        .map(k -> "+" + k),
                                        excludes.stream().map(k -> "-" + k))
                                .toArray(String[]::new);
                        if(visible.length == 0) {
                            data = subject.map(options);
                        }
                        else {
                            data = subject.map(options, visible);
                        }
                    }
                    else {
                        // Allowlist with optional denials. The
                        // audience's "defaults" are (allowed -
                        // denied); apply the caller's negatives and
                        // layer in their additives that the audience
                        // permits.
                        Set<String> selected = new HashSet<>(allowed);
                        selected.removeAll(denied);
                        selected.removeAll(userNegative);
                        userAdditive.stream().filter(allowed::contains)
                                .filter(k -> !denied.contains(k))
                                .forEach(selected::add);
                        long honored = Stream
                                .concat(userBare.stream(),
                                        userAdditive.stream())
                                .filter(selected::contains).count();
                        if(honored < requestedPositives) {
                            RESTRICTED_ACCESS_DETECTED.set(true);
                        }
                        // Pass the audience-permitted set as bare
                        // positives so subject.map runs whitelist mode
                        // &mdash; reattaching `+` would reopen
                        // additive mode on the subject and bypass the
                        // allowlist. Reattach `-` for any selected
                        // key the caller also wrote as a negative.
                        String[] visible = Stream.concat(selected.stream(),
                                selected.stream().filter(userNegative::contains)
                                        .map(k -> "-" + k))
                                .toArray(String[]::new);
                        if(visible.length == 0) {
                            data = new HashMap<>();
                        }
                        else {
                            data = subject.map(options, visible);
                        }
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
                // A named Record or sequence value must fall through to
                // be framed with the target's defaults, like the
                // default-included path; only scalars are terminal here.
                boolean framable = value instanceof Record
                        || (value != null && Sequences.isSequence(value));
                if(nexts != null && nexts.isEmpty() && !framable) {
                    return e;
                }
                else {
                    String[] remaining = nexts != null
                            ? nexts.toArray(Array.containing())
                            : Array.containing();
                    if(seen.contains(value)) {
                        value = ((Record) value).get("id")
                                + " (recursive link)";
                    }
                    else if(value instanceof AccessControl) {
                        Record record = (Record) value;
                        seen.add(record);
                        value = frame(options, ImmutableSet.copyOf(remaining),
                                (T) record);
                        seen.remove(record);
                    }
                    else if(value instanceof Record) {
                        Record record = (Record) value;
                        seen.add(record);
                        value = record.map(options, remaining);
                        seen.remove(record);
                    }
                    else if(Sequences.isSequence(value)) {
                        value = Sequences.stream(value).map(item -> {
                            if(seen.contains(item)) {
                                item = ((Record) item).get("id")
                                        + " (recursive link)";
                            }
                            else {
                                if(item instanceof AccessControl) {
                                    Record record = (Record) item;
                                    seen.add(record);
                                    item = frame(options,
                                            ImmutableSet.copyOf(remaining),
                                            (T) record);
                                    seen.remove(record);
                                }
                                else if(item instanceof Record) {
                                    Record record = (Record) item;
                                    seen.add(record);
                                    item = record.map(options, remaining);
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
            data = subject.map(options, keys.toArray(Array.containing()));
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
     * {@link #frame(SerializationOptions, Collection, Record)} with all of the
     * keys in the {@code record} and default {@link SerializationOptions}.
     * </p>
     * <p>
     * {@link Computed @Computed} properties are excluded from the framed
     * result. To include them, invoke
     * {@link #frame(SerializationOptions, Collection, Record)} with options
     * built via
     * {@code SerializationOptions.builder().includeComputedValuesByDefault(true)}.
     * </p>
     *
     * @param record the {@link Record} to read from
     * @param <T> the type of the {@link Record}
     * @return a map of visible data or {@code null} if the {@code record} is
     *         not discoverable at all by this {@link Audience}
     * @see #frame(SerializationOptions, Collection, Record)
     */
    public default <T extends Record> Map<String, Object> frame(T record) {
        return frame(SerializationOptions.defaults(), ALL_KEYS, record);
    }

    /**
     * Return the unique {@link Record} that agrees with every {@link Unique}
     * constraint of {@code record}, or save {@code record} on behalf of this
     * {@link Audience} when none exists.
     * <p>
     * This {@link Audience} must be permitted to create {@code record}, even
     * when an existing {@link Record} claims the identity, and an existing
     * match must be visible to this {@link Audience}.
     * </p>
     * <p>
     * <strong>NOTE:</strong> A refusal of a hidden match still confirms that a
     * {@link Record} with the identity exists, even though this
     * {@link Audience} cannot see it.
     * </p>
     *
     * @param record the {@link Record} whose identity is interned
     * @param <T> the type of {@link Record}
     * @return the {@link Record} that claims the identity: the sole existing
     *         match, or {@code record} once saved
     * @throws RestrictedAccessException if this {@link Audience} is not
     *             permitted to create {@code record}, or if the identity is
     *             claimed by a {@link Record} that is not visible to this
     *             {@link Audience}
     * @throws DuplicateEntryException if more than one record shares the
     *             identity, whether or not every one is visible to this
     *             {@link Audience}
     * @throws IllegalArgumentException if no field under a {@link Unique}
     *             constraint of {@code record} has a non-null value
     * @throws UnsupportedOperationException if this {@link Audience} has no
     *             transactional scope
     * @throws IllegalStateException if this {@link Audience} has no binding, or
     *             if it is bound to an open transaction that another thread
     *             owns or that a failed save poisoned
     */
    @Override
    public default <T extends Record> T intern(T record)
            throws RestrictedAccessException {
        if(this instanceof Record) {
            return ((Record) this).supply(view -> {
                // The checks below run against this Audience, so the raw
                // transaction is the correct target for the staging
                // operations; the Audience-scoped view would repeat them.
                TransactionInterface transaction = AudienceTransaction
                        .raw(view);
                // Join the record and its reachable graph to the
                // transactional scope before the permission check
                // runs, so the check and the save both resolve within
                // it, consistent with #create.
                Reflection.call(transaction, "join", record);
                verifyIsCreatableByAudience(this, record);
                Record previous = Reflection.get("_author", record);
                Reflection.set("_author", (Record) this, record);
                T interned = transaction.intern(record);
                if(interned != record) {
                    // The record was never saved, so nothing consumed
                    // the author marker; restore it so a later save
                    // is not attributed to this Audience.
                    Reflection.set("_author", previous, record);
                    if(!$checkIfInScopeOrVisible().test(interned)) {
                        throw new RestrictedAccessException();
                    }
                    else {
                        return interned;
                    }
                }
                else {
                    return interned;
                }
            });
        }
        else {
            throw new UnsupportedOperationException();
        }
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

    @Override
    @SuppressWarnings("unchecked")
    public default Selections select(Selection<?>... selections) {
        selections = Arrays.stream(selections).map(selection -> {
            Class<?> clazz = selection.clazz();
            if(clazz != null && AccessControl.class.isAssignableFrom(clazz)) {
                Scope scope = AccessControl.resolveVisibilityScope(clazz, this);
                if(scope != null && scope.isApplicable()) {
                    return scope.apply(selection);
                }
            }
            return Selection.withInjectedFilter((Selection<Record>) selection,
                    $checkIfVisible());
        }).toArray(Selection[]::new);
        return $db().select(selections);
    }

    /**
     * Start a {@link Transaction} that this {@link Audience} joins, so the
     * operations it performs, and the access checks that gate them, resolve
     * within the transaction.
     * <p>
     * Every operation on the returned view behaves the same as the operation on
     * this {@link Audience}, just within the confines of the transaction: reads
     * observe this {@link Audience Audience's} visibility and the writes it
     * permits are the ones that stage.
     * </p>
     * <p>
     * The caller owns the {@link Transaction Transaction's} lifecycle: end it
     * with exactly one of {@link Transaction#commit() commit} or
     * {@link Transaction#abort() abort}, or rely on {@link Transaction#close()
     * close} to abort whatever was not committed. Use a try-with-resources
     * block so the transaction always ends. After the transaction ends, this
     * {@link Audience} operates against the enclosing {@link Runway} again.
     * </p>
     *
     * @return an open {@link Transaction} that this {@link Audience} joined
     * @throws IllegalStateException if this {@link Audience} has no binding, or
     *             if it is already bound to an open {@link Transaction}
     * @throws UnsupportedOperationException if this {@link Audience} is not a
     *             {@link Record}
     */
    @Override
    public default Transaction stage() {
        if(this instanceof Record) {
            Record record = (Record) this;
            Runway harness = Reflection.call(record, "harness");
            Verify.that(harness != null, "Cannot stage a Transaction because"
                    + " this Audience has no binding");
            boolean inOpenTransaction = Reflection.call(record,
                    "isBoundToOpenTransaction");
            Verify.that(!inOpenTransaction, "Cannot stage a Transaction"
                    + " because this Audience is already bound to an open"
                    + " Transaction");
            Transaction transaction = harness.stage();
            try {
                Reflection.call(transaction, "join", record);
            }
            catch (RuntimeException e) {
                transaction.close();
                throw e;
            }
            return new AudienceTransaction(this, transaction);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Execute {@code work} within this {@link Audience Audience's}
     * transactional scope and return its result.
     * <p>
     * If this {@link Audience} is bound to an open {@link Transaction}, then
     * the work joins it; otherwise, the work runs in its own managed
     * transaction that commits after the work completes, per the
     * {@link Transactional#supply(Function) Transactional} contract.
     * </p>
     *
     * @param work the work to run
     * @return the result of {@code work}
     * @throws UnsupportedOperationException if this {@link Audience} is not a
     *             {@link Record}
     */
    @Override
    public default <T> T supply(Function<TransactionInterface, T> work) {
        if(this instanceof Record) {
            return ((Record) this).supply(work);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Return the {@link TransactionInterface} view of {@code transaction}
     * through which work scoped by this {@link Audience} operates: every
     * operation on the view behaves the same as the operation on this
     * {@link Audience}, just within the confines of the transaction.
     *
     * @param transaction the transaction that scopes the work
     * @return the view the work receives
     */
    @Override
    public default TransactionInterface view(TransactionInterface transaction) {
        if(transaction instanceof Transaction
                && !(transaction instanceof AudienceTransaction)) {
            return new AudienceTransaction(this, (Transaction) transaction);
        }
        else {
            return transaction;
        }
    }

    /**
     * Write the {@code data} to the {@code record} on behalf of this
     * {@link Audience}.
     * <p>
     * This method verifies that the {@code record} is visible to this
     * {@link Audience} and that this {@link Audience} is permitted to write to
     * all the keys in the {@code data} map before making the changes.
     * </p>
     *
     * @param data a map from keys to the values to write
     * @param record the {@link Record} to modify
     * @param <T> the type of the {@link Record}
     * @throws RestrictedAccessException if the {@code record} is not visible to
     *             this {@link Audience}, or if this {@link Audience} is not
     *             permitted to write to one or more of the keys in the
     *             {@code data}
     */
    public default <T extends Record> void write(Map<String, Object> data,
            T record) throws RestrictedAccessException {
        verifyIsWritableByAudience(this, data.keySet(), record);
        record.set(data);
        if(this instanceof Record) {
            Reflection.set("_author", (Record) this, record);
        }
    }

    /**
     * Write the {@code value} to the {@code key} in the {@code record} on
     * behalf of this {@link Audience}.
     * <p>
     * This method verifies that the {@code record} is visible to this
     * {@link Audience} and that this {@link Audience} is permitted to write to
     * the specified {@code key} before making the change.
     * </p>
     *
     * @param key the field to write to
     * @param value the data to write
     * @param record the {@link Record} to modify
     * @param <T> the type of the {@link Record}
     * @throws RestrictedAccessException if the {@code record} is not visible to
     *             this {@link Audience}, or if this {@link Audience} is not
     *             permitted to write to the {@code key}
     */
    public default <T extends Record> void write(String key, Object value,
            T record) throws RestrictedAccessException {
        verifyIsWritableByAudience(this, ImmutableSet.of(key), record);
        record.set(key, value);
        if(this instanceof Record) {
            Reflection.set("_author", (Record) this, record);
        }
    }

}
