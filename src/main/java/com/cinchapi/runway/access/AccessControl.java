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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cinchapi.runway.Record;
import com.google.common.collect.ImmutableSet;

/**
 * A marker interface for {@link Record} types that support access control
 * rules. When a {@link Record} implements {@link AccessControl}, it is possible
 * to
 * enforce permissions and access restrictions that govern how different
 * {@link Audience audiences} can interact with it.
 * <p>
 * The {@link AccessControl} interface defines the contract for specifying
 * granular access rules on a per-{@link Audience} basis. These rules control:
 * </p>
 * <ul>
 * <li><strong>Creation</strong> &mdash; Whether an {@link Audience} can create
 * new instances of the {@link Record}</li>
 * <li><strong>Discovery</strong> &mdash; Whether an {@link Audience} can
 * discover that a particular instance exists</li>
 * <li><strong>Reading</strong> &mdash; Which fields an {@link Audience} can
 * read from the {@link Record}</li>
 * <li><strong>Writing</strong> &mdash; Which fields an {@link Audience} can
 * modify in the {@link Record}</li>
 * <li><strong>Deletion</strong> &mdash; Whether an {@link Audience} can delete
 * the {@link Record}</li>
 * </ul>
 * <p>
 * This framework enables the implementation of various access control models
 * including ACL (Access Control Lists), RBAC (Role-Based Access Control), ABAC
 * (Attribute-Based Access Control), and PBAC (Policy-Based Access Control).
 * </p>
 * <h2>Framework Integration</h2>
 * <p>
 * Records implementing {@link AccessControl} gain additional data access
 * methods that accept an {@link Audience} parameter and automatically apply
 * the defined access rules. This enables the framework to manage data access
 * permissions internally rather than requiring external permission management
 * systems.
 * </p>
 * <h2>Usage Patterns</h2>
 * <p>
 * There are two primary ways to perform access-controlled operations:
 * </p>
 * <ol>
 * <li><strong>Through the {@link Audience}</strong>:
 * {@code audience.read(key, record)} &mdash; The {@link Audience} performs the
 * operation</li>
 * <li><strong>Through the {@link AccessControl access controlled}
 * record</strong>:
 * {@code record.readAs(audience, key)} &mdash; The {@link Record} performs the
 * operation on behalf of the {@link Audience}</li>
 * </ol>
 * <h2>Visibility Rule Scope</h2>
 * <p>
 * Access control rules should be defined on the immediate properties of the
 * {@link Record} class (such as intrinsic, computed, or derived keys) rather
 * than on nested or navigation keys. Each {@link Record} should define its own
 * access rules, and the framework will automatically respect those rules when
 * navigating to linked records.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface AccessControl {

    /**
     * Signifies access to all fields in a {@link Record}.
     */
    public static final Set<String> ALL_KEYS = ImmutableSet.of();

    /**
     * Used to indicate that no fields are accessible.
     */
    public static final Set<String> NO_KEYS = null;

    /**
     * Determine whether the specified {@link Audience} is permitted to create
     * instances of this {@link Record} type.
     *
     * @param audience the {@link Audience} to check
     * @return {@code true} if the {@link Audience} can create instances,
     *         {@code false} otherwise
     */
    public boolean $isCreatableBy(@Nonnull Audience audience);

    /**
     * Determine whether anonymous {@link Audience audiences} are permitted to
     * create instances of this {@link Record} type.
     *
     * @return {@code true} if anonymous creation is allowed, {@code false}
     *         otherwise
     */
    public boolean $isCreatableByAnonymous();

    /**
     * Determine whether the specified {@link Audience} is permitted to delete
     * this {@link Record}.
     *
     * @param audience the {@link Audience} to check
     * @return {@code true} if the {@link Audience} can delete this
     *         {@link Record}, {@code false} otherwise
     */
    public boolean $isDeletableBy(@Nonnull Audience audience);

    /**
     * Determine whether the specified {@link Audience} is permitted to discover
     * the existence of this {@link Record}.
     *
     * @param audience the {@link Audience} to check
     * @return {@code true} if the {@link Audience} can discover this
     *         {@link Record}, {@code false} otherwise
     */
    public boolean $isDiscoverableBy(@Nonnull Audience audience);

    /**
     * Determine whether anonymous {@link Audience audiences} are permitted to
     * discover the existence of this {@link Record}.
     *
     * @return {@code true} if anonymous discovery is allowed, {@code false}
     *         otherwise
     */
    public boolean $isDiscoverableByAnonymous();

    /**
     * Return the set of field keys that the specified {@link Audience} is
     * permitted to read from this {@link Record}.
     *
     * @param audience the {@link Audience} to check
     * @return a set of readable field keys, {@link #ALL_KEYS} for all fields,
     *         or {@link #NO_KEYS} for no access
     */
    @Nullable
    public Set<String> $readableBy(@Nonnull Audience audience);

    /**
     * Return the set of field keys that anonymous {@link Audience audiences}
     * are permitted to read from this {@link Record}.
     *
     * @return a set of readable field keys, {@link #ALL_KEYS} for all fields,
     *         or {@link #NO_KEYS} for no access
     */
    @Nullable
    public Set<String> $readableByAnonymous();

    /**
     * Return the set of field keys that the specified {@link Audience} is
     * permitted to write to in this {@link Record}.
     *
     * @param audience the {@link Audience} to check
     * @return a set of writable field keys, {@link #ALL_KEYS} for all fields,
     *         or {@link #NO_KEYS} for no access
     */
    @Nullable
    public Set<String> $writableBy(@Nonnull Audience audience);

    /**
     * Return the set of field keys that anonymous {@link Audience audiences}
     * are permitted to write to in this {@link Record}.
     *
     * @return a set of writable field keys, {@link #ALL_KEYS} for all fields,
     *         or {@link #NO_KEYS} for no access
     */
    @Nullable
    public Set<String> $writableByAnonymous();

    /**
     * Authorize that the specified {@link Audience} is permitted to create this
     * {@link Record}.
     * <p>
     * This method verifies creation permissions before allowing the
     * {@link Record} to be considered valid. It should be called after
     * instantiation but before saving.
     * </p>
     *
     * @param audience the {@link Audience} attempting to create the
     *            {@link Record}, or {@code null} for anonymous access
     * @throws RestrictedAccessException if the {@link Audience} is not
     *             permitted to create this {@link Record}
     */
    public default void authorize(@Nullable Audience audience)
            throws RestrictedAccessException {
        if(((audience == null || audience instanceof Anonymous)
                && !$isCreatableByAnonymous())
                || (audience != null && !$isCreatableBy(audience))) {
            throw new RestrictedAccessException();
        }
    }

    /**
     * Delete this {@link Record} on behalf of the specified {@link Audience}.
     *
     * @param audience the {@link Audience} performing the delete operation, or
     *            {@code null} for anonymous access
     * @throws RestrictedAccessException if the {@link Audience} is not
     *             permitted to delete this {@link Record}
     */
    public void deleteAs(@Nullable Audience audience);

    /**
     * Read a "frame" of data from this {@link Record} containing only the
     * information that is visible to the specified {@link Audience}.
     * <p>
     * This is a convenience method that delegates to
     * {@link Audience#frame(Record)}.
     * </p>
     *
     * @param audience the {@link Audience} performing the frame operation, or
     *            {@code null} for anonymous access
     * @return a map of visible data or {@code null} if the {@link Record} is
     *         not discoverable at all by the {@link Audience}
     */
    public default Map<String, Object> frameAs(@Nullable Audience audience) {
        audience = audience == null ? Audience.anonymous() : audience;
        return audience.frame($this());
    }

    /**
     * Read a "frame" of data from this {@link Record} containing only the
     * information that is visible to the specified {@link Audience}.
     * <p>
     * This is a convenience method that delegates to
     * {@link Audience#frame(Collection, Record)}.
     * </p>
     *
     * @param audience the {@link Audience} performing the frame operation, or
     *            {@code null} for anonymous access
     * @param keys the fields to read from
     * @return a map of visible data or {@code null} if the {@link Record} is
     *         not discoverable at all by the {@link Audience}
     */
    public default Map<String, Object> frameAs(@Nullable Audience audience,
            Collection<String> keys) {
        audience = audience == null ? Audience.anonymous() : audience;
        return audience.frame(keys, $this());
    }

    /**
     * Read the values from the specified {@code keys} in this {@link Record}
     * on behalf of the specified {@link Audience}.
     * <p>
     * This is a convenience method that delegates to
     * {@link Audience#read(Collection, Record)}.
     * </p>
     *
     * @param audience the {@link Audience} performing the read operation, or
     *            {@code null} for anonymous access
     * @param keys the fields to read from
     * @return a map from each key to its value
     * @throws RestrictedAccessException if the {@link Audience} is not
     *             permitted to read one or more of the {@code keys}
     */
    public default Map<String, Object> readAs(@Nullable Audience audience,
            Collection<String> keys) {
        audience = audience == null ? Audience.anonymous() : audience;
        return audience.read(keys, $this());
    }

    /**
     * Read the value from the {@code key} in this {@link Record} on behalf of
     * the specified {@link Audience}.
     * <p>
     * This is a convenience method that delegates to
     * {@link Audience#read(String, Record)}.
     * </p>
     *
     * @param audience the {@link Audience} performing the read operation, or
     *            {@code null} for anonymous access
     * @param key the field to read from
     * @return the value of the {@code key}
     * @throws RestrictedAccessException if the {@link Audience} is not
     *             permitted to read the {@code key}
     */
    public default Object readAs(@Nullable Audience audience, String key) {
        audience = audience == null ? Audience.anonymous() : audience;
        return audience.read(key, $this());
    }

    /**
     * Write the {@code data} to this {@link Record} on behalf of the specified
     * {@link Audience}.
     * <p>
     * This is a convenience method that delegates to
     * {@link Audience#write(Map, Record)}.
     * </p>
     *
     * @param audience the {@link Audience} performing the write operation, or
     *            {@code null} for anonymous access
     * @param data a map from keys to the values to write
     * @throws RestrictedAccessException if the {@link Audience} is not
     *             permitted to write to one or more of the keys in the
     *             {@code data}
     */
    public default void writeAs(@Nullable Audience audience,
            Map<String, Object> data) {
        audience = audience == null ? Audience.anonymous() : audience;
        audience.write(data, $this());
    }

    /**
     * Write the {@code value} to the {@code key} in this {@link Record} on
     * behalf of the specified {@link Audience}.
     * <p>
     * This is a convenience method that delegates to
     * {@link Audience#write(String, Object, Record)}.
     * </p>
     *
     * @param audience the {@link Audience} performing the write operation, or
     *            {@code null} for anonymous access
     * @param key the field to write to
     * @param value the data to write
     * @throws RestrictedAccessException if the {@link Audience} is not
     *             permitted to write to the {@code key}
     */
    public default void writeAs(@Nullable Audience audience, String key,
            Object value) {
        audience = audience == null ? Audience.anonymous() : audience;
        audience.write(key, value, $this());
    }

    /**
     * Return this instance cast as a {@link Record}.
     * <p>
     * This is a framework-private method and should not be called directly.
     * </p>
     *
     * @param <T> the type of {@link Record}
     * @return this instance as a {@link Record}
     * @throws IllegalStateException if this instance is not a {@link Record}
     */
    @SuppressWarnings("unchecked")
    default <T extends Record> T $this() {
        if(this instanceof Record) {
            return (T) this;
        }
        else {
            throw new IllegalStateException(
                    "Illegal attempt to apply the AccessControl interface to a non-Record type: "
                            + this.getClass());
        }
    }

}