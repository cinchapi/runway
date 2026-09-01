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

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;

/**
 * A {@link DeferredReference} is a {@link Record} reference (e.g. {@link Link})
 * in another {@link Record} that is only {@link Runway#load(Class, long)
 * loaded} from the database if it is being {@link #get() accessed}.
 * <p>
 * A {@link DeferredReference} can be used to improve load performance by
 * deferring the loading of linked {@link Record records} until they are
 * actually used.
 * </p>
 * <p>
 * A {@link DeferredReference} should only be used to wrap a member variable in
 * a {@link Record} class. Using a {@link DeferredReference} in
 * {@link Record#derived(), {@link Record#computed()} or
 * {@link Record#set(String, Object)} functions doesn't make sense and has
 * undefined consequences.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public final class DeferredReference<T extends Record> {

    /*
     * NOTE: This class intentionally does not define #hashCode and #equals
     * since the semantics are undefined.
     */

    /**
     * The reference's id.
     */
    private final long id;

    /**
     * The {@link Binding} to use for loading the reference. The reference
     * resolves within the scope of the owning {@link Record Record's} binding
     * at the moment of the first access.
     */
    private final Binding db;

    /**
     * The housing {@link Record}, through which an access applies the
     * {@link ReferenceNotFoundPolicy} for the housing field.
     * <p>
     * This is {@code null} when no field houses the reference, and it is
     * cleared once the reference resolves.
     * </p>
     */
    @Nullable
    private Record holder;

    /**
     * The name of the housing field. This is {@code null} whenever the
     * {@link #holder} is {@code null}.
     */
    @Nullable
    private String key;

    /**
     * The loaded reference.
     */
    private T reference = null;

    /**
     * Construct a new instance.
     *
     * @param reference the already loaded {@link Record} to wrap
     */
    public DeferredReference(T reference) {
        this.reference = reference;
        this.id = reference.id();
        this.db = reference.binding();
        this.holder = null;
        this.key = null;
    }

    /**
     * Construct a new instance for a reference that no field houses, so an
     * access applies no {@link ReferenceNotFoundPolicy}.
     *
     * @param id the id of the referenced {@link Record}
     * @param db the {@link Binding} through which the reference loads
     */
    DeferredReference(long id, Binding db) {
        this(id, db, null, null);
    }

    /**
     * Construct a new instance.
     *
     * @param id the id of the referenced {@link Record}
     * @param db the {@link Binding} through which the reference loads
     * @param holder the housing {@link Record}
     * @param key the name of the housing field
     */
    DeferredReference(long id, Binding db, @Nullable Record holder,
            @Nullable String key) {
        this.id = id;
        this.db = db;
        this.holder = holder;
        this.key = key;
    }

    /**
     * Return the referenced {@link Record}, loading it if this is the first
     * access.
     * <p>
     * The load happens here, so this is also where the
     * {@link ReferenceNotFoundPolicy} for the housing field applies to a stale
     * reference: {@link ReferenceNotFoundPolicy#SKIP SKIP} and
     * {@link ReferenceNotFoundPolicy#REPAIR REPAIR} both answer {@code null},
     * while {@link ReferenceNotFoundPolicy#ERROR ERROR} throws.
     * </p>
     *
     * @return the referenced {@link Record}, or {@code null} if the reference
     *         is stale and the governing policy allows that
     * @throws ReferenceNotFoundException if the reference is stale and the
     *             governing policy is {@link ReferenceNotFoundPolicy#ERROR
     *             ERROR}
     */
    @Nullable
    public T get() {
        if(reference == null) {
            Record holder = this.holder;
            reference = holder != null
                    ? holder.resolveDeferredReference(key, id)
                    : db.load(id);
            if(reference != null) {
                // NOTE: The holder is only needed to resolve the
                // reference, so releasing it keeps a resolved reference from
                // holding on to the holder and everything the holder reaches.
                this.holder = null;
                this.key = null;
            }
        }
        return reference;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).toString();
    }

    /**
     * Return the id of the reference.
     * 
     * @return the id
     */
    long $id() {
        return id;
    }

    /**
     * Return the current, possible {@code null} reference. Unlike
     * {@link #get()} this method does not load the reference if it is not
     * current loaded.
     * 
     * @return the current reference
     */
    @Nullable
    T $ref() {
        return reference;
    }

}
