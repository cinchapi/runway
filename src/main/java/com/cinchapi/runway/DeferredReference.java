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

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;

/**
 * A {@link DeferredReference} is a {@link Record} reference (e.g. {@link Link})
 * in another {@link Record} that is only {@link Runway#load(Class, long)} from
 * the database if it is being {@link #get() accessed}.
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
     * The {@link DatabaseInterface} to use for loading the reference.
     */
    private final DatabaseInterface db;

    /**
     * The loaded reference.
     */
    private T reference = null;

    /**
     * Construct a new instance.
     * 
     * @param reference
     */
    public DeferredReference(T reference) {
        this.reference = reference;
        this.id = reference.id();
        this.db = reference.db;
    }

    /**
     * Construct a new instance.
     * 
     * @param clazz
     * @param id
     * @param db
     */
    DeferredReference(long id, Runway db) {
        this.id = id;
        this.db = db;
    }

    /**
     * Return the reference.
     * 
     * @return the {@link Record reference}
     */
    public T get() {
        if(reference == null) {
            reference = ((Runway) db).load(id); // (authorized)
        }
        return reference;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).toString();
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
