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

import com.cinchapi.common.base.Verify;
import com.cinchapi.runway.DatabaseInterface;

/**
 * An {@link Audience} that represents an unauthenticated or unknown user in the
 * access control framework.
 * <p>
 * The {@link Anonymous} class provides a default {@link Audience} for scenarios
 * where no specific audience context is available, such as public API endpoints
 * or unauthenticated requests. It enables the access control framework to
 * handle these cases consistently without requiring special logic for null or
 * missing {@link Audience} instances.
 * </p>
 * <p>
 * Every {@link Anonymous} audience is equal to every other one, regardless of
 * the database it operates against, because each represents the same absence of
 * identity.
 * </p>
 * <p>
 * Access rules for {@link Anonymous} are typically more restrictive than those
 * for known {@link Audience audiences}. {@link AccessControl Access controlled}
 * records can define specific permissions for anonymous access through methods
 * like {@link AccessControl#$isCreatableByAnonymous()},
 * {@link AccessControl#$readableByAnonymous()}, and
 * {@link AccessControl#$writableByAnonymous()}.
 * </p>
 * <h2>Usage</h2>
 * <p>
 * Obtain an {@link Anonymous} audience through {@link Audience#anonymous()} or
 * {@link Audience#anonymous(DatabaseInterface)}.
 * </p>
 *
 * @author Jeff Nelson
 */
final class Anonymous implements Audience {

    /**
     * Return an {@link Anonymous} audience that operates against {@code db}.
     *
     * @param db the {@link DatabaseInterface} the audience operates against
     * @return the {@link Anonymous} audience
     */
    static Anonymous get(DatabaseInterface db) {
        Verify.thatArgument(db != null,
                "An anonymous Audience requires a database");
        return new Anonymous(db);
    }

    /**
     * The database this {@link Anonymous} audience operates against.
     */
    private final DatabaseInterface db;

    /**
     * Construct a new instance.
     *
     * @param db the {@link DatabaseInterface} this audience operates against
     */
    private Anonymous(DatabaseInterface db) {
        this.db = db;
    }

    @Override
    public DatabaseInterface $db() {
        return db;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Anonymous;
    }

    @Override
    public int hashCode() {
        return Anonymous.class.hashCode();
    }

}
