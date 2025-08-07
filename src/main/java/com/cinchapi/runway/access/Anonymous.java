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

/**
 * A singleton {@link Audience} that represents an unauthenticated or unknown
 * user in the access control framework.
 * <p>
 * The {@link Anonymous} class provides a default {@link Audience} for
 * scenarios where no specific audience context is available, such as public API
 * endpoints or unauthenticated requests. It enables the access control
 * framework to handle these cases consistently without requiring special logic
 * for null or missing {@link Audience} instances.
 * </p>
 * <p>
 * Access rules for {@link Anonymous} are typically more restrictive than those
 * for known {@link Audience audiences}. {@link AccessControl Access
 * controlled} records can define specific permissions for anonymous access
 * through methods like {@link AccessControl#$isCreatableByAnonymous()},
 * {@link AccessControl#$readableByAnonymous()}, and
 * {@link AccessControl#$writableByAnonymous()}.
 * </p>
 * <h2>Usage</h2>
 * <p>
 * The {@link Anonymous} instance should be obtained through
 * {@link Audience#anonymous()} rather than directly calling {@link #get()}.
 * This ensures consistency with the {@link Audience} interface contract.
 * </p>
 *
 * @author Jeff Nelson
 */
final class Anonymous implements Audience {

    /**
     * The singleton instance of {@link Anonymous}.
     */
    private static final Anonymous INSTANCE = new Anonymous();

    /**
     * Return the singleton {@link Anonymous} instance.
     * <p>
     * Prefer using {@link Audience#anonymous()} over this method for
     * consistency with the {@link Audience} interface.
     * </p>
     *
     * @return the {@link Anonymous} instance
     */
    public static Anonymous get() {
        return INSTANCE;
    }

    /**
     * Construct a new instance.
     */
    private Anonymous() {/* no-init */}

}