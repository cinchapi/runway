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

import java.util.Collection;

/**
 * A {@link ReferenceNotFoundPolicy} decides what a load does with a stale
 * reference, which is one that points at a record with no stored data.
 * <p>
 * A reference goes stale when its target is deleted, or when something outside
 * of Runway destructively modifies the target. Deleting a {@link Record} clears
 * that {@link Record Record's} own data, but it leaves every stored reference
 * to it in place unless a {@link CaptureDelete} field asks for that cleanup.
 * </p>
 * <p>
 * A policy governs a field that holds a single {@link Record} and a field that
 * holds a {@link Collection} of them the same way. Only the expression of the
 * missing value differs: a scalar field is left {@code null} and a collection
 * omits the element.
 * </p>
 *
 * @author Jeff Nelson
 */
public enum ReferenceNotFoundPolicy {

    /**
     * Fail the load of the housing record and throw a
     * {@link ReferenceNotFoundException}.
     * <p>
     * Use {@link #ERROR} for a reference the record cannot be correct without.
     * The housing record stays unloadable until something repairs the stale
     * reference. In exchange, no caller ever receives a record whose reference
     * went missing.
     * </p>
     */
    ERROR,

    /**
     * Skip the stale reference, and also delete it from the database.
     * <p>
     * The load reports the same absence that {@link #SKIP} reports, and the
     * housing record stops carrying the stale reference, so no later load
     * encounters it. Within a {@link Transaction}, the delete commits or aborts
     * with that transaction.
     * </p>
     */
    REPAIR,

    /**
     * Skip the stale reference, and leave it in the database.
     * <p>
     * A housing scalar field receives a {@code null} value and a housing
     * collection does not consider the reference at all. The database keeps the
     * reference, so a later load reports the same absence and a caller that
     * reads the underlying data can still see what was referenced.
     * </p>
     */
    SKIP;

}
