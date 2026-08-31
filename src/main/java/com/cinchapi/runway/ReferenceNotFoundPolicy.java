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
 * The ways a load resolves a stored reference whose target holds no data.
 * <p>
 * A reference outlives its target. Deleting a {@link Record} removes that
 * {@link Record Record's} own data and leaves every stored reference to it in
 * place, so a reference that resolves to nothing is an ordinary consequence of
 * a deletion that no annotation asked anyone to notice. A
 * {@link ReferenceNotFoundPolicy} decides what the load reports for such a
 * reference and whether the load's holder repairs its own storage.
 * </p>
 * <p>
 * Every policy applies the same way to a field that holds a single
 * {@link Record} and to one that holds a {@link Collection} of them; the two
 * shapes differ only in how the absence of a value is expressed.
 * </p>
 *
 * @author Jeff Nelson
 */
public enum ReferenceNotFoundPolicy {

    /**
     * Fail the load of the {@link Record} that holds the reference.
     * <p>
     * Choose {@link #ERROR} where a reference is an invariant of the holder
     * rather than an observation about the world, so a broken one is a fault to
     * surface instead of an absence to report. The holder cannot be loaded
     * until something repairs the reference, so the choice trades availability
     * for the guarantee that no caller ever sees a holder whose reference
     * silently went missing.
     * </p>
     */
    ERROR,

    /**
     * Resolve the reference to nothing, and stage the removal of the stored
     * reference when the holder next saves.
     * <p>
     * A load reports the same absence that {@link #SKIP} reports.
     * {@link #REPAIR} additionally treats the dead reference as the holder's
     * own state to correct, so the holder stops carrying it once a save
     * commits. A holder that is only ever read keeps the stored reference,
     * because a load reports what it finds and never writes.
     * </p>
     */
    REPAIR,

    /**
     * Resolve the reference to nothing, and leave the stored reference in
     * place.
     * <p>
     * A field that holds a single {@link Record} is left unset, and a
     * {@link Collection} omits the element. The stored reference is the
     * database's record of an association that once existed, and this policy
     * preserves it: a later load reports the same absence, and a caller that
     * reads the underlying data can still see what was referenced.
     * </p>
     */
    SKIP;

}
