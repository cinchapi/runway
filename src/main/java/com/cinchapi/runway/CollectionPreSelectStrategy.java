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

import com.cinchapi.concourse.Link;

/**
 * A {@link CollectionPreSelectStrategy} determines how {@link Runway}
 * pre-selects data for {@link Collection Collection&lt;Record&gt;} fields when
 * loading {@link Record Records}.
 * <p>
 * When a {@link Record} has a field declared as a {@link Collection} of other
 * {@link Record Records}, loading that field requires fetching the data for
 * each linked {@link Record}. This strategy controls whether and how that data
 * is fetched in bulk before individual field conversion begins.
 * </p>
 *
 * @author Jeff Nelson
 */
public enum CollectionPreSelectStrategy {

    /**
     * Pre-fetch every reachable destination {@link Record} for a load operation
     * using Concourse's path-driven {@code navigate()} API, followed by a
     * bulk-{@code select()} cleanup pass that closes any gaps the navigate
     * paths cannot reach (e.g., multi-field cycles whose fields alternate names
     * so the {@code *} transitive modifier cannot traverse them, or links into
     * records that are not represented by a known {@link Record} class).
     * <p>
     * This is the default strategy and the recommended choice for every load.
     * It dispatches a single {@code navigate()} per request for typed loads
     * (and one per discovered class for untyped loads), adding follow-up bulk
     * selects only when the navigate result does not already cover every
     * reachable target.
     * </p>
     */
    NAVIGATE,

    /**
     * Deprecated alias for {@link #NAVIGATE}. Historically dispatched a
     * client-side {@link Link}-discovery BFS without a path-driven
     * {@code navigate()} stage; that mode is no longer distinct, since
     * {@link #NAVIGATE} now performs the same cleanup pass on top of the
     * navigate result.
     *
     * @deprecated Use {@link #NAVIGATE} directly. Selecting {@code BULK_SELECT}
     *             now behaves identically.
     */
    @Deprecated
    BULK_SELECT,

    /**
     * Opt out of bulk pre-selection: every linked {@link Record} is fetched
     * individually by {@link Record} field conversion through a separate
     * {@code concourse.select(id)} call. Provided as an explicit escape hatch
     * for tests and debugging; production loads should use {@link #NAVIGATE}.
     */
    NONE;
}
