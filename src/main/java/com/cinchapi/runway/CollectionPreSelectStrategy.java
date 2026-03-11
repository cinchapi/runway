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
     * Use Concourse's {@code navigate()} API to pre-fetch all destination
     * {@link Record} data for {@link Collection Collection&lt;Record&gt;}
     * fields in a single call.
     * <p>
     * {@code navigate()} returns data keyed by destination record ID,
     * preserving the per-destination association that {@code select()} with
     * navigation keys loses when following multi-valued {@link Link Links}.
     * This strategy issues one {@code navigate()} call per query and requires
     * {@link Record.StaticAnalysis} to compute navigation paths at startup.
     * </p>
     * <p>
     * <strong>Trade-offs:</strong>
     * <ul>
     * <li>Single RPC with snapshot atomicity &mdash; all destination data
     * reflects a consistent point in time.</li>
     * <li>Requires class-aware path computation via
     * {@link Record.StaticAnalysis}, so it cannot be used for untyped loads
     * where the class is unknown.</li>
     * <li>Actual performance depends on how efficiently the Concourse server
     * implements the {@code navigate()} codepath &mdash; some earlier versions
     * of Concourse did not fully optimize it.</li>
     * </ul>
     */
    NAVIGATE,

    /**
     * Scan the initially loaded data for {@link Link} values, batch-fetch all
     * discovered targets via {@code concourse.select(Set&lt;Long&gt;)}, and
     * repeat until all reachable {@link Record Records} are collected. One
     * batch {@code select()} call is issued per depth level in the object graph
     * (typically 2&ndash;3 iterations).
     * <p>
     * <strong>Trade-offs:</strong>
     * <ul>
     * <li>Schema-agnostic &mdash; does not require
     * {@link Record.StaticAnalysis} or class-specific path computation, so it
     * works for untyped loads and arbitrary object graphs.</li>
     * <li>Issues one batch {@code select()} per depth level rather than a
     * single call, so deeper graphs require more round trips (one per
     * level).</li>
     * <li>Fetches all keys for each destination record, not just the keys
     * matching declared fields &mdash; slightly more data transferred than
     * {@link #NAVIGATE}.</li>
     * <li>No snapshot atomicity across levels &mdash; each batch sees the
     * database state at the time of its call.</li>
     * </ul>
     */
    BULK_SELECT,

    /**
     * Do not pre-select data for {@link Collection Collection&lt;Record&gt;}
     * fields. Each linked {@link Record} is fetched individually as it is
     * encountered during field conversion &mdash; one
     * {@code concourse.select(id)} call per element.
     * <p>
     * <strong>Trade-offs:</strong>
     * <ul>
     * <li>Simplest implementation with no upfront work or extra memory.</li>
     * <li>O(N) round trips for a collection of N elements &mdash; acceptable
     * for small collections or same-region deployments with sub-millisecond
     * latency.</li>
     * <li>Catastrophic for large collections or cross-region deployments where
     * per-call latency is significant.</li>
     * </ul>
     */
    NONE;
}
