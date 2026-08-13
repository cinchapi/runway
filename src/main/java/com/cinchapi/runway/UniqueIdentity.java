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

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.lang.Criteria;

/**
 * One {@link Unique} constraint of a {@link Record}, resolved against the
 * {@link Record Record's} current data: the {@link Criteria} that a record
 * agreeing with the constraint matches, and the scope in which that identity
 * applies.
 *
 * @author Jeff Nelson
 */
@Immutable
final class UniqueIdentity {

    /**
     * Whether the constraint applies across the {@link #window() window's}
     * class hierarchy instead of a single concrete class.
     */
    private final boolean any;

    /**
     * The class that bounds the constraint's identity space.
     */
    private final Class<? extends Record> window;

    /**
     * The {@link Criteria} that a record agreeing with the constraint matches.
     */
    private final Criteria criteria;

    /**
     * Construct a new instance.
     *
     * @param any whether the constraint applies across the {@code window}'s
     *            class hierarchy
     * @param window the class that bounds the constraint's identity space
     * @param criteria the {@link Criteria} that a record agreeing with the
     *            constraint matches
     */
    UniqueIdentity(boolean any, Class<? extends Record> window,
            Criteria criteria) {
        this.any = any;
        this.window = window;
        this.criteria = criteria;
    }

    /**
     * Return whether the constraint applies across the {@link #window()
     * window's} class hierarchy: the window and every descendant share one
     * identity space. When {@code false}, the constraint applies among records
     * of the same concrete class.
     *
     * @return {@code true} if the constraint applies across the hierarchy
     */
    boolean any() {
        return any;
    }

    /**
     * Return the class that bounds the constraint's identity space. For a
     * hierarchy-scoped constraint, this is the class that declares the
     * constraint; for a class-scoped constraint, it is the {@link Record
     * Record's} concrete class.
     *
     * @return the class that bounds the identity space
     */
    Class<? extends Record> window() {
        return window;
    }

    /**
     * Return the {@link Criteria} that a record agreeing with the constraint
     * matches. The {@link Criteria} covers the constraint's non-null data and
     * does not constrain the record's class.
     *
     * @return the constraint's identity {@link Criteria}
     */
    Criteria criteria() {
        return criteria;
    }

}
