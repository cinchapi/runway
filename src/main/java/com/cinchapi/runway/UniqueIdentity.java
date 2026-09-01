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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.collect.Sequences;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.google.common.collect.ImmutableMap;

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
     * Return the values of {@code value} that participate in a constraint: the
     * elements when {@code value} is a {@link Sequences#isSequence(Object)
     * sequence}, and {@code value} itself otherwise. A {@code null} value
     * participates in no constraint.
     *
     * @param value the value whose participating values are returned
     * @return the participating values
     */
    private static Set<Object> participatingValues(Object value) {
        Set<Object> values = new LinkedHashSet<>();
        if(value != null) {
            if(Sequences.isSequence(value)) {
                Sequences.forEach(value, values::add);
            }
            else {
                values.add(value);
            }
        }
        return values;
    }

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
     * The values that participate in the constraint, by the name of the field
     * that holds them. A value that the {@link #criteria} does not cover, a
     * {@code null} or an empty sequence, does not participate.
     */
    private final Map<String, Set<Object>> values;

    /**
     * Construct a new instance.
     *
     * @param any whether the constraint applies across the {@code window}'s
     *            class hierarchy
     * @param window the class that bounds the constraint's identity space
     * @param criteria the {@link Criteria} that a record agreeing with the
     *            constraint matches
     * @param data the constraint's data, by the name of the field that holds it
     */
    UniqueIdentity(boolean any, Class<? extends Record> window,
            Criteria criteria, Map<String, Object> data) {
        this.any = any;
        this.window = window;
        this.criteria = criteria;
        ImmutableMap.Builder<String, Set<Object>> values = ImmutableMap
                .builder();
        data.forEach((key, value) -> {
            Set<Object> participating = participatingValues(value);
            if(!participating.isEmpty()) {
                values.put(key, participating);
            }
        });
        this.values = values.build();
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

    /**
     * Return whether {@code record} agrees with this constraint: for every
     * participating field, {@code record} holds at least one of the values that
     * participate.
     *
     * @param record the {@link Record} to test, which must declare every
     *            participating field
     * @return {@code true} if {@code record} agrees with the constraint
     */
    boolean matches(Record record) {
        boolean matches = true;
        for (Entry<String, Set<Object>> entry : values.entrySet()) {
            Set<Object> stored = participatingValues(
                    Reflection.get(entry.getKey(), record));
            matches = matches
                    && !Collections.disjoint(entry.getValue(), stored);
        }
        return matches;
    }

}
