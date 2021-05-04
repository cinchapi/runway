/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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

import java.util.Collection;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.collect.Collections;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * A matcher for one or more realms in which {@link Record Records} may exist.
 * <p>
 * Realms simulate housing {@link Record Records} in logically distinct groups,
 * even though the data is physically located in the same environment. Realms
 * are flexible such that a {@link Record} may exist in multiple realms at the
 * same time.
 * </p>
 * <p>
 * Realms make it easy to logically segregate data for different purposes
 * without incurring the overhead of connecting to multiple environments. The
 * nature of Realms also makes it easy to have data that is considered common
 * across realms without creating multiple copies.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public final class Realms {

    /**
     * Match any {@link Realms}.
     */
    private static final Realms ANY = anyOf(ImmutableSet.of());

    /**
     * Match any and all realms.
     * 
     * @return a matcher
     */
    public static Realms any() {
        return ANY;
    }

    /**
     * Match any of the specified {@code realms} such that a {@link Record}
     * would be valid if any of its realms overlaps with any of the provided
     * {@code realms}.
     * 
     * @param realms
     * @return a matcher
     */
    public static Realms anyOf(Collection<String> realms) {
        return new Realms(realms);
    }

    /**
     * Match any of the specified {@code realms} such that a {@link Record}
     * would be valid if any of its realms overlaps with any of the provided
     * {@code realms}.
     * 
     * @param realms
     * @return a matcher
     */
    public static Realms anyOf(String... realms) {
        return anyOf(Sets.newHashSet(realms));
    }

    /**
     * Match only one {@code realm}.
     * 
     * @param realm
     * @return a matcher
     */
    public static Realms only(String realm) {
        return anyOf(ImmutableSet.of(realm));
    }

    /**
     * The matched realms.
     */
    private final Set<String> realms;

    /**
     * Construct a new instance.
     * 
     * @param realms
     */
    private Realms(Collection<String> realms) {
        this.realms = Collections.ensureSet(realms);
    }

    @Override
    public boolean equals(Object object) {
        if(object instanceof Realms) {
            return realms.equals(((Realms) object).realms);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return realms.hashCode();
    }

    @Override
    public String toString() {
        return realms.toString();
    }

    /**
     * Return the names of the matched realms.
     * 
     * @return the matched realms
     */
    /* package */ Set<String> names() {
        return java.util.Collections.unmodifiableSet(realms);
    }

}
