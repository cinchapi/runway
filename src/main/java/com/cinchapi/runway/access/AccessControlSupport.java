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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.cinchapi.runway.Record;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Utility class for evaluating and applying access control rules within the
 * access control framework.
 * <p>
 * {@link AccessControlSupport} provides static methods for processing access
 * rule sets that define which fields an {@link Audience} can access in
 * {@link AccessControl access controlled} records. Rules support both allowlist
 * (positive) and denylist (negative) patterns, where negative rules are
 * prefixed with a dash character ({@code -}).
 * </p>
 * <h2>Rule Syntax</h2>
 * <ul>
 * <li><strong>Positive rules</strong>: {@code "field"} allows access to the
 * specified field</li>
 * <li><strong>Negative rules</strong>: {@code "-field"} denies access to the
 * specified field</li>
 * <li><strong>Path matching</strong>: Rules support hierarchical field paths
 * using dot notation (e.g., {@code "user.profile"} matches
 * {@code "user.profile.name"})</li>
 * </ul>
 * <h2>Special Rule Sets</h2>
 * <ul>
 * <li>{@link AccessControl#ALL_KEYS} grants access to all fields</li>
 * <li>{@link AccessControl#NO_KEYS} denies access to all fields</li>
 * </ul>
 *
 * @author Jeff Nelson
 */
class AccessControlSupport {

    /**
     * Return {@code true} if {@code audience} is permitted to create
     * {@code record}.
     *
     * @param audience the {@link Audience} on whose behalf the create runs
     * @param record the {@link Record} to create
     * @return {@code true} if {@code record} is not {@link AccessControl access
     *         controlled} or {@code audience} is permitted to create it
     */
    public static boolean isCreatableByAudience(Audience audience,
            Record record) {
        if(record instanceof AccessControl) {
            AccessControl subject = (AccessControl) record;
            return audience instanceof Anonymous
                    ? subject.$isCreatableByAnonymous()
                    : subject.$isCreatableBy(audience);
        }
        else {
            return true;
        }
    }

    /**
     * Check whether the {@code requested} keys are permitted by the access
     * {@code rules}.
     * <p>
     * This method validates that all requested keys comply with the allowlist
     * and denylist rules. The evaluation follows these principles:
     * </p>
     * <ul>
     * <li>If {@code rules} is {@link AccessControl#NO_KEYS}, only empty
     * requests are permitted</li>
     * <li>If {@code rules} is {@link AccessControl#ALL_KEYS}, all requests are
     * permitted</li>
     * <li>Otherwise, keys must match allowlist rules (if any) and not match any
     * denylist rules</li>
     * </ul>
     *
     * @param requested the collection of field keys being requested
     * @param rules the set of access rules to evaluate against, or {@code null}
     *            for no access
     * @return {@code true} if all requested keys are permitted by the rules,
     *         {@code false} otherwise
     */
    public static boolean isPermittedAccess(Collection<String> requested,
            @Nullable Set<String> rules) {
        if(rules == AccessControl.NO_KEYS) {
            return requested.isEmpty();
        }
        else if(rules == AccessControl.ALL_KEYS) {
            return true;
        }
        else {
            Set<String> allowed = new HashSet<>();
            Set<String> denied = new HashSet<>();
            for (String key : rules) {
                if(key.startsWith("-")) {
                    denied.add(key.substring(1));
                }
                else {
                    allowed.add(key);
                }
            }
            for (String key : requested) {
                if(!allowed.isEmpty()
                        && allowed.stream().noneMatch(key::startsWith)) {
                    return false;
                }
                if(denied.stream().anyMatch(key::startsWith)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Return {@code true} if {@code audience} is permitted to write to each of
     * the {@code keys} on {@code subject}: the {@code subject} exists, it is
     * visible to the {@code audience}, and every one of the {@code keys} is
     * writable by the {@code audience}.
     *
     * @param audience the {@link Audience} on whose behalf the write runs
     * @param keys the fields to write to
     * @param subject the {@link Record} to modify, or {@code null} when there
     *            is nothing to modify
     * @return {@code true} if the write is permitted
     */
    public static boolean isWritableByAudience(Audience audience,
            Collection<String> keys, @Nullable Record subject) {
        if(subject == null
                || !audience.$checkIfInScopeOrVisible().test(subject)) {
            return false;
        }
        else if(subject instanceof AccessControl) {
            AccessControl gated = (AccessControl) subject;
            Set<String> rules = audience instanceof Anonymous
                    ? gated.$writableByAnonymous()
                    : gated.$writableBy(audience);
            return isPermittedAccess(keys, rules);
        }
        else {
            return true;
        }
    }

    /**
     * Verify that {@code audience} is permitted to create {@code record}.
     *
     * @param audience the {@link Audience} on whose behalf the create runs
     * @param record the {@link Record} to create
     * @throws RestrictedAccessException if the create is not permitted
     */
    public static void verifyIsCreatableByAudience(Audience audience,
            Record record) {
        if(!isCreatableByAudience(audience, record)) {
            throw new RestrictedAccessException();
        }
    }

    /**
     * Verify that {@code audience} is permitted to write to each of the
     * {@code keys} on {@code subject}.
     *
     * @param audience the {@link Audience} on whose behalf the write runs
     * @param keys the fields to write to
     * @param subject the {@link Record} to modify, or {@code null} when there
     *            is nothing to modify
     * @throws RestrictedAccessException if the write is not permitted
     */
    public static void verifyIsWritableByAudience(Audience audience,
            Collection<String> keys, @Nullable Record subject) {
        if(!isWritableByAudience(audience, keys, subject)) {
            throw new RestrictedAccessException();
        }
    }

    /**
     * Registry mapping each {@link AccessControl} class to a provider
     * {@link Function} that, given an {@link Audience}, returns the
     * {@link Scope} describing that audience's database-level visibility.
     * <p>
     * Populated via
     * {@link AccessControl#registerVisibilityScope(Class, Function)} and
     * consulted at query time by
     * {@link Audience#select(com.cinchapi.runway.Selection[])}.
     * </p>
     */
    static final Map<Class<?>, Function<Audience, Scope>> VISIBILITY_SCOPES = new ConcurrentHashMap<>();

    /**
     * Return a {@link ThreadLocal} variable to keep track of processed records
     * during the {@link Audience#frame(java.util.Collection, Record)} routine.
     */
    public static final ThreadLocal<Multiset<Record>> PREVIOUSLY_FRAMED_RECORDS = ThreadLocal
            .withInitial(HashMultiset::create);

    /**
     * Return a {@link ThreadLocal} variable to keep track of whether there was
     * an attempt to access restricted data during the
     * {@link Audience#frame(java.util.Collection, Record)} routine.
     */
    public static final ThreadLocal<Boolean> RESTRICTED_ACCESS_DETECTED = ThreadLocal
            .withInitial(() -> false);

}