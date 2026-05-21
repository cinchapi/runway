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
package com.cinchapi.runway.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Lists;

/**
 * Shared parsing for the {@code +}, {@code -}, and bare key conventions used by
 * {@link com.cinchapi.runway.Record#map Record#map},
 * {@link com.cinchapi.runway.Record#json Record#json}, and
 * {@link com.cinchapi.runway.access.Audience#frame Audience#frame}.
 * <p>
 * A leading {@code +} marks an <em>additive</em> key (one that augments the
 * default payload), a leading {@code -} marks an <em>exclude</em> key (one that
 * is filtered out of the result), and any other key is <em>bare</em> (a
 * positive key that triggers whitelist mode at the call site).
 * </p>
 *
 * @author Jeff Nelson
 */
public final class KeySelection {

    /**
     * Classify {@code key} as a {@link Kind}.
     *
     * @param key the key to classify
     * @return the {@link Kind} indicated by {@code key}'s leading character
     */
    public static Kind kindOf(String key) {
        if(key.startsWith("+")) {
            return Kind.ADDITIVE;
        }
        else if(key.startsWith("-")) {
            return Kind.EXCLUDE;
        }
        else {
            return Kind.BARE;
        }
    }

    /**
     * Return {@code key} with its leading {@code +} or {@code -} prefix
     * removed, if present.
     *
     * @param key the key to strip
     * @return {@code key} with any leading {@code +} or {@code -} removed
     */
    public static String stripPrefix(String key) {
        if(key.isEmpty()) {
            return key;
        }
        char c = key.charAt(0);
        if(c == '+' || c == '-') {
            return key.substring(1);
        }
        else {
            return key;
        }
    }

    /**
     * Partition {@code keys} into bare, additive, and exclude buckets &mdash;
     * with the leading {@code +} or {@code -} prefix stripped on positive and
     * exclude entries respectively.
     * <p>
     * The partitioning is <strong>order-independent</strong>: the same set of
     * input keys always produces the same buckets, regardless of the order they
     * are iterated. When a key root appears with both {@code +} and {@code -},
     * the {@code -} wins &mdash; the additive is dropped so a downstream
     * resolver never fires a {@link com.cinchapi.runway.Computed @Computed}
     * supplier (or navigates a link) for a key the caller has also excluded.
     * The {@code -} wins both for <em>exact</em> matches ({@code +foo} vs.
     * {@code -foo}) and for <em>root</em> matches ({@code +foo.bar} vs.
     * {@code -foo}); the latter prevents a navigation additive from doing work
     * on a root the caller excluded outright.
     * </p>
     * <p>
     * <strong>NOTE:</strong> A bare key that also appears with {@code -}
     * deliberately remains in {@link Partition#bare() bare} so it can still
     * force whitelist mode at the call site. The downstream exclude filter
     * removes it from the final result.
     * </p>
     *
     * @param keys the keys to partition
     * @return the {@link Partition}
     */
    public static Partition partition(Iterable<String> keys) {
        List<String> bare = Lists.newArrayList();
        List<String> additive = Lists.newArrayList();
        List<String> exclude = Lists.newArrayList();
        for (String key : keys) {
            switch (kindOf(key)) {
            case BARE:
                bare.add(key);
                break;
            case ADDITIVE:
                additive.add(key.substring(1));
                break;
            case EXCLUDE:
                exclude.add(key.substring(1));
                break;
            }
        }
        // Exclusion wins over addition for both exact matches and root
        // matches. Catching the root case ("+a.b" vs. "-a") prevents a
        // navigation additive from firing #resolveEntry &mdash; loading
        // a linked record or running a @Computed supplier &mdash; for a
        // root the caller has already excluded outright.
        additive.removeIf(k -> {
            int dot = k.indexOf('.');
            String root = dot < 0 ? k : k.substring(0, dot);
            return exclude.contains(k) || exclude.contains(root);
        });
        return new Partition(bare, additive, exclude);
    }

    /**
     * Partition {@code keys} into bare, additive, and exclude buckets.
     *
     * @param keys the keys to partition
     * @return the {@link Partition}
     * @see #partition(Iterable)
     */
    public static Partition partition(String... keys) {
        return partition(Arrays.asList(keys));
    }

    private KeySelection() {/* no-init */}

    /**
     * The three categories of keys recognized by the selection prefix
     * convention.
     *
     * @author Jeff Nelson
     */
    public enum Kind {

        /**
         * A positive key with no leading prefix. Forces whitelist mode at the
         * call site &mdash; only the named keys appear in the result.
         */
        BARE,

        /**
         * A key prefixed with {@code +}. Layers on top of the defaults without
         * dropping them.
         */
        ADDITIVE,

        /**
         * A key prefixed with {@code -}. Filtered out of the result.
         */
        EXCLUDE
    }

    /**
     * The result of {@link #partition(Iterable) partitioning} a collection of
     * keys by prefix.
     *
     * @author Jeff Nelson
     */
    @Immutable
    public static final class Partition {

        private final List<String> bare;

        private final List<String> additive;

        private final List<String> exclude;

        private Partition(List<String> bare, List<String> additive,
                List<String> exclude) {
            this.bare = bare;
            this.additive = additive;
            this.exclude = exclude;
        }

        /**
         * Return the bare positive keys &mdash; keys with no leading {@code +}
         * or {@code -}.
         *
         * @return the bare keys
         */
        public List<String> bare() {
            return Collections.unmodifiableList(bare);
        }

        /**
         * Return the {@code +}-prefixed positive keys with the prefix stripped.
         * Any key whose exact value <em>or whose root</em> also appears as
         * {@link #exclude() an exclusion} has been removed.
         *
         * @return the additive keys
         */
        public List<String> additive() {
            return Collections.unmodifiableList(additive);
        }

        /**
         * Return the {@code -}-prefixed exclusion keys with the prefix
         * stripped.
         *
         * @return the exclude keys
         */
        public List<String> exclude() {
            return Collections.unmodifiableList(exclude);
        }
    }

}
