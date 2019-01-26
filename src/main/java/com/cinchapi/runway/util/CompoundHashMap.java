/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.runway.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A {@link CompoundHashMap} is a {@link HashMap} that is configured to fetch
 * data from other {@link Map}s if a requested key was not explicitly added.
 * <p>
 * NOTE: This map only reads through to the other sources and never modifies
 * them.
 * </p>
 *
 * @author Jeff Nelson
 */
public class CompoundHashMap<K, V> extends HashMap<K, V> {

    private static final long serialVersionUID = 7140831886907668552L;

    /**
     * Create a new {@link CompoundHashMap} that is configured to use
     * additional lookup {@code sources} for data that is not in the map.
     * 
     * @param sources the additional sources for this {@link Map} in order of
     *            decreasing priority (e.g. the value for a non-native key is
     *            sought starting with the first source provided)
     * @return the {@link CompoundHashMap} configured to use the additional
     *         {@code sources}.
     */
    @SafeVarargs
    public static <K, V> CompoundHashMap<K, V> create(Map<K, V>... sources) {
        return new CompoundHashMap<>(sources);
    }

    /**
     * The additional sources in increasing order of priority.
     */
    private final List<Map<K, V>> additionalSources;

    /**
     * Construct a new instance.
     * 
     * @param sources
     */
    @SafeVarargs
    private CompoundHashMap(Map<K, V>... sources) {
        this.additionalSources = Lists.newArrayList(sources);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> entries = Sets.newLinkedHashSet();
        additionalSources.forEach(source -> {
            entries.addAll(source.entrySet());
        });
        entries.addAll(super.entrySet());
        return entries;
    }

    @Override
    public V get(Object key) {
        V value = super.get(key);
        if(value == null) {
            for (Map<K, V> source : additionalSources) {
                value = source.get(key);
                if(value != null) {
                    return value;
                }
                else {
                    continue;
                }
            }
        }
        return value;
    }

}
