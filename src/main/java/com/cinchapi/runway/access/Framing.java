/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
package com.cinchapi.runway.access;

import java.util.HashMap;
import java.util.Map;

import com.cinchapi.runway.Record;
import com.cinchapi.runway.cache.Signature;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 *
 *
 * @author jeff
 */
final class Framing {

    /**
     * A thread-local cache that stores previously mapped record data to avoid
     * redundant processing.
     * <p>
     * The cache uses a {@link Signature} as a key to uniquely identify
     * record-keys combinations that have been processed. When the same record
     * and set of keys are requested again within the same request processing
     * cycle, the cached result is returned.
     * </p>
     * <p>
     * This cache is thread-local to ensure request isolation and prevent data
     * leakage between concurrent requests.
     * </p>
     */
    public static final ThreadLocal<Map<Signature, Map<String, Object>>> CACHE = ThreadLocal
            .withInitial(HashMap::new);

    /**
     * Return a {@link ThreadLocal} variable to keep track of processed records
     * during the {@link Audience#frame(java.util.Collection, Record)} routine.
     */
    public static final ThreadLocal<Multiset<Record>> PROCESSED = ThreadLocal
            .withInitial(HashMultiset::create);

}
