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
package com.cinchapi.runway.cache;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import com.cinchapi.common.base.Array;
import com.cinchapi.runway.Record;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * A {@link Signature} is a unique identifier for a {@link Record} with specific
 * keys.
 * It encapsulates a specific lookup of one or more keys in a record, providing
 * a consistent way to identify the same record-keys combination across multiple
 * operations.
 * <p>
 * This is particularly useful for caching scenarios where the system needs to
 * determine if the same record and set of keys have been processed before.
 * </p>
 * <p>
 * Instances of {@link Signature} are immutable and can be safely used as keys
 * in
 * hash-based collections.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class Signature {

    /**
     * Creates a unique hash representation for the given record and keys.
     * 
     * @param record the record to identify
     * @param keys the specific keys to include in the identifier
     * @return a byte array containing the unique identifier
     */
    private static byte[] hash(Record record, String... keys) {
        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putLong(record.id());
        for (String key : keys) {
            hasher.putString(key, StandardCharsets.UTF_8);
        }
        return hasher.hash().asBytes();
    }

    /**
     * The unique identifier for this checksum.
     */
    private final byte[] hash;

    /**
     * Creates a new {@link Signature} for the specified record and keys.
     * 
     * @param record the record to create a checksum for
     * @param keys the specific keys to include in the checksum
     */
    public Signature(Record record, String... keys) {
        this.hash = hash(record, keys);
    }

    /**
     * Creates a new {@link Signature} for the specified record and keys.
     * 
     * @param record the record to create a checksum for
     * @param keys the specific keys to include in the checksum
     */
    public Signature(Record record, Collection<String> keys) {
        this(record, keys.toArray(Array.containing()));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(hash);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Signature) {
            return Arrays.equals(hash, ((Signature) obj).hash);
        }
        else {
            return false;
        }
    }
}