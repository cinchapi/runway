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
package com.cinchapi.runway.meta;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.cinchapi.common.base.Array;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.runway.Computed;
import com.cinchapi.runway.Record;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * An optional interface that provides computed metadata properties for
 * {@link Record} objects. This interface enables Records to access temporal
 * information about their lifecycle without requiring explicit storage of
 * metadata fields.
 * <p>
 * Records implementing this interface gain access to computed properties that
 * provide creation and modification timestamps. These values are calculated
 * on-demand and are never cached, representing a performance vs. convenience
 * trade-off compared to storing metadata directly as fields.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Metadata {

    /**
     * Return the timestamp when this {@link Record} was first saved to the
     * database.
     *
     * @return the creation timestamp, or {@code null} if the {@link Record} has
     *         no audit history
     * @throws IllegalStateException if the {@link Record} has never been saved
     * @throws UnsupportedOperationException if this interface is implemented by
     *             a non-{@link Record} class
     */
    @Computed
    @Nullable
    public default Timestamp createdAt() {
        if(this instanceof Record) {
            Record record = (Record) this;
            Map<Timestamp, List<String>> _audit = Reflection.get("_audit",
                    record);
            if(_audit == null) {
                ConnectionPool connections = Reflection.get("connections",
                        record);
                Concourse concourse = connections.request();
                try {
                    _audit = concourse.review(record.id());
                }
                finally {
                    connections.release(concourse);
                }
                Reflection.set("_audit", _audit, record);
            }
            if(_audit.isEmpty()) {
                throw new IllegalStateException(
                        "Cannot return metadata for a Record that has never been saved");
            }
            else {
                return _audit.keySet().iterator().next();
            }
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Return the timestamp of the most recent update to any field in this
     * {@link Record}.
     *
     * @return the last update timestamp
     * @throws IllegalStateException if the {@link Record} has never been saved
     * @throws UnsupportedOperationException if this interface is implemented by
     *             a non-{@link Record} class
     */
    @Computed
    public default Timestamp lastUpdatedAt() {
        return lastUpdatedAt(Array.containing());
    }

    /**
     * Return the timestamp of the most recent update among the specified
     * fields.
     *
     * @param keys the field names to check for updates
     * @return the timestamp of the most recent update among the specified
     *         fields, or {@code null} if no updates were found
     * @throws IllegalStateException if the {@link Record} has never been saved
     * @throws UnsupportedOperationException if this interface is implemented by
     *             a non-{@link Record} class
     */
    @Nullable
    public default Timestamp lastUpdatedAt(String... keys) {
        if(this instanceof Record) {
            Record record = (Record) this;
            ConnectionPool connections = Reflection.get("connections", record);
            if(keys.length == 1) {
                Concourse concourse = connections.request();
                String key = keys[0];
                try {
                    Map<Timestamp, List<String>> audit = concourse.review(key,
                            record.id());
                    return Iterables.getLast(audit.keySet(), null);
                }
                finally {
                    connections.release(concourse);
                }
            }
            else {
                Map<Timestamp, List<String>> _audit = Reflection.get("_audit",
                        record);
                if(_audit == null) {
                    Concourse concourse = connections.request();
                    try {
                        _audit = concourse.review(record.id());
                    }
                    finally {
                        connections.release(concourse);
                    }
                    Reflection.set("_audit", _audit, record);
                }
                if(_audit.isEmpty()) {
                    throw new IllegalStateException(
                            "Cannot return metadata for a Record that has never been saved");
                }
                else if(keys.length == 0) {
                    return Iterables.getLast(_audit.keySet());
                }
                else {
                    List<Entry<Timestamp, List<String>>> entries = Lists
                            .newArrayList(_audit.entrySet());
                    ListIterator<Entry<Timestamp, List<String>>> it = entries
                            .listIterator(entries.size());
                    while (it.hasPrevious()) {
                        Map.Entry<Timestamp, List<String>> entry = it
                                .previous();
                        Timestamp timestamp = entry.getKey();
                        List<String> changes = entry.getValue();
                        for (String change : changes) {
                            // Format: ACTION <key> AS <value> IN <record> AT
                            // <timestamp>
                            String[] parts = change.split("\\s+");
                            if(parts.length >= 3) {
                                String changed = parts[1];
                                for (String key : keys) {
                                    if(changed.equals(key)) {
                                        return timestamp;
                                    }
                                }
                            }
                        }
                    }
                    return null;
                }
            }
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

}
