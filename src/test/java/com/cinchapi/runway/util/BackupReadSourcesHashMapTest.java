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

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.collect.Continuation;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link BackupReadSourcesHashMap}.
 *
 * @author Jeff Nelson
 */
public class BackupReadSourcesHashMapTest {

    @Test
    public void testMultiSourceHashMapGet() {
        Map<String, Supplier<Object>> computed = ImmutableMap.of("b",
                () -> Continuation.of(UUID::randomUUID));
        Map<String, Object> data = BackupReadSourcesHashMap.create(
                ImmutableMap.of("a", 1), new AbstractMap<String, Object>() {
                    // Wrap the #computed data in a map that computes the
                    // requested values on-demand.

                    @Override
                    public Set<Entry<String, Object>> entrySet() {
                        return computed.entrySet().stream().map(
                                e -> new AbstractMap.SimpleImmutableEntry<>(
                                        e.getKey(), e.getValue().get()))
                                .collect(Collectors.toSet());
                    }

                    @Override
                    public Object get(Object key) {
                        Supplier<?> computer = computed.get(key);
                        if(computer != null) {
                            return computer.get();
                        }
                        else {
                            return null;
                        }
                    }

                });
        data.put("c", 3);
        Assert.assertEquals(1, data.get("a"));
        Assert.assertEquals(3, data.get("c"));
        Assert.assertTrue(data.get("b") instanceof Continuation);
    }

}
