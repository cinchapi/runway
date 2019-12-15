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
package com.cinchapi.runway.cache;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.CacheBuilder;

/**
 * Unit tests for {@link LeasingCache}
 *
 * @author Jeff Nelson
 */
public class LeasingCacheTest {
    
    private LeasingCache<String, String> cache;
    
    @Before
    public void init() {
        cache = new LeasingCache<>(CacheBuilder.newBuilder().build());
    }
    
    @Test
    public void testInitialLease() {
        long lease = cache.lease("foo");
        Assert.assertEquals(0, lease);
    }
    
    @Test
    public void testCannotWriteToCacheWithInvalidLease() {
        Assert.assertFalse(cache.put(100, "foo", "bar"));
        Assert.assertNull(cache.getIfPresent("foo"));
        long lease = cache.lease("foo");
        cache.put("foo", "bar");
        Assert.assertFalse(cache.put(lease, "foo", "baz"));
        Assert.assertEquals("bar", cache.getIfPresent("foo"));
    }
    
    @Test
    public void testGetLoadUpdatesLease() throws ExecutionException {
        long lease = cache.lease("foo");
        String value = cache.get("foo", () -> "bar");
        Assert.assertEquals("bar", value);
        Assert.assertFalse(cache.put(lease, "foo", "baz"));
        value = cache.get("foo", () -> "bang");
        Assert.assertEquals("bar", value);
    }

}
