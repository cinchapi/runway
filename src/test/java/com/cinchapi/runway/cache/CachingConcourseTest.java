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

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link CacheEnabledConcourse}.
 *
 * @author Jeff Nelson
 */
public class CachingConcourseTest extends ClientServerTest {
    
    private CacheEnabledConcourse db;
    private Cache<Long, Map<String, Set<Object>>> cache;

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }
    
    @Override
    public void beforeEachTest() {
        cache = CacheBuilder.newBuilder().build();
        db = new CacheEnabledConcourse(client, cache);
    }
    
    @Test
    public void testSelectUsesCache() {
        long record = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company", "Cinchapi", "age", 100));
        Map<String, Set<Object>> expected = client.select(record);
        Map<String, Set<Object>> actual = db.select(record);
        Assert.assertEquals(expected, actual);
        Assert.assertNotSame(expected, client.select(record));
        Assert.assertSame(actual, db.select(record));
    }
    
    @Test
    public void testAddInvalidatesCache() {
        long record = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company", "Cinchapi", "age", 100));
        Map<String, Set<Object>> a = db.select(record);
        db.add("score", 5, record);
        Map<String, Set<Object>> b = db.select(record);
        Assert.assertEquals(b, client.select(record));
        Assert.assertNotSame(a, b);
        Assert.assertSame(db.select(record), b);
    }
    
    @Test
    public void testSelectMultipleRecordsCachesThemAll() {
        long a = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company", "Cinchapi", "age", 100));
        long b = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company", "Cinchapi", "age", 100));
        long c = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company", "Cinchapi", "age", 100));
        Map<Long, Map<String, Set<Object>>> data = db.select(ImmutableList.of(a,b,c));
        while(cache.getIfPresent(a) == null || cache.getIfPresent(b) == null || cache.getIfPresent(c) == null) {
            // Wait for cache to populate in background
            continue;
        }
        Assert.assertSame(data.get(a), db.select(a));
        Assert.assertSame(data.get(b), db.select(b));
        Assert.assertSame(data.get(c), db.select(c));
        db.add("artist", "Common", a);
        Assert.assertNotSame(data.get(a), db.select(a));
        Assert.assertSame(data.get(b), db.select(b));
        Assert.assertSame(data.get(c), db.select(c));
    }

}
