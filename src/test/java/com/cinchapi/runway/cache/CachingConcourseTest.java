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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link CachingConcourse}.
 *
 * @author Jeff Nelson
 */
public class CachingConcourseTest extends ClientServerTest {

    private CachingConcourse db;
    private Cache<Long, Map<String, Set<Object>>> cache;

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

    @Override
    public void beforeEachTest() {
        cache = CacheBuilder.newBuilder().build();
        db = new CachingConcourse(client, cache);
    }

    @Test
    public void testSelectUsesCache() {
        long record = client.insert(ImmutableMap.of("name", "Jeff Nelson",
                "company", "Cinchapi", "age", 100));
        Map<String, Set<Object>> expected = client.select(record);
        Map<String, Set<Object>> actual = db.select(record);
        Assert.assertEquals(expected, actual);
        Assert.assertNotSame(expected, client.select(record));
        Assert.assertSame(actual, db.select(record));
    }

    @Test
    public void testAddInvalidatesCache() {
        long record = client.insert(ImmutableMap.of("name", "Jeff Nelson",
                "company", "Cinchapi", "age", 100));
        Map<String, Set<Object>> a = db.select(record);
        db.add("score", 5, record);
        Map<String, Set<Object>> b = db.select(record);
        Assert.assertEquals(b, client.select(record));
        Assert.assertNotSame(a, b);
        Assert.assertSame(db.select(record), b);
    }

    @Test
    public void testSelectMultipleRecordsCachesThemAll() {
        long a = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company",
                "Cinchapi", "age", 100));
        long b = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company",
                "Cinchapi", "age", 100));
        long c = client.insert(ImmutableMap.of("name", "Jeff Nelson", "company",
                "Cinchapi", "age", 100));
        Map<Long, Map<String, Set<Object>>> data = db
                .select(ImmutableList.of(a, b, c));
        while (cache.getIfPresent(a) == null || cache.getIfPresent(b) == null
                || cache.getIfPresent(c) == null) {
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

    @Test
    public void testCachevsNonCachePerformance() throws InterruptedException {
        List<Long> records = Lists.newArrayList();
        for (int i = 0; i < 10000; ++i) {
            records.add(client.insert(ImmutableMap.of("name",
                    Random.getString(), "count", i, "foo", Random.getString(),
                    "bar", Random.getBoolean(), "baz", Random.getNumber())));
        }
        client.select("count >= 0");
        Concourse client2 = Concourse.connect("localhost",
                server.getClientPort(), "admin", "admin");
        try {
            Benchmark cache = new Benchmark(TimeUnit.MILLISECONDS) {

                @Override
                public void action() {
                    db.select(Criteria.where().key("count")
                            .operator(Operator.GREATER_THAN_OR_EQUALS)
                            .value(0));
                }

            };

            Benchmark noCache = new Benchmark(TimeUnit.MILLISECONDS) {

                @Override
                public void action() {
                    client2.select(Criteria.where().key("count")
                            .operator(Operator.GREATER_THAN_OR_EQUALS)
                            .value(0));
                }

            };
            AtomicReference<Double> cacheTime = new AtomicReference<>(null);
            AtomicReference<Double> noCacheTime = new AtomicReference<>(null);
            Thread t1 = new Thread(() -> {
                cacheTime.set(cache.average(10));
            });
            Thread t2 = new Thread(() -> {
                noCacheTime.set(noCache.average(10));
            });
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            System.out.println("No cache took " + noCacheTime
                    + " ms and cache took " + cacheTime + " ms");
            Assert.assertTrue(cacheTime.get() - noCacheTime.get() <= 100);
        }
        finally {
            client2.close();
        }
    }

}
