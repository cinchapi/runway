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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.runway.Testing;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 *
 * @author jeff
 */
public class CachingConnectionPoolTest extends ClientServerTest {

    @Test
    public void testCacheIsNotNull() throws Exception {
        CachingConnectionPool pool = new CachingConnectionPool("localhost",
                server.getClientPort(), "admin", "admin", "",
                CacheBuilder.newBuilder().build());
        client.add("name", "jeff", 1);
        Concourse concourse = pool.request();
        try {
            Assert.assertEquals(CachingConcourse.class, concourse.getClass());
            concourse.select(1);
            Assert.assertTrue(true); // lack of NPE means test passes
        }
        finally {
            pool.release(concourse);
            pool.close();
        }
    }

    @Test
    public void testConcurrentBulkSelectAccuracy() throws Exception {
        CachingConnectionPool pool = new CachingConnectionPool("localhost",
                server.getClientPort(), "admin", "admin", "",
                CacheBuilder.newBuilder().build());
        AtomicInteger workers = new AtomicInteger(14);
        try {
            Set<Long> records = Sets.newLinkedHashSet();
            int count = Random.getScaleCount();
            Map<Long, Map<String, Set<Object>>> expected = Maps
                    .newLinkedHashMap();
            Concourse db = pool.request();
            for (long i = 0; i < count; ++i) {
                db.add("foo", "bar", i);
                records.add(i);
                Map<String, Set<Object>> data = Maps.newLinkedHashMap();
                data.put("foo", Sets.newHashSet("bar"));
                expected.put(i, data);
            }
            pool.release(db);
            Executor executor = Executors.newCachedThreadPool();
            AtomicBoolean done = new AtomicBoolean(false);
            for (int i = 0; i < 10; ++i) {
                executor.execute(() -> {
                    while (!done.get()) {
                        Iterator<Long> it = records.iterator();
                        Set<Long> $records = Sets.newLinkedHashSet();
                        while (it.hasNext()) {
                            long next = it.next();
                            if(Random.getScaleCount() % 3 == 0) {
                                $records.add(next);
                            }
                        }
                        workers.addAndGet(-1);
                        Concourse concourse = pool.request();
                        try {
                            concourse.select($records);
                        }
                        finally {
                            pool.release(concourse);
                            workers.incrementAndGet();
                        }
                    }
                });

            }
            for (int i = 0; i < 4; ++i) {
                executor.execute(() -> {
                    while (!done.get()) {
                        List<Long> list = Lists.newArrayList(records);
                        Collections.shuffle(list);
                        Long record = list.get(0);
                        workers.addAndGet(-1);
                        Concourse concourse = pool.request();
                        try {
                            long value = System.currentTimeMillis();
                            if(concourse.add("foo", value, record)) {
                                synchronized (record) {
                                    expected.get(record).get("foo").add(value);
                                }
                            }

                        }
                        finally {
                            pool.release(concourse);
                            workers.incrementAndGet();
                        }
                    }
                });
            }
            Thread.sleep(1000);
            done.set(true);
            Concourse con = pool.request();
            try {
                Assert.assertEquals(expected, con.select(records));
            }
            finally {
                pool.release(con);
            }
        }
        finally {
            while (workers.get() != 14) {
                continue;
            }
            pool.close();
        }

    }

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

}
