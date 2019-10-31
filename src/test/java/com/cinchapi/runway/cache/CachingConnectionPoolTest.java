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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.test.ClientServerTest;
import com.google.common.cache.CacheBuilder;

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

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
    }

}
