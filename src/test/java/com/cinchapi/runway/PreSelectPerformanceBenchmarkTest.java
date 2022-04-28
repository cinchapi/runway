/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.runway;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.test.CrossVersionTest;
import com.cinchapi.concourse.test.runners.CrossVersionTestRunner.Versions;
import com.cinchapi.runway.Runway.ReadStrategy;

/**
 * Test pre-select performance compared to legacy behaviour.
 *
 * @author Jeff Nelson
 */
@Versions({ "0.11.2", "latest" })
public class PreSelectPerformanceBenchmarkTest extends CrossVersionTest {

    @Override
    public void beforeEachTest() {
        Runway db = Runway.builder().port(server.getClientPort()).build();
        int count = 5000;
        for (int i = 1; i <= count; ++i) {
            Company company = new Company("company" + count, count);
            company.a = "a" + count;
            company.b = "b" + count;
            company.assign(db);
            company.save();
            Address address = new Address();
            address.a = "a" + count;
            address.b = "b" + count;
            address.c = "c" + count;
            address.d = "d" + count;
            address.assign(db);
            address.save();
            User user = new User("user" + count, company);
            user.a = "a" + count;
            user.b = "b" + count;
            user.c = "c" + count;
            user.address = address;
            user.assign(db);
            user.save();
        }
    }

    @Test
    public void testPerformance() {
        Runway db = Runway.builder().readStrategy(ReadStrategy.BULK)
                .port(server.getClientPort()).build();
        Benchmark benchmark = new Benchmark(TimeUnit.SECONDS) {

            @Override
            public void action() {
                Set<User> users = db.load(User.class);
                users.forEach(user -> {
                    Assert.assertNotNull(user.company.name);
                    Assert.assertNotNull(user.company.a);
                    Assert.assertNotNull(user.company.b);
                    Assert.assertNotNull(user.address.a);
                    Assert.assertNotNull(user.address.b);
                    Assert.assertNotNull(user.address.c);
                    Assert.assertNotNull(user.address.d);
                });
            }

        };
        long time = benchmark.run();
        record("Performance", time);

    }

    class User extends Record {

        String name;
        String a;
        String b;
        String c;
        Company company;
        Address address;

        public User(String name, Company company) {
            this.name = name;
            this.company = company;
        }
    }

    class Company extends Record {

        String name;
        String a;
        String b;
        int index;

        public Company(String name, int index) {
            this.name = name;
            this.index = index;
        }
    }

    class Address extends Record {

        String a;
        String b;
        String c;
        String d;
    }

}
