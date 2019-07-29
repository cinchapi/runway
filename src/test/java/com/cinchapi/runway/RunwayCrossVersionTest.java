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
package com.cinchapi.runway;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.CrossVersionTest;
import com.cinchapi.concourse.test.runners.CrossVersionTestRunner.Versions;
import com.cinchapi.concourse.time.Time;

/**
 * Unit tests for functionality that may have different implementations across
 * versions, but should work the same.
 *
 * @author Jeff Nelson
 */
@Versions({ "latest", "0.9.6" })
public class RunwayCrossVersionTest extends CrossVersionTest {
    
    private Runway runway;


    @Override
    public void afterEachTest() {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }
    
    @Override
    public void beforeEachTest() {
        runway = Runway.builder().port(server.getClientPort()).build();
        for(int i = 1; i <= 1000; ++i) {
            A a = new A();
            a.foo = "a"+i;
            a.save();
            
            AB ab = new AB();
            ab.bar = "ab"+i;
            ab.save();
            
            AC ac = new AC();
            ac.car = "ac"+i;
            ac.save();
            
            ACD acd = new ACD();
            acd.dart = "acd"+i;
            acd.save();
        }
    }
    
    @Test
    public void testLoadSortBenchmark() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                runway.loadAny(A.class, Order.by("ts"));
            }
            
        };
        double avg = benchmark.run(10) / 10;
        record("Load Sort Latency", avg);
    }
    
    @Test
    public void testLoadSortPageBenchmark() {
        Benchmark benchmark = new Benchmark(TimeUnit.MILLISECONDS) {

            @Override
            public void action() {
                runway.loadAny(A.class, Order.by("ts"), Page.of(100, 238));
            }
            
        };
        double avg = benchmark.run(10) / 10;
        record("Load Sort Page Latency", avg);
    }
    
    @Test
    public void testLoadSort() {
        Set<ACD> acds = runway.load(ACD.class, Order.by("ts").descending());
        long ts = Long.MAX_VALUE;
        for(ACD acd : acds) {
            Assert.assertTrue(acd.ts < ts);
            ts = acd.ts;
        }
    }
    
    class A extends Record {    
        String foo;
        long ts = Time.now();
    }
    
    class AB extends A {
        String bar;
    }
    
    class AC extends A {
        String car;
    }
    
    class ACD extends AC {
        String dart;
    }
    

}
