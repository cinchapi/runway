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
package com.cinchapi.runway.util;

import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.util.OnDemandMap.ComputedEntry;
import com.cinchapi.runway.util.OnDemandMap.DerivedEntry;

/**
 * Unit tests for {@link OnDemandMap}.
 *
 * @author Jeff Nelson
 */
public class OnDemandMapTest {

    /**
     * <strong>Goal:</strong> Verify that a derived value is produced once and
     * then returned by every read.
     * <p>
     * <strong>Start state:</strong> An {@link OnDemandMap} that derives
     * {@code a} from a counting supplier.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code a} with {@code get} twice.</li>
     * <li>Read the value of the entry for {@code a}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Every read returns {@code 1}, and the supplier
     * ran once.
     */
    @Test
    public void testDeriveProducesValueOnce() {
        AtomicInteger runs = new AtomicInteger();
        OnDemandMap<String, Object> map = new OnDemandMap<>();
        map.derive("a", runs::incrementAndGet);
        Assert.assertEquals(1, map.get("a"));
        Assert.assertEquals(1, map.get("a"));
        Assert.assertEquals(1, entry(map, "a").getValue());
        Assert.assertEquals(1, runs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a computed value is produced anew by
     * every read.
     * <p>
     * <strong>Start state:</strong> An {@link OnDemandMap} that computes
     * {@code a} from a counting supplier.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code a} with {@code get} twice.</li>
     * <li>Read the value of the entry for {@code a}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reads return {@code 1}, {@code 2} and
     * {@code 3}.
     */
    @Test
    public void testComputeProducesValueOnEveryRead() {
        AtomicInteger runs = new AtomicInteger();
        OnDemandMap<String, Object> map = new OnDemandMap<>();
        map.compute("a", runs::incrementAndGet);
        Assert.assertEquals(1, map.get("a"));
        Assert.assertEquals(2, map.get("a"));
        Assert.assertEquals(3, entry(map, "a").getValue());
    }

    /**
     * <strong>Goal:</strong> Verify that listing the entries produces no
     * derived or computed value, and that each entry declares its kind.
     * <p>
     * <strong>Start state:</strong> An {@link OnDemandMap} that holds
     * {@code a}, derives {@code b} and computes {@code c}, each derived or
     * computed from a supplier that fails the test.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Find the entry for each key.</li>
     * <li>Read the value of the entry for {@code a}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value of {@code a} is {@code 1}, {@code b}
     * is a {@link DerivedEntry}, {@code c} is a {@link ComputedEntry}, and no
     * supplier ran.
     */
    @Test
    public void testEntrySetProducesNoValues() {
        OnDemandMap<String, Object> map = new OnDemandMap<>();
        map.put("a", 1);
        map.derive("b", () -> {
            throw new AssertionError("b was derived");
        });
        map.compute("c", () -> {
            throw new AssertionError("c was computed");
        });
        Assert.assertEquals(1, entry(map, "a").getValue());
        Assert.assertTrue(entry(map, "b") instanceof DerivedEntry);
        Assert.assertTrue(entry(map, "c") instanceof ComputedEntry);
    }

    /**
     * <strong>Goal:</strong> Verify that a derived value whose supplier fails
     * is produced again by the next read.
     * <p>
     * <strong>Start state:</strong> An {@link OnDemandMap} that derives
     * {@code a} from a supplier that fails on its first run.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code a} with {@code get} and expect an
     * {@link IllegalStateException}.</li>
     * <li>Read {@code a} with {@code get} again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second read returns {@code 2}.
     */
    @Test
    public void testDeriveProducesValueAgainAfterFailure() {
        AtomicInteger runs = new AtomicInteger();
        OnDemandMap<String, Object> map = new OnDemandMap<>();
        map.derive("a", () -> {
            if(runs.incrementAndGet() == 1) {
                throw new IllegalStateException();
            }
            else {
                return runs.get();
            }
        });
        try {
            map.get("a");
            Assert.fail("Expected IllegalStateException");
        }
        catch (IllegalStateException e) {
            Assert.assertEquals(1, runs.get());
        }
        Assert.assertEquals(2, map.get("a"));
    }

    /**
     * <strong>Goal:</strong> Verify that a read of a derived key while its
     * supplier runs returns {@code null} instead of running the supplier again.
     * <p>
     * <strong>Start state:</strong> An {@link OnDemandMap} that derives
     * {@code a} from a supplier that reads {@code a}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code a} with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is {@code "a:null"}.
     */
    @Test
    public void testDerivedKeyReadWhileItsSupplierRunsIsNull() {
        OnDemandMap<String, Object> map = new OnDemandMap<>();
        map.derive("a", () -> "a:" + map.get("a"));
        Assert.assertEquals("a:null", map.get("a"));
    }

    /**
     * <strong>Goal:</strong> Verify that a later registration of a key replaces
     * the earlier one, whatever their kinds.
     * <p>
     * <strong>Start state:</strong> An empty {@link OnDemandMap}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Derive {@code a}, then compute {@code a}, and read it.</li>
     * <li>Put a value for {@code a} and read it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first read returns {@code "computed"} and
     * the second returns {@code "stored"}.
     */
    @Test
    public void testLaterRegistrationReplacesEarlierOne() {
        OnDemandMap<String, Object> map = new OnDemandMap<>();
        map.derive("a", () -> "derived");
        map.compute("a", () -> "computed");
        Assert.assertEquals("computed", map.get("a"));
        map.put("a", "stored");
        Assert.assertEquals("stored", map.get("a"));
    }

    /**
     * Return the entry for {@code key} in {@code map}.
     *
     * @param map the {@link OnDemandMap} to search
     * @param key the key of the entry
     * @return the entry for {@code key}
     */
    private static Entry<String, Object> entry(OnDemandMap<String, Object> map,
            String key) {
        return map.entrySet().stream()
                .filter(entry -> entry.getKey().equals(key)).findFirst()
                .orElseThrow(AssertionError::new);
    }

}
