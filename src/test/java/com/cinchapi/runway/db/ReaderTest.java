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
package com.cinchapi.runway.db;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.google.common.collect.ImmutableSet;

/**
 * Behavioral contract tests for {@link Reader} implementations.
 * <p>
 * Concrete subclasses supply the implementation under test by overriding
 * {@link #newReader()} and inherit the full suite of behavioral tests.
 *
 * @author Jeff Nelson
 */
public abstract class ReaderTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a recorded {@code find(Criteria)}
     * resolves to the ids of the matching records.
     * <p>
     * <strong>Start state:</strong> Two records are added &mdash; one with
     * {@code score = 5} and one with {@code score = 10}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} with a criteria matching the {@code score = 10}
     * record.</li>
     * <li>Call {@link Supplier#get()} on the returned {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved {@link Set} contains exactly the
     * {@code score = 10} record's id.
     */
    @Test
    public void testFindByCriteriaYieldsMatchingIds() {
        long low = client.add("score", 5);
        long high = client.add("score", 10);

        Reader reader = newReader();
        Supplier<Set<Long>> supplier = reader.find(Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(7));
        Set<Long> ids = supplier.get();

        Assert.assertEquals(ImmutableSet.of(high), ids);
        Assert.assertFalse(ids.contains(low));
    }

    /**
     * <strong>Goal:</strong> Verify that multiple recorded reads each resolve
     * to their own correct value.
     * <p>
     * <strong>Start state:</strong> Three records are added &mdash; two
     * "active" and one "inactive".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} for active records.</li>
     * <li>Record a {@code find} for inactive records.</li>
     * <li>Resolve both {@link Supplier Suppliers}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The select {@link Supplier} resolves to a
     * {@link Map} keyed by the two active record ids; the find {@link Supplier}
     * resolves to a {@link Set} containing the single inactive record id.
     */
    @Test
    public void testMultipleReadsResolveIndependently() {
        long a = client.add("active", true);
        long b = client.add("active", true);
        long c = client.add("active", false);

        Reader reader = newReader();
        Supplier<Map<Long, Map<String, Set<Object>>>> active = reader
                .select(Criteria.where().key("active").operator(Operator.EQUALS)
                        .value(true));
        Supplier<Set<Long>> inactive = reader.find(Criteria.where()
                .key("active").operator(Operator.EQUALS).value(false));

        Assert.assertEquals(ImmutableSet.of(a, b), active.get().keySet());
        Assert.assertEquals(ImmutableSet.of(c), inactive.get());
    }

    /**
     * <strong>Goal:</strong> Verify that records recorded after a previous
     * resolution start a new batch and resolve to their own value, independent
     * of the prior batch.
     * <p>
     * <strong>Start state:</strong> Two records are added &mdash; one with
     * {@code tag = "first"} and one with {@code tag = "second"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} for {@code tag = "first"} and resolve it.</li>
     * <li>Record another {@code find} for {@code tag = "second"} and resolve
     * it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first resolution yields a {@link Set}
     * containing only the {@code "first"} id; the second resolution yields a
     * {@link Set} containing only the {@code "second"} id.
     */
    @Test
    public void testRecordsAfterResolutionStartNewBatch() {
        long first = client.add("tag", "first");
        long second = client.add("tag", "second");

        Reader reader = newReader();
        Supplier<Set<Long>> firstSupplier = reader.find(Criteria.where()
                .key("tag").operator(Operator.EQUALS).value("first"));
        Assert.assertEquals(ImmutableSet.of(first), firstSupplier.get());

        Supplier<Set<Long>> secondSupplier = reader.find(Criteria.where()
                .key("tag").operator(Operator.EQUALS).value("second"));
        Assert.assertEquals(ImmutableSet.of(second), secondSupplier.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a recorded {@code select(Criteria)}
     * resolves to a {@link Map} keyed by the matching records' ids.
     * <p>
     * <strong>Start state:</strong> Two records are added &mdash; one with
     * {@code age = 30} and one with {@code age = 40}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} with a criteria matching only the
     * {@code age = 40} record.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved {@link Map} has exactly one entry
     * keyed by the {@code age = 40} record's id.
     */
    @Test
    public void testSelectByCriteriaYieldsMatchingRecord() {
        long alice = client.add("name", "alice");
        client.add("age", 30, alice);
        long bob = client.add("name", "bob");
        client.add("age", 40, bob);

        Reader reader = newReader();
        Supplier<Map<Long, Map<String, Set<Object>>>> supplier = reader
                .select(Criteria.where().key("age")
                        .operator(Operator.GREATER_THAN).value(35));
        Map<Long, Map<String, Set<Object>>> data = supplier.get();

        Assert.assertEquals(1, data.size());
        Assert.assertTrue(data.containsKey(bob));
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code keys} parameter on
     * {@code select(Set, Criteria)} restricts the resolved data to only the
     * requested keys.
     * <p>
     * <strong>Start state:</strong> One record is added with three keys
     * ({@code name}, {@code age}, {@code city}).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} with {@code keys = {"name", "city"}}.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The inner {@link Map} for the matching record
     * has exactly the {@code name} and {@code city} entries; {@code age} is
     * absent.
     */
    @Test
    public void testSelectWithKeysIncludesOnlyRequestedKeys() {
        long jeff = client.add("name", "jeff");
        client.add("age", 32, jeff);
        client.add("city", "Atlanta", jeff);

        Reader reader = newReader();
        Supplier<Map<Long, Map<String, Set<Object>>>> supplier = reader
                .select(ImmutableSet.of("name", "city"), Criteria.where()
                        .key("name").operator(Operator.EQUALS).value("jeff"));
        Map<Long, Map<String, Set<Object>>> data = supplier.get();
        Map<String, Set<Object>> entry = data.get(jeff);

        Assert.assertEquals(ImmutableSet.of("name", "city"), entry.keySet());
    }

    /**
     * <strong>Goal:</strong> Verify that a recorded {@code count} resolves to
     * the number of records matching the criteria.
     * <p>
     * <strong>Start state:</strong> Three records are added with
     * {@code score = 1}, {@code score = 5}, and {@code score = 10}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code count} on the {@code score} key with a criteria
     * matching the records with {@code score > 3}.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved count is {@code 2}.
     */
    @Test
    public void testCountByCriteriaYieldsMatchCount() {
        client.add("score", 1);
        client.add("score", 5);
        client.add("score", 10);

        Reader reader = newReader();
        Supplier<Long> supplier = reader.count("score", Criteria.where()
                .key("score").operator(Operator.GREATER_THAN).value(3));

        Assert.assertEquals(Long.valueOf(2L), supplier.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code select(long record)} resolves
     * to all of the data stored in the given record.
     * <p>
     * <strong>Start state:</strong> One record is added with two keys
     * ({@code name}, {@code age}).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} for that record id.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved {@link Map} contains entries for
     * both {@code name} and {@code age}.
     */
    @Test
    public void testSelectByRecordYieldsAllFields() {
        long id = client.add("name", "jeff");
        client.add("age", 32, id);

        Reader reader = newReader();
        Supplier<Map<String, Set<Object>>> supplier = reader.select(id);
        Map<String, Set<Object>> data = supplier.get();

        Assert.assertEquals(ImmutableSet.of("name", "age"), data.keySet());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code select(Set keys, long record)}
     * resolves to only the requested keys for the given record.
     * <p>
     * <strong>Start state:</strong> One record is added with three keys
     * ({@code name}, {@code age}, {@code city}).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} for {@code keys = {"name", "city"}} on that
     * record.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved {@link Map} has exactly the
     * {@code name} and {@code city} entries.
     */
    @Test
    public void testSelectByKeysAndRecordIncludesOnlyRequestedKeys() {
        long id = client.add("name", "jeff");
        client.add("age", 32, id);
        client.add("city", "Atlanta", id);

        Reader reader = newReader();
        Supplier<Map<String, Set<Object>>> supplier = reader
                .select(ImmutableSet.of("name", "city"), id);
        Map<String, Set<Object>> data = supplier.get();

        Assert.assertEquals(ImmutableSet.of("name", "city"), data.keySet());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code select(String key, long record)} resolves to the values stored
     * under that key in the given record.
     * <p>
     * <strong>Start state:</strong> One record is added with multiple values
     * stored under the same key.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} for {@code key = "tag"} on that record.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved {@link Set} contains every value
     * that was added under {@code tag}.
     */
    @Test
    public void testSelectByKeyAndRecordYieldsAllValuesForKey() {
        long id = client.add("tag", "alpha");
        client.add("tag", "beta", id);
        client.add("tag", "gamma", id);

        Reader reader = newReader();
        Supplier<Set<Object>> supplier = reader.select("tag", id);

        Assert.assertEquals(ImmutableSet.of("alpha", "beta", "gamma"),
                supplier.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code get(String key, long record)}
     * resolves to the most recent value stored under that key in the given
     * record.
     * <p>
     * <strong>Start state:</strong> One record is added with two values stored
     * under the same key, the second one being the most recent.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code get} for {@code key = "status"} on that record.</li>
     * <li>Resolve the {@link Supplier}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved value is the most recently added
     * one.
     */
    @Test
    public void testGetByKeyAndRecordYieldsMostRecentValue() {
        long id = client.add("status", "pending");
        client.add("status", "approved", id);

        Reader reader = newReader();
        Supplier<Object> supplier = reader.get("status", id);

        Assert.assertEquals("approved", supplier.get());
    }

    /**
     * Return a fresh {@link Reader} for the implementation under test.
     *
     * @return the {@link Reader} under test
     */
    protected abstract Reader newReader();

    @Override
    protected boolean reuseServerAcrossTests() {
        return true;
    }

}
