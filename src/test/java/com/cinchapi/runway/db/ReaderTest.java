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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.google.common.collect.ImmutableSet;

/**
 * Behavioral contract tests for {@link Reader} implementations.
 * <p>
 * Concrete subclasses supply the implementation under test by overriding
 * {@link #instantiateReader(Concourse)} and inherit the full suite of
 * behavioral tests.
 *
 * @author Jeff Nelson
 */
public abstract class ReaderTest extends RunwayBaseClientServerTest {

    /**
     * Tracks the {@link Concourse} connections wrapped by every {@link Reader}
     * returned from {@link #newReader()} so they can be released in
     * {@link #afterTestRun()} instead of leaking against a server reused across
     * tests.
     */
    private final List<Concourse> readerConnections = new ArrayList<>();

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
     * <li>Resolve the {@link Pending}.</li>
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
        Set<Long> ids = resolve(reader, reader.find(Criteria.where()
                .key("score").operator(Operator.GREATER_THAN).value(7)));

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
     * <li>Drain the {@link Reader} and capture both results.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The select resolves to a {@link Map} keyed by
     * the two active record ids; the find resolves to a {@link Set} containing
     * the single inactive record id.
     */
    @Test
    public void testMultipleReadsResolveIndependently() {
        long a = client.add("active", true);
        long b = client.add("active", true);
        long c = client.add("active", false);

        Reader reader = newReader();
        AtomicReference<Map<Long, Map<String, Set<Object>>>> active = new AtomicReference<>();
        AtomicReference<Set<Long>> inactive = new AtomicReference<>();
        reader.select(Criteria.where().key("active").operator(Operator.EQUALS)
                .value(true)).onResolve(active::set);
        reader.find(Criteria.where().key("active").operator(Operator.EQUALS)
                .value(false)).onResolve(inactive::set);
        reader.drain();

        Assert.assertEquals(ImmutableSet.of(a, b), active.get().keySet());
        Assert.assertEquals(ImmutableSet.of(c), inactive.get());
    }

    /**
     * <strong>Goal:</strong> Verify that reads recorded after a previous
     * {@link Reader#drain()} still resolve correctly on a subsequent drain.
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
        Set<Long> firstResult = resolve(reader, reader.find(Criteria.where()
                .key("tag").operator(Operator.EQUALS).value("first")));
        Assert.assertEquals(ImmutableSet.of(first), firstResult);

        Set<Long> secondResult = resolve(reader, reader.find(Criteria.where()
                .key("tag").operator(Operator.EQUALS).value("second")));
        Assert.assertEquals(ImmutableSet.of(second), secondResult);
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
     * <li>Resolve the {@link Pending}.</li>
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
        Map<Long, Map<String, Set<Object>>> data = resolve(reader,
                reader.select(Criteria.where().key("age")
                        .operator(Operator.GREATER_THAN).value(35)));

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
     * <li>Resolve the {@link Pending}.</li>
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
        Map<Long, Map<String, Set<Object>>> data = resolve(reader,
                reader.select(ImmutableSet.of("name", "city"), Criteria.where()
                        .key("name").operator(Operator.EQUALS).value("jeff")));
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
     * <li>Resolve the {@link Pending}.</li>
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
        Long count = resolve(reader, reader.count("score", Criteria.where()
                .key("score").operator(Operator.GREATER_THAN).value(3)));

        Assert.assertEquals(Long.valueOf(2L), count);
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
     * <li>Resolve the {@link Pending}.</li>
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
        Map<String, Set<Object>> data = resolve(reader, reader.select(id));

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
     * <li>Resolve the {@link Pending}.</li>
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
        Map<String, Set<Object>> data = resolve(reader,
                reader.select(ImmutableSet.of("name", "city"), id));

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
     * <li>Resolve the {@link Pending}.</li>
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
        Set<Object> values = resolve(reader, reader.select("tag", id));

        Assert.assertEquals(ImmutableSet.of("alpha", "beta", "gamma"), values);
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
     * <li>Resolve the {@link Pending}.</li>
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
        Object value = resolve(reader, reader.get("status", id));

        Assert.assertEquals("approved", value);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Reader#select(java.util.Collection)} resolves to a {@link Map}
     * keyed by every record id in the supplied collection.
     * <p>
     * <strong>Start state:</strong> Three records exist with values stored
     * under {@code tag}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} for an explicit set of two of the three
     * record ids.</li>
     * <li>Resolve the {@link Pending}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The resolved {@link Map} contains exactly the
     * two requested record ids; the unrequested record is absent.
     */
    @Test
    public void testSelectByRecordCollectionYieldsRequestedRecords() {
        long alpha = client.add("tag", "alpha");
        long bravo = client.add("tag", "bravo");
        long charlie = client.add("tag", "charlie");

        Reader reader = newReader();
        Map<Long, Map<String, Set<Object>>> data = resolve(reader,
                reader.select(ImmutableSet.of(alpha, bravo)));

        Assert.assertEquals(ImmutableSet.of(alpha, bravo), data.keySet());
        Assert.assertFalse(data.containsKey(charlie));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Pending#then chained
     * continuation} can record a follow-up read whose result is observed by a
     * subsequent {@link Pending#onResolve} sink in the same drain.
     * <p>
     * <strong>Start state:</strong> Two records exist &mdash; one with
     * {@code stage = "first"} and one with {@code stage = "second"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} for {@code stage = "first"}.</li>
     * <li>Chain {@code .then} so that the result of the first find is observed
     * and a second {@code find} for {@code stage = "second"} is recorded.</li>
     * <li>Drain the {@link Reader}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both finds resolve and the captured result of
     * the chained find contains the {@code stage = "second"} record's id.
     */
    @Test
    public void testThenChainsAcrossDrainPasses() {
        long first = client.add("stage", "first");
        long second = client.add("stage", "second");

        Reader reader = newReader();
        AtomicReference<Set<Long>> firstResult = new AtomicReference<>();
        AtomicReference<Set<Long>> secondResult = new AtomicReference<>();
        reader.find(Criteria.where().key("stage").operator(Operator.EQUALS)
                .value("first")).then(ids -> {
                    firstResult.set(ids);
                    return reader.find(Criteria.where().key("stage")
                            .operator(Operator.EQUALS).value("second"));
                }).onResolve(secondResult::set);
        reader.drain();

        Assert.assertEquals(ImmutableSet.of(first), firstResult.get());
        Assert.assertEquals(ImmutableSet.of(second), secondResult.get());
    }

    /**
     * Capture the value of {@code pending} by attaching an
     * {@link Pending#onResolve} sink, calling {@link Reader#drain()}, and
     * returning the captured value.
     *
     * @param reader the {@link Reader} that owns {@code pending}
     * @param pending the {@link Pending} to resolve
     * @param <T> the value type
     * @return the resolved value
     */
    protected final <T> T resolve(Reader reader, Pending<T> pending) {
        AtomicReference<T> result = new AtomicReference<>();
        pending.onResolve(result::set);
        reader.drain();
        return result.get();
    }

    /**
     * Open a fresh {@link Concourse} connection on the same environment as
     * {@link #client}, hand it to the subclass to wrap into a {@link Reader},
     * and track the connection so {@link #afterTestRun()} can release it.
     *
     * @return the {@link Reader} under test
     */
    protected final Reader newReader() {
        Concourse connection = Concourse.at().port(server.getClientPort())
                .environment(environment).connect();
        readerConnections.add(connection);
        return instantiateReader(connection);
    }

    /**
     * Wrap {@code connection} in the {@link Reader} implementation under test.
     *
     * @param connection the {@link Concourse} connection the {@link Reader}
     *            should target
     * @return the {@link Reader} under test
     */
    protected abstract Reader instantiateReader(Concourse connection);

    @Override
    protected void afterTestRun() {
        try {
            super.afterTestRun();
        }
        finally {
            for (Concourse connection : readerConnections) {
                try {
                    connection.close();
                }
                catch (Exception ignored) {/*
                                            * second-close on a connection a
                                            * test already closed is benign
                                            */}
            }
            readerConnections.clear();
        }
    }

}
