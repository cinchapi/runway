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
package com.cinchapi.runway;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableSet;

/**
 * Behavioral contract tests for {@link ReadHandle} implementations.
 * <p>
 * Concrete subclasses supply the implementation under test by overriding
 * {@link #newReadHandle()} and inherit the full suite of behavioral tests.
 *
 * @author Jeff Nelson
 */
public abstract class ReadHandleTest extends ClientServerTest {

    /**
     * The {@link Concourse} connection passed to every {@link ReadHandle} under
     * test.
     */
    protected Concourse concourse;

    @Override
    public void afterStartedTest() {
        concourse.close();
    }

    @Override
    public void beforeEachTest() {
        concourse = Concourse.at().port(server.getClientPort()).connect();
    }

    /**
     * <strong>Goal:</strong> Verify that a single recorded
     * {@code find(Criteria)} returns the matching record ids from
     * {@link ReadHandle#materialize()}.
     * <p>
     * <strong>Start state:</strong> Two records are added &mdash; one with
     * {@code score = 5} and one with {@code score = 10}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} with a criteria matching the {@code score = 10}
     * record.</li>
     * <li>Call {@code reader.materialize()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The list returned by {@code materialize()} has
     * exactly one element &mdash; a {@link Set} containing the
     * {@code score = 10} record's id.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testFindByCriteriaMaterializesMatchingIds() {
        long low = concourse.add("score", 5);
        long high = concourse.add("score", 10);

        ReadHandle reader = newReadHandle();
        reader.find(Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(7));
        List<Object> results = reader.materialize();

        Assert.assertEquals(1, results.size());
        Set<Long> ids = (Set<Long>) results.get(0);
        Assert.assertEquals(ImmutableSet.of(high), ids);
        Assert.assertFalse(ids.contains(low));
    }

    /**
     * <strong>Goal:</strong> Verify that calling
     * {@link ReadHandle#materialize()} on a reader with no recorded reads
     * returns an empty list.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link ReadHandle}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code reader.materialize()} without recording any reads
     * first.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned list is empty.
     */
    @Test
    public void testMaterializeWithNoRecordedReadsReturnsEmptyList() {
        ReadHandle reader = newReadHandle();
        Assert.assertTrue(reader.materialize().isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that multiple recorded reads each produce
     * their own correct value at the matching index of the list returned by
     * {@link ReadHandle#materialize()}.
     * <p>
     * <strong>Start state:</strong> Three records are added &mdash; two
     * "active" and one "inactive".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} for active records.</li>
     * <li>Record a {@code find} for inactive records.</li>
     * <li>Call {@code reader.materialize()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first list element is a {@link Map} keyed
     * by the two active record ids; the second list element is a {@link Set}
     * containing the single inactive record id.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testMultipleReadsMaterializeIndependently() {
        long a = concourse.add("active", true);
        long b = concourse.add("active", true);
        long c = concourse.add("active", false);

        ReadHandle reader = newReadHandle();
        reader.select(Criteria.where().key("active").operator(Operator.EQUALS)
                .value(true));
        reader.find(Criteria.where().key("active").operator(Operator.EQUALS)
                .value(false));
        List<Object> results = reader.materialize();

        Assert.assertEquals(2, results.size());
        Map<Long, Map<String, Set<Object>>> activeData = (Map<Long, Map<String, Set<Object>>>) results
                .get(0);
        Set<Long> inactiveIds = (Set<Long>) results.get(1);
        Assert.assertEquals(ImmutableSet.of(a, b), activeData.keySet());
        Assert.assertEquals(ImmutableSet.of(c), inactiveIds);
    }

    /**
     * <strong>Goal:</strong> Verify that recorded reads accumulate across calls
     * to {@link ReadHandle#materialize()} &mdash; materialization does not
     * reset the recorder, so reads recorded after a previous materialization
     * extend the result list rather than starting a new one.
     * <p>
     * <strong>Start state:</strong> Two records are added &mdash; one with
     * {@code tag = "first"} and one with {@code tag = "second"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code find} for {@code tag = "first"}, materialize, capture
     * results.</li>
     * <li>Record a {@code find} for {@code tag = "second"}, materialize,
     * capture results.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first materialization returns one entry
     * containing the "first" id. The second materialization returns two entries
     * &mdash; the "first" id at index 0 and the "second" id at index 1.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRecordsAccumulateAcrossMaterializations() {
        long first = concourse.add("tag", "first");
        long second = concourse.add("tag", "second");

        ReadHandle reader = newReadHandle();
        reader.find(Criteria.where().key("tag").operator(Operator.EQUALS)
                .value("first"));
        List<Object> firstBatch = reader.materialize();
        Assert.assertEquals(1, firstBatch.size());
        Assert.assertEquals(ImmutableSet.of(first),
                (Set<Long>) firstBatch.get(0));

        reader.find(Criteria.where().key("tag").operator(Operator.EQUALS)
                .value("second"));
        List<Object> secondBatch = reader.materialize();
        Assert.assertEquals(2, secondBatch.size());
        Assert.assertEquals(ImmutableSet.of(first),
                (Set<Long>) secondBatch.get(0));
        Assert.assertEquals(ImmutableSet.of(second),
                (Set<Long>) secondBatch.get(1));
    }

    /**
     * <strong>Goal:</strong> Verify that a single recorded
     * {@code select(Criteria)} returns the matching record's data from
     * {@link ReadHandle#materialize()}.
     * <p>
     * <strong>Start state:</strong> Two records are added &mdash; one with
     * {@code age = 30} and one with {@code age = 40}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} with a criteria matching only the
     * {@code age = 40} record.</li>
     * <li>Call {@code reader.materialize()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The list returned by {@code materialize()} has
     * exactly one element &mdash; a {@link Map} keyed by the id of the
     * {@code age = 40} record.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSelectByCriteriaMaterializesMatchingRecord() {
        long alice = concourse.add("name", "alice");
        concourse.add("age", 30, alice);
        long bob = concourse.add("name", "bob");
        concourse.add("age", 40, bob);

        ReadHandle reader = newReadHandle();
        reader.select(Criteria.where().key("age")
                .operator(Operator.GREATER_THAN).value(35));
        List<Object> results = reader.materialize();

        Assert.assertEquals(1, results.size());
        Map<Long, Map<String, Set<Object>>> data = (Map<Long, Map<String, Set<Object>>>) results
                .get(0);
        Assert.assertEquals(1, data.size());
        Assert.assertTrue(data.containsKey(bob));
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code keys} parameter on
     * {@code select(Set, Criteria)} causes only the requested keys to appear in
     * the resulting record data.
     * <p>
     * <strong>Start state:</strong> One record is added with three keys
     * ({@code name}, {@code age}, {@code city}).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record a {@code select} with {@code keys = {"name", "city"}}.</li>
     * <li>Call {@code reader.materialize()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The inner {@link Map} for the matching record
     * has exactly the {@code name} and {@code city} entries; {@code age} is
     * absent.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSelectWithKeysIncludesOnlyRequestedKeys() {
        long jeff = concourse.add("name", "jeff");
        concourse.add("age", 32, jeff);
        concourse.add("city", "Atlanta", jeff);

        ReadHandle reader = newReadHandle();
        reader.select(ImmutableSet.of("name", "city"), Criteria.where()
                .key("name").operator(Operator.EQUALS).value("jeff"));
        List<Object> results = reader.materialize();

        Map<Long, Map<String, Set<Object>>> data = (Map<Long, Map<String, Set<Object>>>) results
                .get(0);
        Map<String, Set<Object>> entry = data.get(jeff);
        Assert.assertEquals(ImmutableSet.of("name", "city"), entry.keySet());
    }

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

    /**
     * Return a fresh {@link ReadHandle} for the implementation under test.
     *
     * @return the {@link ReadHandle} under test
     */
    protected abstract ReadHandle newReadHandle();

    @Override
    protected boolean reuseServerAcrossTests() {
        return true;
    }

}
