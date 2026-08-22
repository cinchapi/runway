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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.runway.CountingConcourseConnectionPool.CountingConcourse;

/**
 * Round-trip count regression tests for {@link Runway#save(Record...)} and
 * {@link Runway#save(boolean, Record...)} when routed through the
 * {@link com.cinchapi.runway.db.BatchSaver BatchSaver}.
 * <p>
 * Each test reflectively replaces the {@link Runway} connection pool with a
 * {@link CountingConcourseConnectionPool}, performs one save, and asserts the
 * exact number of {@code submit(CommandGroup)} round trips it issued. These
 * tests lock in the {@code 2.0.0} contract that a save with no validation reads
 * costs {@code 1} round trip and a save that needs at least one
 * {@link Unique @Unique}-uniqueness check, a {@code preventStaleWrites} audit,
 * or an existence verification for a previously persisted record costs
 * {@code 2}, regardless of how many records the save covers.
 * </p>
 *
 * @author Jeff Nelson
 */
public class SaveRoundTripCountTest extends RunwayBaseClientServerTest {

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", true, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that saving one {@link Record} with no
     * {@link Unique @Unique} fields and {@code preventStaleWrites=false} costs
     * exactly one server round trip through the
     * {@link com.cinchapi.runway.db.BatchSaver BatchSaver}.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} with bulk-commands forced
     * on and a fresh {@link Plain} instance in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call {@code runway.save(record)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly one
     * {@code submit(CommandGroup)} round trip is observed &mdash; the single
     * writes-plus-commit submission.
     */
    @Test
    public void testSingleRecordWithoutValidationCostsOneRoundTrip() {
        Plain record = new Plain("alpha", 7);
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(record));
        Assert.assertEquals(1, rpcs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that saving one {@link Record} with a
     * {@link Unique @Unique} field costs exactly two server round trips through
     * the {@link com.cinchapi.runway.db.BatchSaver BatchSaver}.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} with bulk-commands forced
     * on and a fresh {@link UniqueNamed} instance in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call {@code runway.save(record)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly two
     * {@code submit(CommandGroup)} round trips are observed &mdash; one for the
     * writes-plus-uniqueness-{@code find} batch and one for the commit.
     */
    @Test
    public void testSingleRecordWithUniqueCostsTwoRoundTrips() {
        UniqueNamed record = new UniqueNamed("alpha");
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(record));
        Assert.assertEquals(2, rpcs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that re-saving a previously-loaded
     * {@link Record} under {@code preventStaleWrites=true} costs exactly two
     * server round trips through the {@link com.cinchapi.runway.db.BatchSaver
     * BatchSaver}.
     * <p>
     * <strong>Start state:</strong> A {@link Plain} that has been saved and
     * then reloaded, with one mutated field in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save and reload a {@link Plain} before instrumentation, so the record
     * carries a non-zero checkpoint timestamp that arms the stale-data
     * audit.</li>
     * <li>Mutate the reloaded record.</li>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call
     * {@code runway.save(true, reloaded)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly two
     * {@code submit(CommandGroup)} round trips are observed &mdash; one for the
     * audit-plus-writes batch and one for the commit.
     */
    @Test
    public void testSingleRecordWithStaleCheckOnLoadedCostsTwoRoundTrips() {
        Plain record = new Plain("alpha", 7);
        Assert.assertTrue(runway.save(record));
        Plain reloaded = runway.load(Plain.class, record.id());
        reloaded.value = 8;
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(true, reloaded));
        Assert.assertEquals(2, rpcs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that re-saving an unchanged
     * previously-loaded {@link Record} under {@code preventStaleWrites=true}
     * costs exactly one server round trip, because a save that writes nothing
     * checks nothing.
     * <p>
     * <strong>Start state:</strong> A {@link Plain} that has been saved and
     * then reloaded, with no mutation.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save and reload a {@link Plain} before instrumentation, so the record
     * carries a non-zero checkpoint timestamp.</li>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call
     * {@code runway.save(true, reloaded)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly one
     * {@code submit(CommandGroup)} round trip is observed &mdash; the single
     * writes-plus-commit submission, with no stale-check read.
     */
    @Test
    public void testUnchangedRecordWithStaleCheckCostsOneRoundTrip() {
        Plain record = new Plain("alpha", 7);
        Assert.assertTrue(runway.save(record));
        Plain reloaded = runway.load(Plain.class, record.id());
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(true, reloaded));
        Assert.assertEquals(1, rpcs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that re-saving a previously-loaded
     * {@link Record} with both a {@link Unique @Unique} field and
     * {@code preventStaleWrites=true} still costs exactly two server round
     * trips &mdash; the audit and the uniqueness {@code find} ride along in the
     * same {@code flushReads} batch as the writes.
     * <p>
     * <strong>Start state:</strong> A {@link UniqueNamed} that has been saved
     * and reloaded, with a new unique name in memory so the save both audits
     * and checks uniqueness.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save and reload a {@link UniqueNamed} before instrumentation.</li>
     * <li>Give the reloaded record a different unique name.</li>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call
     * {@code runway.save(true, reloaded)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly two
     * {@code submit(CommandGroup)} round trips are observed.
     */
    @Test
    public void testSingleRecordWithUniqueAndStaleCheckCostsTwoRoundTrips() {
        UniqueNamed record = new UniqueNamed("alpha");
        Assert.assertTrue(runway.save(record));
        UniqueNamed reloaded = runway.load(UniqueNamed.class, record.id());
        reloaded.name = "beta";
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(true, reloaded));
        Assert.assertEquals(2, rpcs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a bulk save of {@code N}
     * validation-free {@link Record Records} still costs exactly one server
     * round trip &mdash; not {@code N} &mdash; through the
     * {@link com.cinchapi.runway.db.BatchSaver BatchSaver}.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} with bulk-commands forced
     * on and ten fresh {@link Plain} instances in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call {@code runway.save(records...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly one
     * {@code submit(CommandGroup)} round trip is observed regardless of how
     * many records the call covers.
     */
    @Test
    public void testBulkSaveWithoutValidationCostsOneRoundTrip() {
        Plain[] records = new Plain[10];
        for (int i = 0; i < records.length; i++) {
            records[i] = new Plain("r" + i, i);
        }
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(records));
        Assert.assertEquals(1, rpcs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a bulk save of {@code N} {@link Record
     * Records} each carrying a {@link Unique @Unique} field costs exactly two
     * server round trips &mdash; not {@code 2 * N} &mdash; because every
     * record's uniqueness {@code find} accumulates into a single
     * {@code flushReads} batch alongside the writes.
     * <p>
     * <strong>Start state:</strong> Ten fresh {@link UniqueNamed} instances
     * with distinct names.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call {@code runway.save(records...)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly two
     * {@code submit(CommandGroup)} round trips are observed.
     */
    @Test
    public void testBulkSaveWithUniqueCostsTwoRoundTrips() {
        UniqueNamed[] records = new UniqueNamed[10];
        for (int i = 0; i < records.length; i++) {
            records[i] = new UniqueNamed("u" + i);
        }
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(records));
        Assert.assertEquals(2, rpcs.get());
    }

    /**
     * Reflectively replace {@link #runway runway's} connection pool with a
     * {@link CountingConcourseConnectionPool} and return that pool's shared RPC
     * counter, already reset to zero so the next save's round trips are the
     * only ones tallied.
     *
     * @return the RPC counter that the swapped-in pool shares across every
     *         connection it produces
     */
    private AtomicInteger installCountingPool() {
        ConnectionPool pool = new CountingConcourseConnectionPool(
                Concourse.connect("localhost", server.getClientPort(), "admin",
                        "admin", environment));
        Reflection.set("connections", pool, runway); // (authorized)
        Concourse connection = pool.request();
        AtomicInteger rpcs = ((CountingConcourse) connection).rpcs();
        pool.release(connection);
        rpcs.set(0);
        return rpcs;
    }

    /**
     * A minimal {@link Record} with no {@link Unique @Unique} fields, used to
     * exercise the save path that needs no validation reads.
     *
     * @author Jeff Nelson
     */
    public static class Plain extends Record {

        /**
         * An arbitrary label.
         */
        String name;

        /**
         * An arbitrary integer value.
         */
        int value;

        /**
         * Construct a new instance.
         *
         * @param name the {@link #name} value
         * @param value the {@link #value} value
         */
        public Plain(String name, int value) {
            this.name = name;
            this.value = value;
        }

    }

    /**
     * A {@link Record} with one {@link Unique @Unique} field, used to exercise
     * the save path that adds a uniqueness {@code find} to the
     * {@code flushReads} batch.
     *
     * @author Jeff Nelson
     */
    public static class UniqueNamed extends Record {

        /**
         * The unique name.
         */
        @Unique
        String name;

        /**
         * Construct a new instance.
         *
         * @param name the {@link #name} value
         */
        public UniqueNamed(String name) {
            this.name = name;
        }

    }

    /**
     * <strong>Goal:</strong> Verify that re-saving a previously persisted
     * {@link Record} costs exactly two server round trips, since the save
     * verifies that the {@link Record} still exists before it writes.
     * <p>
     * <strong>Start state:</strong> A {@link Plain} that has been saved, with
     * one mutated field in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Plain} before instrumentation, so the record carries a
     * baseline.</li>
     * <li>Mutate the record.</li>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and call {@code runway.save(record)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly two
     * {@code submit(CommandGroup)} round trips are observed &mdash; one for the
     * existence-verification-plus-writes batch and one for the commit.
     */
    @Test
    public void testResaveOfPersistedRecordCostsTwoRoundTrips() {
        Plain record = new Plain("alpha", 7);
        Assert.assertTrue(runway.save(record));
        record.value = 8;
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(record));
        Assert.assertEquals(2, rpcs.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a bulk re-save of {@code N} previously
     * persisted {@link Record Records} costs exactly two server round trips,
     * not {@code 2 * N}, because every existence verification accumulates into
     * a single batch.
     * <p>
     * <strong>Start state:</strong> Ten saved {@link Plain} records, each with
     * one mutated field in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save ten {@link Plain} records before instrumentation.</li>
     * <li>Mutate each record.</li>
     * <li>Install a {@link CountingConcourseConnectionPool} on
     * {@link #runway}.</li>
     * <li>Reset the RPC counter and save all ten together.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and exactly two
     * {@code submit(CommandGroup)} round trips are observed.
     */
    @Test
    public void testBulkResaveOfPersistedRecordsCostsTwoRoundTrips() {
        Plain[] records = new Plain[10];
        for (int i = 0; i < records.length; i++) {
            records[i] = new Plain("r" + i, i);
        }
        Assert.assertTrue(runway.save(records));
        for (Plain record : records) {
            record.value = record.value + 1;
        }
        AtomicInteger rpcs = installCountingPool();
        Assert.assertTrue(runway.save(records));
        Assert.assertEquals(2, rpcs.get());
    }

}
