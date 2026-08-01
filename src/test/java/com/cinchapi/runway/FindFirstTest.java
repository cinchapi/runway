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

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;

/**
 * Tests for {@link DatabaseInterface#findFirst(Class, Criteria, Order)
 * findFirst} and {@link DatabaseInterface#findAnyFirst(Class, Criteria, Order)
 * findAnyFirst}. Each test runs once with bulk Command-API support enabled and
 * once with it disabled so the behavior is verified on both the bulk and
 * incremental command-read paths.
 *
 * @author Javier Lores
 */
@RunWith(Parameterized.class)
public class FindFirstTest extends RunwayBaseClientServerTest {

    /**
     * Return the parameter matrix that drives each test once per Command-API
     * capability.
     *
     * @return one row with bulk commands enabled and one with it disabled
     */
    @Parameters(name = "bulkCommands={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to exercise the bulk Command-API read
     *            path; {@code false} for the incremental path
     */
    public FindFirstTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} returns the record
     * that sorts first under an ascending {@link Order}.
     * <p>
     * <strong>Start state:</strong> Three {@link Job Jobs} with ranks 3, 2, and
     * 1 saved in non-sorted insertion order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Job Jobs} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirst} with {@code rank > 0} ordered by {@code rank}
     * ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Job} has rank 1.
     */
    @Test
    public void testFindFirstReturnsLowestUnderAscendingOrder() {
        runway.save(new Job(3), new Job(2), new Job(1));
        Job first = runway.findFirst(Job.class, rankPositive(),
                Order.by("rank").ascending());
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} returns the record
     * that sorts first under a descending {@link Order}, i.e. a different
     * record than the ascending order yields over the same match set.
     * <p>
     * <strong>Start state:</strong> Three {@link Job Jobs} with ranks 3, 2, and
     * 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Job Jobs} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirst} with {@code rank > 0} ordered by {@code rank}
     * descending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Job} has rank 3.
     */
    @Test
    public void testFindFirstReturnsHighestUnderDescendingOrder() {
        runway.save(new Job(3), new Job(2), new Job(1));
        Job first = runway.findFirst(Job.class, rankPositive(),
                Order.by("rank").descending());
        Assert.assertNotNull(first);
        Assert.assertEquals(3, first.rank);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} returns {@code null}
     * when no record matches the criteria.
     * <p>
     * <strong>Start state:</strong> Three {@link Job Jobs} whose ranks are all
     * below the criteria threshold.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Job Jobs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findFirst} with {@code rank > 100}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testFindFirstReturnsNullWhenNoMatch() {
        runway.save(new Job(1), new Job(2), new Job(3));
        Job first = runway.findFirst(
                Job.class, Criteria.where().key("rank")
                        .operator(Operator.GREATER_THAN).value(100).build(),
                Order.by("rank").ascending());
        Assert.assertNull(first);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} returns the sole
     * match when exactly one record matches.
     * <p>
     * <strong>Start state:</strong> Three {@link Job Jobs} of which only one
     * matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Job Jobs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findFirst} with {@code rank == 2}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Job} has rank 2.
     */
    @Test
    public void testFindFirstReturnsSoleMatch() {
        runway.save(new Job(1), new Job(2), new Job(3));
        Job first = runway.findFirst(
                Job.class, Criteria.where().key("rank")
                        .operator(Operator.EQUALS).value(2).build(),
                Order.by("rank").ascending());
        Assert.assertNotNull(first);
        Assert.assertEquals(2, first.rank);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} does not throw when
     * more than one record matches &mdash; the key contrast with
     * {@code findUnique} &mdash; and instead returns the first under the order.
     * <p>
     * <strong>Start state:</strong> Three {@link Job Jobs} that all match the
     * criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Job Jobs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findFirst} with {@code rank > 0} ordered by {@code rank}
     * ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> No exception is thrown and the returned
     * {@link Job} has rank 1.
     */
    @Test
    public void testFindFirstDoesNotThrowWhenMultipleMatch() {
        runway.save(new Job(1), new Job(2), new Job(3));
        Job first = runway.findFirst(Job.class, rankPositive(),
                Order.by("rank").ascending());
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyFirst} includes
     * descendant types while {@code findFirst} excludes them, over the same
     * match set.
     * <p>
     * <strong>Start state:</strong> A base {@link Job} with rank 2 and a
     * descendant {@link PriorityJob} with rank 1 (which sorts first).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Job} with rank 2 and a {@link PriorityJob} with rank
     * 1.</li>
     * <li>Call {@code findAnyFirst(Job.class, ...)} ordered by {@code rank}
     * ascending.</li>
     * <li>Call {@code findFirst(Job.class, ...)} ordered by {@code rank}
     * ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code findAnyFirst} returns the
     * {@link PriorityJob} (rank 1); {@code findFirst} returns the base
     * {@link Job} (rank 2), excluding the descendant.
     */
    @Test
    public void testFindAnyFirstIncludesDescendants() {
        runway.save(new Job(2), new PriorityJob(1));
        Job any = runway.findAnyFirst(Job.class, rankPositive(),
                Order.by("rank").ascending());
        Assert.assertTrue(any instanceof PriorityJob);
        Assert.assertEquals(1, any.rank);
        Job exact = runway.findFirst(Job.class, rankPositive(),
                Order.by("rank").ascending());
        Assert.assertNotNull(exact);
        Assert.assertFalse(exact instanceof PriorityJob);
        Assert.assertEquals(2, exact.rank);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirst} honors the requested
     * {@link Realms}, skipping a record that would sort first overall but lies
     * outside the realm.
     * <p>
     * <strong>Start state:</strong> A rank-1 {@link Job} in realm {@code other}
     * and rank-2 and rank-3 {@link Job Jobs} in realm {@code acme}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a rank-1 {@link Job} in {@code other} and rank-2 and rank-3
     * {@link Job Jobs} in {@code acme}.</li>
     * <li>Call {@code findFirst} scoped to {@code Realms.only("acme")} ordered
     * by {@code rank} ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Job} has rank 2 (the
     * rank-1 record is excluded by the realm scope).
     */
    @Test
    public void testFindFirstRespectsRealms() {
        Job other = new Job(1);
        other.addRealm("other");
        Job acmeTwo = new Job(2);
        acmeTwo.addRealm("acme");
        Job acmeThree = new Job(3);
        acmeThree.addRealm("acme");
        runway.save(other, acmeTwo, acmeThree);
        Job first = runway.findFirst(Job.class, rankPositive(),
                Order.by("rank").ascending(), Realms.only("acme"));
        Assert.assertNotNull(first);
        Assert.assertEquals(2, first.rank);
    }

    /**
     * <strong>Goal:</strong> Verify that a client-side {@link Predicate} is
     * applied before the one-row limit, so a rejected order-first row does not
     * produce a false {@code null} but instead yields the next row that both
     * matches the criteria and passes the filter.
     * <p>
     * <strong>Start state:</strong> Three {@link Job Jobs} with ranks 1, 2, and
     * 3.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Job Jobs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findFirst} ordered by {@code rank} ascending with a
     * filter that rejects the rank-1 head row.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Job} has rank 2 &mdash;
     * not {@code null}, and not the rejected rank-1 row.
     */
    @Test
    public void testFindFirstWithFilterSkipsRejectedHeadRow() {
        runway.save(new Job(1), new Job(2), new Job(3));
        Predicate<Job> notRankOne = job -> job.rank != 1;
        Job first = runway.findFirst(Job.class, rankPositive(),
                Order.by("rank").ascending(), notRankOne);
        Assert.assertNotNull(first);
        Assert.assertEquals(2, first.rank);
    }

    /**
     * <strong>Goal:</strong> Verify that the order-first record passing the
     * {@link Predicate} is returned even when it lies well past the one-row
     * page, i.e. the filter rejects several order-leading rows and the match is
     * found by widening the page rather than by inspecting only the first
     * fetched row.
     * <p>
     * <strong>Start state:</strong> Ten {@link Job Jobs} with ranks 1 through
     * 10 saved in non-sorted insertion order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Job Jobs} with ranks 1 through 10.</li>
     * <li>Call {@code findFirst} ordered by {@code rank} ascending with a
     * filter that rejects every rank below 8.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Job} has rank 8 &mdash;
     * the order-first row that passes the filter, even though ranks 1 through 7
     * sort ahead of it and are rejected.
     */
    @Test
    public void testFindFirstWithFilterSkipsRejectedRowsBeyondFirstPage() {
        runway.save(new Job(5), new Job(9), new Job(1), new Job(7), new Job(3),
                new Job(10), new Job(2), new Job(8), new Job(6), new Job(4));
        Predicate<Job> atLeastEight = job -> job.rank >= 8;
        Job first = runway.findFirst(Job.class, rankPositive(),
                Order.by("rank").ascending(), atLeastEight);
        Assert.assertNotNull(first);
        Assert.assertEquals(8, first.rank);
    }

    /**
     * <strong>Goal:</strong> Verify the connector-claim use case: under a
     * staleness criteria, {@code findFirst} selects the eligible connector
     * (lock unset or lock timestamp older than the cutoff), preferring the one
     * that has waited longest.
     * <p>
     * <strong>Start state:</strong> A fresh-locked {@link Connector} (not
     * claimable), a stale-locked {@link Connector} (claimable, oldest
     * {@code lastSyncedAt}), and an unlocked {@link Connector} (claimable).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Compute a staleness {@code cutoff} relative to
     * {@link Time#now()}.</li>
     * <li>Save a fresh-locked, a stale-locked, and an unlocked
     * {@link Connector} with distinct {@code lastSyncedAt} values.</li>
     * <li>Call {@code findFirst} with {@code locked == false OR lockedAt <
     * cutoff} ordered by {@code lastSyncedAt} ascending.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Connector} is the
     * stale-locked one (the claimable record with the oldest
     * {@code lastSyncedAt}).
     */
    @Test
    public void testFindFirstClaimsEligibleConnectorIncludingStaleLock() {
        long now = Time.now();
        long timeout = 60000000L;
        long cutoff = now - timeout;
        Connector fresh = new Connector(true, now, 300L);
        Connector stale = new Connector(true, cutoff - 1, 100L);
        Connector unlocked = new Connector(false, 0L, 200L);
        runway.save(fresh, stale, unlocked);
        Criteria claimable = Criteria.where().key("locked")
                .operator(Operator.EQUALS).value(false).or().key("lockedAt")
                .operator(Operator.LESS_THAN).value(cutoff).build();
        Connector next = runway.findFirst(Connector.class, claimable,
                Order.by("lastSyncedAt").ascending());
        Assert.assertNotNull(next);
        Assert.assertEquals(stale.id(), next.id());
        Assert.assertEquals(100L, next.lastSyncedAt);
    }

    /**
     * Return a {@link Criteria} matching every {@link Job} whose {@code rank}
     * is positive, which is every {@link Job} these tests create.
     *
     * @return the {@code rank > 0} {@link Criteria}
     */
    private static Criteria rankPositive() {
        return Criteria.where().key("rank").operator(Operator.GREATER_THAN)
                .value(0).build();
    }

    /**
     * A test {@link Record} with a single orderable {@code rank}.
     *
     * @author Javier Lores
     */
    public static class Job extends Record {

        /**
         * The orderable rank.
         */
        int rank;

        /**
         * Construct a new instance.
         *
         * @param rank the orderable rank
         */
        public Job(int rank) {
            this.rank = rank;
        }
    }

    /**
     * A descendant {@link Job} used to verify the {@code findFirst} (exact
     * type) versus {@code findAnyFirst} (type hierarchy) contract.
     *
     * @author Javier Lores
     */
    public static class PriorityJob extends Job {

        /**
         * Construct a new instance.
         *
         * @param rank the orderable rank
         */
        public PriorityJob(int rank) {
            super(rank);
        }
    }

    /**
     * A connector-flavored {@link Record} modeling the stale-lock claim use
     * case: a {@code locked} flag, the {@code lockedAt} instant the lock was
     * asserted, and the {@code lastSyncedAt} instant used to order claims.
     *
     * @author Javier Lores
     */
    public static class Connector extends Record {

        /**
         * Whether a lock is currently asserted.
         */
        boolean locked;

        /**
         * The instant the current lock was asserted.
         */
        long lockedAt;

        /**
         * The instant this connector was last synced; claims prefer the oldest.
         */
        long lastSyncedAt;

        /**
         * Construct a new instance.
         *
         * @param locked whether a lock is asserted
         * @param lockedAt the instant the lock was asserted
         * @param lastSyncedAt the instant of the last sync
         */
        public Connector(boolean locked, long lockedAt, long lastSyncedAt) {
            this.locked = locked;
            this.lockedAt = lockedAt;
            this.lastSyncedAt = lastSyncedAt;
        }
    }

}
