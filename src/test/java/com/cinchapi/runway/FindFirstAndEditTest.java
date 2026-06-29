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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests for
 * {@link Runway#findFirstAndEdit(Class, Criteria, Order, java.util.function.Consumer)
 * findFirstAndEdit}, the atomic claim-and-update primitive. Each test runs
 * under both Command-API modes (bulk enabled and disabled); the atomic edit
 * itself always uses the incremental, synchronously-staged transaction path, so
 * the matrix additionally guards the surrounding save and load operations under
 * both modes.
 *
 * @author Javier Lores
 */
@RunWith(Parameterized.class)
public class FindFirstAndEditTest extends RunwayBaseClientServerTest {

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
    public FindFirstAndEditTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndEdit} edits exactly
     * the record that sorts first under the {@link Order} and durably persists
     * the edit.
     * <p>
     * <strong>Start state:</strong> Three unclaimed {@link Task Tasks} with
     * ranks 3, 2, and 1 saved in non-sorted insertion order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Task Tasks} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirstAndEdit} ordered by {@code rank} ascending with
     * a consumer that sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load the returned {@link Task} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Task} has rank 1 and
     * {@code owner == "worker"}, and the re-loaded {@link Task} shows the same
     * persisted {@code owner}.
     */
    @Test
    public void testFindFirstAndEditEditsFirstUnderOrderAndPersists() {
        runway.save(new Task(3), new Task(2), new Task(1));
        Task first = runway.findFirstAndEdit(Task.class, unclaimed(),
                Order.by("rank").ascending(), task -> task.owner = "worker");
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
        Assert.assertEquals("worker", first.owner);
        Task reloaded = runway.load(Task.class, first.id());
        Assert.assertEquals("worker", reloaded.owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndEdit} applies the
     * {@link Order} client-side and still edits exactly the record that sorts
     * first when the server cannot sort or paginate natively.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} forced onto the legacy
     * path (no native sorting/pagination) with three unclaimed {@link Task
     * Tasks} of ranks 3, 2, and 1 saved in non-sorted insertion order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Force {@code hasNativeSortingAndPagination} to {@code false}.</li>
     * <li>Save {@link Task Tasks} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirstAndEdit} ordered by {@code rank} ascending with
     * a consumer that sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load the returned {@link Task} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Task} has rank 1 and
     * {@code owner == "worker"}, and the re-loaded {@link Task} shows the same
     * persisted {@code owner}.
     */
    @Test
    public void testFindFirstAndEditAppliesOrderClientSideOnLegacyServer() {
        Reflection.set("hasNativeSortingAndPagination", false, runway); // (authorized)
        runway.save(new Task(3), new Task(2), new Task(1));
        Task first = runway.findFirstAndEdit(Task.class, unclaimed(),
                Order.by("rank").ascending(), task -> task.owner = "worker");
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
        Assert.assertEquals("worker", first.owner);
        Task reloaded = runway.load(Task.class, first.id());
        Assert.assertEquals("worker", reloaded.owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndEdit} returns
     * {@code null} and never invokes the consumer when no record matches.
     * <p>
     * <strong>Start state:</strong> Three {@link Task Tasks} whose ranks are
     * all below the criteria threshold.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Task Tasks} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findFirstAndEdit} with {@code rank > 100} and a consumer
     * that flips an {@link AtomicBoolean}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} and the consumer
     * never ran.
     */
    @Test
    public void testFindFirstAndEditReturnsNullAndSkipsConsumerWhenNoMatch() {
        runway.save(new Task(1), new Task(2), new Task(3));
        AtomicBoolean consumerRan = new AtomicBoolean(false);
        Task first = runway.findFirstAndEdit(Task.class,
                Criteria.where().key("rank").operator(Operator.GREATER_THAN)
                        .value(100).build(),
                Order.by("rank").ascending(), task -> consumerRan.set(true));
        Assert.assertNull(first);
        Assert.assertFalse(consumerRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndEdit} rejects a
     * {@code null} {@link Order}, since "first" is undefined without one.
     * <p>
     * <strong>Start state:</strong> A single unclaimed {@link Task}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save one {@link Task}.</li>
     * <li>Call {@code findFirstAndEdit} with a {@code null} {@link Order}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link NullPointerException} is thrown.
     */
    @Test(expected = NullPointerException.class)
    public void testFindFirstAndEditRequiresOrder() {
        runway.save(new Task(1));
        runway.findFirstAndEdit(Task.class, unclaimed(), null,
                task -> task.owner = "worker");
    }

    /**
     * <strong>Goal:</strong> Verify the core mutual-exclusion guarantee: two
     * threads racing to claim the same single candidate cannot both succeed.
     * <p>
     * <strong>Start state:</strong> One unclaimed {@link Task}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a single unclaimed {@link Task}.</li>
     * <li>Start two threads that, gated by a common latch, each call
     * {@code findFirstAndEdit} to claim it under their own owner id.</li>
     * <li>{@code join()} both threads, then re-load the {@link Task}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Exactly one thread receives a non-null claim
     * and the other receives {@code null}; the persisted {@code owner} equals
     * the single winner's id.
     */
    @Test
    public void testFindFirstAndEditClaimsExactlyOneUnderConcurrency()
            throws InterruptedException {
        Task task = new Task(1);
        runway.save(task);
        long id = task.id();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Task> claim1 = new AtomicReference<>();
        AtomicReference<Task> claim2 = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                claim1.set(runway.findFirstAndEdit(Task.class, unclaimed(),
                        Order.by("rank").ascending(), t -> {
                            t.claimed = true;
                            t.owner = "one";
                        }));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                claim2.set(runway.findFirstAndEdit(Task.class, unclaimed(),
                        Order.by("rank").ascending(), t -> {
                            t.claimed = true;
                            t.owner = "two";
                        }));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t1.start();
        t2.start();
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        t1.join(10000);
        t2.join(10000);
        boolean oneWon = claim1.get() != null;
        boolean twoWon = claim2.get() != null;
        Assert.assertTrue("Exactly one thread must claim the task",
                oneWon ^ twoWon);
        Task reloaded = runway.load(Task.class, id);
        Assert.assertTrue(reloaded.claimed);
        Assert.assertEquals(oneWon ? "one" : "two", reloaded.owner);
    }

    /**
     * <strong>Goal:</strong> Verify that two threads racing over a candidate
     * set of two records each claim a distinct record, so no record is claimed
     * twice.
     * <p>
     * <strong>Start state:</strong> Two unclaimed {@link Task Tasks} with ranks
     * 1 and 2.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two unclaimed {@link Task Tasks}.</li>
     * <li>Start two threads that, gated by a common latch, each claim the first
     * unclaimed {@link Task} under their own owner id.</li>
     * <li>{@code join()} both threads.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both threads receive a non-null claim and the
     * two claims are distinct {@link Task Tasks} (different ids).
     */
    @Test
    public void testFindFirstAndEditConcurrentClaimsTakeDistinctRecords()
            throws InterruptedException {
        runway.save(new Task(1), new Task(2));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Task> claim1 = new AtomicReference<>();
        AtomicReference<Task> claim2 = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                claim1.set(runway.findFirstAndEdit(Task.class, unclaimed(),
                        Order.by("rank").ascending(), t -> {
                            t.claimed = true;
                            t.owner = "one";
                        }));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                claim2.set(runway.findFirstAndEdit(Task.class, unclaimed(),
                        Order.by("rank").ascending(), t -> {
                            t.claimed = true;
                            t.owner = "two";
                        }));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t1.start();
        t2.start();
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        t1.join(10000);
        t2.join(10000);
        Assert.assertNotNull(claim1.get());
        Assert.assertNotNull(claim2.get());
        Assert.assertNotEquals(claim1.get().id(), claim2.get().id());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndEdit} throws
     * {@link RetryExhaustedException} &mdash; rather than returning
     * {@code null} &mdash; when it loses the commit race on every attempt.
     * <p>
     * <strong>Start state:</strong> One unclaimed {@link Task}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a single unclaimed {@link Task}.</li>
     * <li>Call {@code findFirstAndEdit} with a consumer that, on every
     * invocation, writes to the same record through a separate connection so
     * the staged transaction always conflicts at commit time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RetryExhaustedException} is thrown
     * and the persisted {@code owner} was never set by this caller.
     */
    @Test(expected = RetryExhaustedException.class)
    public void testFindFirstAndEditThrowsRetryExhaustedUnderPersistentContention() {
        Task task = new Task(1);
        runway.save(task);
        long id = task.id();
        runway.findFirstAndEdit(Task.class, unclaimed(),
                Order.by("rank").ascending(), t -> {
                    // Force a conflicting external write on every attempt so
                    // the staged transaction can never commit. The nested
                    // request borrows a second connection while the edit holds
                    // its own; this is safe because Runway uses an expandable
                    // cached pool that grows on demand rather than blocking.
                    Concourse other = runway.connections.request();
                    try {
                        other.set("rank", t.rank + 1, id);
                    }
                    finally {
                        runway.connections.release(other);
                    }
                    t.owner = "worker";
                });
    }

    /**
     * Return a {@link Criteria} matching every unclaimed {@link Task}, i.e. one
     * whose {@code claimed} flag is {@code false}.
     *
     * @return the {@code claimed == false} {@link Criteria}
     */
    private static Criteria unclaimed() {
        return Criteria.where().key("claimed").operator(Operator.EQUALS)
                .value(false).build();
    }

    /**
     * A claimable unit of work with an orderable {@code rank}, a
     * {@code claimed} flag, and an {@code owner} that records the claimant.
     *
     * @author Javier Lores
     */
    public static class Task extends Record {

        /**
         * The orderable rank; lowest is claimed first under ascending order.
         */
        int rank;

        /**
         * Whether this task has been claimed.
         */
        boolean claimed;

        /**
         * The id of the claimant, or {@code null} when unclaimed.
         */
        String owner;

        /**
         * Construct a new, unclaimed instance.
         *
         * @param rank the orderable rank
         */
        public Task(int rank) {
            this.rank = rank;
            this.claimed = false;
        }
    }

}
