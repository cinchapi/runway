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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * {@link Runway#findFirstAndUpdate(Class, Criteria, Order, String, java.util.function.UnaryOperator)
 * findFirstAndUpdate} and
 * {@link Runway#findAnyFirstAndUpdate(Class, Criteria, Order, String, java.util.function.UnaryOperator)
 * findAnyFirstAndUpdate}, the atomic claim primitives. Each test runs under
 * both Command-API modes (bulk enabled and disabled), so the matrix drives the
 * transactional find through both of its read paths.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class FindFirstAndUpdateTest extends RunwayBaseClientServerTest {

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

    /**
     * Whether the test run exercises the bulk Command-API read path.
     */
    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to exercise the bulk Command-API read
     *            path; {@code false} for the incremental path
     */
    public FindFirstAndUpdateTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} updates
     * exactly the record that sorts first under the {@link Order} and durably
     * persists the update.
     * <p>
     * <strong>Start state:</strong> Three unclaimed {@link Task Tasks} with
     * ranks 3, 2, and 1 saved in non-sorted insertion order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Task Tasks} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirstAndUpdate} ordered by {@code rank} ascending
     * with an operator on {@code claimed} that returns {@code true}.</li>
     * <li>Re-load every {@link Task} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Task} has rank 1 and
     * {@code claimed == true}, the re-loaded rank-1 {@link Task} is claimed,
     * and the other {@link Task Tasks} are still unclaimed.
     */
    @Test
    public void testFindFirstAndUpdateUpdatesFirstUnderOrderAndPersists() {
        Task three = new Task(3);
        Task two = new Task(2);
        Task one = new Task(1);
        runway.save(three, two, one);
        Task first = runway.findFirstAndUpdate(Task.class, unclaimed(),
                Order.by("rank").ascending(), "claimed", claimed -> true);
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
        Assert.assertTrue(first.claimed);
        Assert.assertTrue(runway.load(Task.class, one.id()).claimed);
        Assert.assertFalse(runway.load(Task.class, two.id()).claimed);
        Assert.assertFalse(runway.load(Task.class, three.id()).claimed);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} applies the
     * {@link Order} client-side and still updates exactly the record that sorts
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
     * <li>Call {@code findFirstAndUpdate} ordered by {@code rank} ascending
     * with an operator on {@code claimed} that returns {@code true}.</li>
     * <li>Re-load the returned {@link Task} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Task} has rank 1 and the
     * re-loaded {@link Task} is claimed.
     */
    @Test
    public void testFindFirstAndUpdateAppliesOrderClientSideOnLegacyServer() {
        Reflection.set("hasNativeSortingAndPagination", false, runway); // (authorized)
        runway.save(new Task(3), new Task(2), new Task(1));
        Task first = runway.findFirstAndUpdate(Task.class, unclaimed(),
                Order.by("rank").ascending(), "claimed", claimed -> true);
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
        Assert.assertTrue(runway.load(Task.class, first.id()).claimed);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} returns
     * {@code null} and never invokes the operator when no record matches.
     * <p>
     * <strong>Start state:</strong> Three {@link Task Tasks} whose ranks are
     * all below the criteria threshold.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Task Tasks} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findFirstAndUpdate} with {@code rank > 100} and an
     * operator that flips an {@link AtomicBoolean}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} and the operator
     * never ran.
     */
    @Test
    public void testFindFirstAndUpdateReturnsNullAndSkipsOperatorWhenNoMatch() {
        runway.save(new Task(1), new Task(2), new Task(3));
        AtomicBoolean operatorRan = new AtomicBoolean(false);
        Task first = runway.findFirstAndUpdate(Task.class,
                Criteria.where().key("rank").operator(Operator.GREATER_THAN)
                        .value(100).build(),
                Order.by("rank").ascending(), "claimed", claimed -> {
                    operatorRan.set(true);
                    return claimed;
                });
        Assert.assertNull(first);
        Assert.assertFalse(operatorRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} returns the
     * first matching record unchanged when the operator returns the current
     * value, so a no-op update still reports which record matched without
     * writing anything.
     * <p>
     * <strong>Start state:</strong> Three unclaimed {@link Task Tasks} with
     * ranks 3, 2, and 1 saved in non-sorted insertion order.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Task Tasks} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirstAndUpdate} ordered by {@code rank} ascending
     * with an operator on {@code claimed} that captures its input and returns
     * it unchanged.</li>
     * <li>Re-load every {@link Task} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Task} has rank 1 and is
     * still unclaimed, the operator received the stored {@code false}, and
     * every re-loaded {@link Task} is still unclaimed.
     */
    @Test
    public void testFindFirstAndUpdateReturnsFirstMatchWhenOperatorIsNoOp() {
        Task three = new Task(3);
        Task two = new Task(2);
        Task one = new Task(1);
        runway.save(three, two, one);
        AtomicReference<Boolean> observed = new AtomicReference<>();
        Task first = runway.findFirstAndUpdate(Task.class, unclaimed(),
                Order.by("rank").ascending(), "claimed", (Boolean claimed) -> {
                    observed.set(claimed);
                    return claimed;
                });
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
        Assert.assertFalse(first.claimed);
        Assert.assertFalse(observed.get());
        Assert.assertFalse(runway.load(Task.class, one.id()).claimed);
        Assert.assertFalse(runway.load(Task.class, two.id()).claimed);
        Assert.assertFalse(runway.load(Task.class, three.id()).claimed);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} claims a
     * record whose target field has no value, passing {@code null} to the
     * operator, so an unset lock field can be claimed atomically.
     * <p>
     * <strong>Start state:</strong> Three unclaimed {@link Task Tasks} with
     * ranks 3, 2, and 1 whose {@code assignee} fields are all unset.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Task Tasks} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirstAndUpdate} ordered by {@code rank} ascending
     * with an operator on {@code assignee} that captures its input and returns
     * {@code "worker"}.</li>
     * <li>Re-load every {@link Task} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Task} has rank 1 and
     * {@code assignee == "worker"}, the operator received {@code null}, the
     * re-loaded rank-1 {@link Task} persists the claim, and the other
     * {@link Task Tasks} still have no {@code assignee}.
     */
    @Test
    public void testFindFirstAndUpdateClaimsFieldWithNoValue() {
        Task three = new Task(3);
        Task two = new Task(2);
        Task one = new Task(1);
        runway.save(three, two, one);
        AtomicReference<String> observed = new AtomicReference<>("unset");
        Task first = runway.findFirstAndUpdate(Task.class, unclaimed(),
                Order.by("rank").ascending(), "assignee", (String assignee) -> {
                    observed.set(assignee);
                    return "worker";
                });
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
        Assert.assertEquals("worker", first.assignee);
        Assert.assertNull(observed.get());
        Assert.assertEquals("worker",
                runway.load(Task.class, one.id()).assignee);
        Assert.assertNull(runway.load(Task.class, two.id()).assignee);
        Assert.assertNull(runway.load(Task.class, three.id()).assignee);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} rejects a
     * {@code null} {@link Order}, since "first" is undefined without one.
     * <p>
     * <strong>Start state:</strong> A single unclaimed {@link Task}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save one {@link Task}.</li>
     * <li>Call {@code findFirstAndUpdate} with a {@code null}
     * {@link Order}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link NullPointerException} is thrown.
     */
    @Test(expected = NullPointerException.class)
    public void testFindFirstAndUpdateRequiresOrder() {
        runway.save(new Task(1));
        runway.findFirstAndUpdate(Task.class, unclaimed(), null, "claimed",
                claimed -> true);
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
     * {@code findFirstAndUpdate} with an operator on {@code claimed} that
     * returns {@code true}, capturing any {@link Throwable} a worker
     * throws.</li>
     * <li>{@code join()} both threads, assert both finished without a failure,
     * then re-load the {@link Task}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Neither worker hangs or fails, exactly one
     * thread receives a non-null claim and the other receives {@code null}, and
     * the re-loaded {@link Task} is claimed.
     */
    @Test
    public void testFindFirstAndUpdateClaimsExactlyOneUnderConcurrency()
            throws InterruptedException {
        Task task = new Task(1);
        runway.save(task);
        long id = task.id();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Task> claim1 = new AtomicReference<>();
        AtomicReference<Task> claim2 = new AtomicReference<>();
        AtomicReference<Throwable> failure1 = new AtomicReference<>();
        AtomicReference<Throwable> failure2 = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                claim1.set(runway.findFirstAndUpdate(Task.class, unclaimed(),
                        Order.by("rank").ascending(), "claimed",
                        claimed -> true));
            }
            catch (Throwable t) {
                failure1.set(t);
            }
        });
        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                claim2.set(runway.findFirstAndUpdate(Task.class, unclaimed(),
                        Order.by("rank").ascending(), "claimed",
                        claimed -> true));
            }
            catch (Throwable t) {
                failure2.set(t);
            }
        });
        t1.start();
        t2.start();
        Assert.assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        t1.join(10000);
        t2.join(10000);
        Assert.assertFalse("Worker 1 is still running", t1.isAlive());
        Assert.assertFalse("Worker 2 is still running", t2.isAlive());
        Assert.assertNull(failure1.get());
        Assert.assertNull(failure2.get());
        boolean oneWon = claim1.get() != null;
        boolean twoWon = claim2.get() != null;
        Assert.assertTrue("Exactly one thread must claim the task",
                oneWon ^ twoWon);
        Assert.assertTrue(runway.load(Task.class, id).claimed);
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
     * unclaimed {@link Task}.</li>
     * <li>{@code join()} both threads.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both threads receive a non-null claim and the
     * two claims are distinct {@link Task Tasks} (different ids).
     */
    @Test
    public void testFindFirstAndUpdateConcurrentClaimsTakeDistinctRecords()
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
                claim1.set(runway.findFirstAndUpdate(Task.class, unclaimed(),
                        Order.by("rank").ascending(), "claimed",
                        claimed -> true));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                claim2.set(runway.findFirstAndUpdate(Task.class, unclaimed(),
                        Order.by("rank").ascending(), "claimed",
                        claimed -> true));
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
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} throws
     * {@link RetryExhaustedException} &mdash; rather than returning
     * {@code null} &mdash; when it loses the commit race on every attempt.
     * <p>
     * <strong>Start state:</strong> One unclaimed {@link Task}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a single unclaimed {@link Task}.</li>
     * <li>Call {@code findFirstAndUpdate} with an operator that, on every
     * invocation, writes to the same record through a separate connection so
     * the staged transaction always conflicts at commit time.</li>
     * <li>Catch the expected exception, then re-load the {@link Task} by id
     * from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RetryExhaustedException} is thrown
     * and the re-loaded {@link Task} is still unclaimed.
     */
    @Test
    public void testFindFirstAndUpdateThrowsRetryExhaustedUnderContention() {
        Task task = new Task(1);
        runway.save(task);
        long id = task.id();
        AtomicInteger clock = new AtomicInteger(1000);
        boolean threw = false;
        try {
            runway.findFirstAndUpdate(Task.class, unclaimed(),
                    Order.by("rank").ascending(), "claimed", claimed -> {
                        // Force a conflicting external write on every attempt
                        // so the staged transaction can never commit. The
                        // nested request borrows a second connection while the
                        // update holds its own; this is safe because Runway
                        // uses an expandable cached pool that grows on demand
                        // rather than blocking.
                        Concourse other = runway.connections.request();
                        try {
                            other.set("rank", clock.incrementAndGet(), id);
                        }
                        finally {
                            runway.connections.release(other);
                        }
                        return true;
                    });
        }
        catch (RetryExhaustedException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertFalse(runway.load(Task.class, id).claimed);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} supports a
     * {@link Criteria} over derived data that the database cannot resolve,
     * updating exactly the matching record that sorts first under the
     * {@link Order}.
     * <p>
     * <strong>Start state:</strong> Three unclaimed {@link Task Tasks} with
     * ranks 3, 2, and 1 saved in non-sorted insertion order, of which the two
     * odd-ranked ones match the derived {@code parity} criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Task Tasks} with ranks 3, 2, and 1.</li>
     * <li>Call {@code findFirstAndUpdate} with {@code parity == "odd"} ordered
     * by {@code rank} ascending and an operator on {@code claimed} that returns
     * {@code true}.</li>
     * <li>Re-load every {@link Task} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Task} has rank 1 and is
     * persisted as claimed, and neither other {@link Task} (including the
     * odd-ranked rank-3 match) is claimed.
     */
    @Test
    public void testFindFirstAndUpdateUpdatesFirstDerivedCriteriaMatch() {
        Task three = new Task(3);
        Task two = new Task(2);
        Task one = new Task(1);
        runway.save(three, two, one);
        Task first = runway.findFirstAndUpdate(Task.class, parity("odd"),
                Order.by("rank").ascending(), "claimed", claimed -> true);
        Assert.assertNotNull(first);
        Assert.assertEquals(1, first.rank);
        Assert.assertTrue(runway.load(Task.class, one.id()).claimed);
        Assert.assertFalse(runway.load(Task.class, two.id()).claimed);
        Assert.assertFalse(runway.load(Task.class, three.id()).claimed);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyFirstAndUpdate} matches
     * across the {@link Task} hierarchy and updates the record that sorts
     * first, even when it is a subclass instance.
     * <p>
     * <strong>Start state:</strong> One unclaimed {@link Task} with rank 3 and
     * one unclaimed {@link SpecialTask} with rank 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Task} with rank 3 and a {@link SpecialTask} with rank
     * 1.</li>
     * <li>Call {@code findAnyFirstAndUpdate} ordered by {@code rank} ascending
     * with an operator on {@code claimed} that returns {@code true}.</li>
     * <li>Re-load both records by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned record is the rank-1
     * {@link SpecialTask}, it is persisted as claimed, and the {@link Task} is
     * still unclaimed.
     */
    @Test
    public void testFindAnyFirstAndUpdateClaimsFirstAcrossHierarchy() {
        Task task = new Task(3);
        SpecialTask special = new SpecialTask(1);
        runway.save(task, special);
        Task first = runway.findAnyFirstAndUpdate(Task.class, unclaimed(),
                Order.by("rank").ascending(), "claimed", claimed -> true);
        Assert.assertNotNull(first);
        Assert.assertEquals(special.id(), first.id());
        Assert.assertTrue(runway.load(SpecialTask.class, special.id()).claimed);
        Assert.assertFalse(runway.load(Task.class, task.id()).claimed);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} rejects a
     * key that does not name an intrinsic field, without updating anything.
     * <p>
     * <strong>Start state:</strong> One unclaimed {@link Task}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save one {@link Task}.</li>
     * <li>Call {@code findFirstAndUpdate} on the derived {@code parity}
     * key.</li>
     * <li>Catch the expected exception, then re-load the {@link Task}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the re-loaded {@link Task} is still unclaimed.
     */
    @Test
    public void testFindFirstAndUpdateRejectsIneligibleKey() {
        Task task = new Task(1);
        runway.save(task);
        boolean threw = false;
        try {
            runway.findFirstAndUpdate(Task.class, unclaimed(),
                    Order.by("rank").ascending(), "parity", value -> "even");
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertFalse(runway.load(Task.class, task.id()).claimed);
    }

    /**
     * Return a {@link Criteria} matching every {@link Task} whose derived
     * {@code parity} equals the given {@code value}.
     *
     * @param value the parity to match; {@code "odd"} or {@code "even"}
     * @return the {@code parity == value} {@link Criteria}
     */
    private static Criteria parity(String value) {
        return Criteria.where().key("parity").operator(Operator.EQUALS)
                .value(value).build();
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
     * A claimable unit of work with an orderable {@code rank} and a
     * {@code claimed} flag.
     *
     * @author Jeff Nelson
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
         * The claim holder; unset until a worker claims this task.
         */
        String assignee;

        /**
         * Construct a new, unclaimed instance.
         *
         * @param rank the orderable rank
         */
        public Task(int rank) {
            this.rank = rank;
            this.claimed = false;
        }

        @Override
        protected Map<String, Object> derived() {
            Map<String, Object> derived = new HashMap<>();
            derived.put("parity", rank % 2 == 0 ? "even" : "odd");
            return derived;
        }
    }

    /**
     * A {@link Task} subclass used to verify that the {@code Any} variant
     * matches across the class hierarchy.
     *
     * @author Jeff Nelson
     */
    public static class SpecialTask extends Task {

        /**
         * Construct a new instance.
         *
         * @param rank the orderable rank
         */
        public SpecialTask(int rank) {
            super(rank);
        }
    }

}
