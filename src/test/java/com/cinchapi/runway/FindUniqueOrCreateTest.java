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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests for
 * {@link Runway#findUniqueOrCreate(Class, Criteria, java.util.function.Supplier)
 * findUniqueOrCreate} and
 * {@link Runway#findAnyUniqueOrCreate(Class, Criteria, java.util.function.Supplier)
 * findAnyUniqueOrCreate}, along with their {@link TransactionInterface}
 * counterparts. Each test runs under both Command-API modes (bulk enabled and
 * disabled), so the matrix drives the transactional find through both of its
 * read paths.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class FindUniqueOrCreateTest extends RunwayBaseClientServerTest {

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
    public FindUniqueOrCreateTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueOrCreate} creates,
     * saves and returns the record from the factory when no record matches.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 2} and a factory
     * that returns a new {@link Item} with code 2 and
     * {@code owner == "creator"}.</li>
     * <li>Re-load the returned {@link Item} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Item} has code 2 and
     * {@code owner == "creator"}, and the re-loaded {@link Item} shows the same
     * persisted state.
     */
    @Test
    public void testFindUniqueOrCreateCreatesAndPersistsWhenNoMatch() {
        Item item = runway.findUniqueOrCreate(Item.class, code(2), () -> {
            Item created = new Item(2);
            created.owner = "creator";
            return created;
        });
        Assert.assertNotNull(item);
        Assert.assertEquals(2, item.code);
        Assert.assertEquals("creator", item.owner);
        Item loaded = runway.load(Item.class, item.id());
        Assert.assertEquals(2, loaded.code);
        Assert.assertEquals("creator", loaded.owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueOrCreate} returns the
     * sole matching record and never invokes the factory when a match exists.
     * <p>
     * <strong>Start state:</strong> Three {@link Item Items} with distinct
     * codes, exactly one of which matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Item Items} with codes 1, 2, and 3.</li>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 2} and a factory
     * that flips an {@link AtomicBoolean}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Item} is the saved match,
     * the factory never ran, and no additional record exists.
     */
    @Test
    public void testFindUniqueOrCreateReturnsMatchAndSkipsFactory() {
        Item existing = new Item(2);
        runway.save(new Item(1), existing, new Item(3));
        AtomicBoolean factoryRan = new AtomicBoolean(false);
        Item item = runway.findUniqueOrCreate(Item.class, code(2), () -> {
            factoryRan.set(true);
            return new Item(2);
        });
        Assert.assertNotNull(item);
        Assert.assertEquals(existing.id(), item.id());
        Assert.assertFalse(factoryRan.get());
        Assert.assertEquals(1, runway.find(Item.class, code(2)).size());
    }

    /**
     * <strong>Goal:</strong> Verify the convergence guarantee: two threads
     * racing to get-or-create the same record both receive the same record, and
     * only one record exists afterwards.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start two threads that, gated by a common latch, each call
     * {@code findUniqueOrCreate} with {@code code == 2}, capturing any
     * {@link Throwable} a worker throws.</li>
     * <li>Share a factory that waits until both threads have observed no match
     * before it returns a new {@link Item} with code 2, so the create race is
     * guaranteed rather than schedule-dependent.</li>
     * <li>{@code join()} both threads, then query for every {@link Item} with
     * code 2.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Neither worker hangs or fails, both threads
     * receive a non-null {@link Item} with the same id, and exactly one record
     * matches the criteria.
     */
    @Test
    public void testFindUniqueOrCreateConvergesConcurrentCallersOnOneRecord()
            throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Item> result1 = new AtomicReference<>();
        AtomicReference<Item> result2 = new AtomicReference<>();
        AtomicReference<Throwable> failure1 = new AtomicReference<>();
        AtomicReference<Throwable> failure2 = new AtomicReference<>();
        CountDownLatch bothFoundNoMatch = new CountDownLatch(2);
        Supplier<Item> factory = () -> {
            bothFoundNoMatch.countDown();
            try {
                Assert.assertTrue(
                        "Both workers should observe no match before either"
                                + " creates",
                        bothFoundNoMatch.await(5, TimeUnit.SECONDS));
            }
            catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            return new Item(2);
        };
        Thread t1 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                result1.set(runway.findUniqueOrCreate(Item.class, code(2),
                        factory));
            }
            catch (Throwable t) {
                failure1.set(t);
            }
        });
        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                result2.set(runway.findUniqueOrCreate(Item.class, code(2),
                        factory));
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
        Assert.assertNotNull(result1.get());
        Assert.assertNotNull(result2.get());
        Assert.assertEquals(result1.get().id(), result2.get().id());
        Assert.assertEquals(1, runway.find(Item.class, code(2)).size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueOrCreate} throws
     * {@link DuplicateEntryException} when more than one record matches, and
     * that the factory never runs and nothing is created.
     * <p>
     * <strong>Start state:</strong> Two {@link Item Items} that share the same
     * code.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Item Items} both with code 7.</li>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 7} and a factory
     * that flips an {@link AtomicBoolean}.</li>
     * <li>Catch the expected exception, then query for every {@link Item} with
     * code 7.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown,
     * the factory never ran, and exactly the two original records match.
     */
    @Test
    public void testFindUniqueOrCreateThrowsOnDuplicateWithoutCreating() {
        runway.save(new Item(7), new Item(7));
        AtomicBoolean factoryRan = new AtomicBoolean(false);
        boolean threw = false;
        try {
            runway.findUniqueOrCreate(Item.class, code(7), () -> {
                factoryRan.set(true);
                return new Item(7);
            });
        }
        catch (DuplicateEntryException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertFalse(factoryRan.get());
        Assert.assertEquals(2, runway.find(Item.class, code(7)).size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueOrCreate} rejects a
     * factory result that does not match the criteria, without persisting
     * anything.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 2} and a factory
     * that returns a new {@link Item} with code 99.</li>
     * <li>Catch the expected exception, then load every {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and no {@link Item} exists in the database.
     */
    @Test
    public void testFindUniqueOrCreateRejectsFactoryResultThatDoesNotMatch() {
        boolean threw = false;
        try {
            runway.findUniqueOrCreate(Item.class, code(2), () -> new Item(99));
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.load(Item.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueOrCreate} rejects a
     * factory that returns {@code null}, without persisting anything.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 2} and a factory
     * that returns {@code null}.</li>
     * <li>Catch the expected exception, then load every {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and no {@link Item} exists in the database.
     */
    @Test
    public void testFindUniqueOrCreateRejectsNullFactoryResult() {
        boolean threw = false;
        try {
            runway.findUniqueOrCreate(Item.class, code(2), () -> null);
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.load(Item.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyUniqueOrCreate} returns
     * an existing subclass instance that matches through the parent class,
     * without invoking the factory.
     * <p>
     * <strong>Start state:</strong> One {@link Item} with code 1 and one
     * {@link SpecialItem} with code 2.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Item} with code 1 and a {@link SpecialItem} with code
     * 2.</li>
     * <li>Call {@code findAnyUniqueOrCreate} on {@link Item} with
     * {@code code == 2} and a factory that flips an {@link AtomicBoolean}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned record is the saved
     * {@link SpecialItem} and the factory never ran.
     */
    @Test
    public void testFindAnyUniqueOrCreateReturnsSubclassMatchThroughParent() {
        SpecialItem special = new SpecialItem(2);
        runway.save(new Item(1), special);
        AtomicBoolean factoryRan = new AtomicBoolean(false);
        Item item = runway.findAnyUniqueOrCreate(Item.class, code(2), () -> {
            factoryRan.set(true);
            return new Item(2);
        });
        Assert.assertNotNull(item);
        Assert.assertEquals(special.id(), item.id());
        Assert.assertFalse(factoryRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyUniqueOrCreate} creates,
     * saves and returns the record from the factory when nothing in the class
     * hierarchy matches.
     * <p>
     * <strong>Start state:</strong> One {@link SpecialItem} whose code does not
     * match the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link SpecialItem} with code 1.</li>
     * <li>Call {@code findAnyUniqueOrCreate} on {@link Item} with
     * {@code code == 5} and a factory that returns a new {@link Item} with code
     * 5.</li>
     * <li>Re-load the returned {@link Item} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Item} has code 5 and the
     * re-loaded {@link Item} persists it.
     */
    @Test
    public void testFindAnyUniqueOrCreateCreatesWhenNoMatchInHierarchy() {
        runway.save(new SpecialItem(1));
        Item item = runway.findAnyUniqueOrCreate(Item.class, code(5),
                () -> new Item(5));
        Assert.assertNotNull(item);
        Assert.assertEquals(5, item.code);
        Assert.assertEquals(5, runway.load(Item.class, item.id()).code);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyUniqueOrCreate} throws
     * {@link DuplicateEntryException} when the match set spans the class
     * hierarchy, and that the factory never runs and nothing is created.
     * <p>
     * <strong>Start state:</strong> One {@link Item} and one
     * {@link SpecialItem} that share the same code.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Item} and a {@link SpecialItem} both with code 7.</li>
     * <li>Call {@code findAnyUniqueOrCreate} with {@code code == 7} and a
     * factory that flips an {@link AtomicBoolean}.</li>
     * <li>Catch the expected exception, then query for every {@link Item} in
     * the hierarchy with code 7.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown,
     * the factory never ran, and exactly the two original records match.
     */
    @Test
    public void testFindAnyUniqueOrCreateThrowsOnDuplicateAcrossHierarchy() {
        runway.save(new Item(7), new SpecialItem(7));
        AtomicBoolean factoryRan = new AtomicBoolean(false);
        boolean threw = false;
        try {
            runway.findAnyUniqueOrCreate(Item.class, code(7), () -> {
                factoryRan.set(true);
                return new Item(7);
            });
        }
        catch (DuplicateEntryException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertFalse(factoryRan.get());
        Assert.assertEquals(2, runway.findAny(Item.class, code(7)).size());
    }

    /**
     * <strong>Goal:</strong> Verify that, within a caller-owned
     * {@link Transaction}, {@code findUniqueOrCreate} stages the created record
     * so it is invisible outside the transaction until the commit and visible
     * after it.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
     * <li>Call {@code findUniqueOrCreate} on the transaction with
     * {@code code == 2} and a factory that returns a new {@link Item} with code
     * 2.</li>
     * <li>Query for the record through the enclosing {@link Runway} before the
     * commit, then {@code commit()} and query again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The pre-commit query observes no match, the
     * commit succeeds, and the post-commit query returns the created
     * {@link Item}.
     */
    @Test
    public void testFindUniqueOrCreateStagesCreateWithinOpenTransaction() {
        long id;
        try (Transaction transaction = runway.stage()) {
            Item created = transaction.findUniqueOrCreate(Item.class, code(2),
                    () -> new Item(2));
            Assert.assertNotNull(created);
            Assert.assertNull(runway.findUnique(Item.class, code(2)));
            Assert.assertTrue(transaction.commit());
            id = created.id();
        }
        Item visible = runway.findUnique(Item.class, code(2));
        Assert.assertNotNull(visible);
        Assert.assertEquals(id, visible.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a repeat {@code findUniqueOrCreate}
     * within the same {@link Transaction} observes the staged create instead of
     * creating again, and that an abort discards the staged record.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
     * <li>Call {@code findUniqueOrCreate} twice with {@code code == 2} and a
     * factory that counts its runs.</li>
     * <li>Leave the block without a commit, then load every {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same record id, the
     * factory ran exactly once, and no {@link Item} exists after the abort.
     */
    @Test
    public void testFindUniqueOrCreateObservesStagedCreateWithinTransaction() {
        AtomicInteger factoryRuns = new AtomicInteger(0);
        try (Transaction transaction = runway.stage()) {
            Item first = transaction.findUniqueOrCreate(Item.class, code(2),
                    () -> {
                        factoryRuns.incrementAndGet();
                        return new Item(2);
                    });
            Item second = transaction.findUniqueOrCreate(Item.class, code(2),
                    () -> {
                        factoryRuns.incrementAndGet();
                        return new Item(2);
                    });
            Assert.assertEquals(first.id(), second.id());
            Assert.assertEquals(1, factoryRuns.get());
        }
        Assert.assertTrue(runway.load(Item.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a factory result that does not match
     * the criteria poisons a caller-owned {@link Transaction}, so the staged
     * save can never commit.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 2} and a factory
     * that returns a new {@link Item} with code 99.</li>
     * <li>Catch the expected rejection, then attempt to {@code commit()}.</li>
     * <li>Leave the block, then load every {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The rejection is an
     * {@link IllegalArgumentException}, the commit attempt is refused with an
     * {@link IllegalStateException}, and no {@link Item} exists after the
     * abort.
     */
    @Test
    public void testFindUniqueOrCreateMismatchPoisonsTransaction() {
        try (Transaction transaction = runway.stage()) {
            boolean threw = false;
            try {
                transaction.findUniqueOrCreate(Item.class, code(2),
                        () -> new Item(99));
            }
            catch (IllegalArgumentException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            boolean refused = false;
            try {
                transaction.commit();
            }
            catch (IllegalStateException e) {
                refused = true;
            }
            Assert.assertTrue(refused);
        }
        Assert.assertTrue(runway.load(Item.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a factory cannot {@code abort()} the
     * {@link Transaction} that the operation is in flight on, so the create can
     * never fall through to the database as an immediate durable write.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 2} and a factory
     * that calls {@code abort()} on the transaction before it returns a new
     * {@link Item} with code 2.</li>
     * <li>Catch the expected refusal, then {@code commit()} the transaction and
     * load every {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The refusal is an
     * {@link IllegalStateException}, the transaction remains usable and
     * commits, and no {@link Item} exists in the database.
     */
    @Test
    public void testFindUniqueOrCreateRefusesFactoryThatAbortsTransaction() {
        try (Transaction transaction = runway.stage()) {
            boolean refused = false;
            try {
                transaction.findUniqueOrCreate(Item.class, code(2), () -> {
                    transaction.abort();
                    return new Item(2);
                });
            }
            catch (IllegalStateException e) {
                refused = true;
            }
            Assert.assertTrue(refused);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertTrue(runway.load(Item.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a factory cannot {@code commit()} the
     * {@link Transaction} that the operation is in flight on, so the create can
     * never fall through to the database as an immediate durable write.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
     * <li>Call {@code findUniqueOrCreate} with {@code code == 2} and a factory
     * that calls {@code commit()} on the transaction before it returns a new
     * {@link Item} with code 2.</li>
     * <li>Catch the expected refusal, then {@code commit()} the transaction and
     * load every {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The refusal is an
     * {@link IllegalStateException}, the transaction remains usable and
     * commits, and no {@link Item} exists in the database.
     */
    @Test
    public void testFindUniqueOrCreateRefusesFactoryThatCommitsTransaction() {
        try (Transaction transaction = runway.stage()) {
            boolean refused = false;
            try {
                transaction.findUniqueOrCreate(Item.class, code(2), () -> {
                    transaction.commit();
                    return new Item(2);
                });
            }
            catch (IllegalStateException e) {
                refused = true;
            }
            Assert.assertTrue(refused);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertTrue(runway.load(Item.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueOrCreate} resumes
     * against the enclosing {@link Runway} after the {@link Transaction} ends.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items} and a committed
     * {@link Transaction}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block and {@code commit()} it immediately.</li>
     * <li>Call {@code findUniqueOrCreate} on the ended transaction with
     * {@code code == 2} and a factory that returns a new {@link Item} with code
     * 2.</li>
     * <li>Query for the record through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The create persists directly through the
     * {@link Runway}: the query returns the created {@link Item} with the same
     * id.
     */
    @Test
    public void testFindUniqueOrCreateResumesAgainstRunwayAfterTransactionEnds() {
        try (Transaction transaction = runway.stage()) {
            Assert.assertTrue(transaction.commit());
            Item item = transaction.findUniqueOrCreate(Item.class, code(2),
                    () -> new Item(2));
            Assert.assertNotNull(item);
            Item visible = runway.findUnique(Item.class, code(2));
            Assert.assertNotNull(visible);
            Assert.assertEquals(item.id(), visible.id());
        }
    }

    /**
     * Return a {@link Criteria} matching every {@link Item} whose {@code code}
     * equals the given {@code value}.
     *
     * @param value the code to match
     * @return the {@code code == value} {@link Criteria}
     */
    private static Criteria code(int value) {
        return Criteria.where().key("code").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * A {@link Record} with a queryable {@code code} and a factory-populated
     * {@code owner}.
     *
     * @author Jeff Nelson
     */
    public static class Item extends Record {

        /**
         * The queryable code.
         */
        int code;

        /**
         * The owner; populated by the caller or its factory.
         */
        String owner;

        /**
         * Construct a new instance.
         *
         * @param code the queryable code
         */
        public Item(int code) {
            this.code = code;
        }
    }

    /**
     * An {@link Item} subclass used to verify that the {@code Any} variant
     * matches across the class hierarchy.
     *
     * @author Jeff Nelson
     */
    public static class SpecialItem extends Item {

        /**
         * Construct a new instance.
         *
         * @param code the queryable code
         */
        public SpecialItem(int code) {
            super(code);
        }
    }

}
