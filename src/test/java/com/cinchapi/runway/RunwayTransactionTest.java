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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.access.AccessControl;
import com.cinchapi.runway.access.Audience;

/**
 * Tests for {@link Runway#stage()},
 * {@link Runway#run(java.util.function.Consumer) run} and
 * {@link Runway#call(java.util.function.Function) call}: the
 * {@link Transaction} view that scopes reads and writes to a single ACID
 * transaction.
 *
 * @author Jeff Nelson
 */
public class RunwayTransactionTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a read through a {@link Transaction}
     * observes the transaction's own uncommitted writes while readers outside
     * the transaction do not.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and load the {@link Item} through
     * it.</li>
     * <li>Change the score to 2 and {@code save()} the record.</li>
     * <li>Load the {@link Item} again through the transaction and through the
     * enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The transactional load observes 2; the
     * non-transactional load still observes 1.
     */
    @Test
    public void testReadsWithinTransactionObserveUncommittedWrites() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Item inside = transaction.load(Item.class, item.id());
            Item outside = runway.load(Item.class, item.id());
            Assert.assertEquals(2, inside.score);
            Assert.assertEquals(1, outside.score);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Transaction#commit()} makes
     * every staged write durable and visible outside the transaction.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction}, load the {@link Item}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Call {@code commit()}.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code commit()} returns {@code true} and the
     * non-transactional load observes 2.
     */
    @Test
    public void testCommitMakesStagedWritesDurable() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Transaction#abort()} discards
     * every staged write while the caller's in-memory edits remain, the same as
     * after a failed save.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction}, load the {@link Item}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Call {@code abort()}.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The non-transactional load observes 1, and the
     * transactional copy keeps its in-memory edit of 2.
     */
    @Test
    public void testAbortDiscardsStagedWrites() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            transaction.abort();
            Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
            Assert.assertEquals(2, txItem.score);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Transaction#close() close}
     * without a prior {@code commit()} aborts the transaction.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>In a try-with-resources block, load the {@link Item}, set the score
     * to 2 and {@code save()}, then exit the block without a commit.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The non-transactional load observes 1.
     */
    @Test
    public void testCloseWithoutCommitAborts() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
        }
        Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#startTransaction()} is
     * an alias for {@link Runway#stage()} that starts an open
     * {@link Transaction}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code startTransaction()}, load the {@link Item}, set the score
     * to 2 and {@code save()}.</li>
     * <li>Call {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code commit()} returns {@code true} and the
     * stored score is 2.
     */
    @Test
    public void testStartTransactionIsAnAliasForStage() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.startTransaction()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Transaction} whose read data
     * changes concurrently fails instead of writing a decision that was made on
     * a stale snapshot.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and load the {@link Item} through it, so
     * the read joins the conflict footprint.</li>
     * <li>Write a score of 99 through a separate client connection.</li>
     * <li>Set the score to 50 through the transactional copy, {@code save()},
     * and attempt to {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save or the commit fails with a conflict,
     * and the stored score is 99.
     */
    @Test
    public void testCommitFailsWhenReadDataChangesConcurrently() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        Transaction transaction = runway.stage();
        boolean conflicted;
        try {
            Item txItem = transaction.load(Item.class, item.id());
            client.set("score", 99, item.id());
            txItem.score = 50;
            txItem.save();
            conflicted = !transaction.commit();
        }
        catch (TransactionException e) {
            conflicted = true;
        }
        finally {
            transaction.close();
        }
        Assert.assertTrue(conflicted);
        Assert.assertEquals(99, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a new {@link Record} saved through
     * {@link Transaction#save(Record...)} is invisible outside the transaction
     * until the commit.
     * <p>
     * <strong>Start state:</strong> No stored {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and save a new {@link Item} through
     * it.</li>
     * <li>Count {@link Item Items} outside the transaction, then commit and
     * count again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count is 0 before the commit and 1 after
     * it.
     */
    @Test
    public void testNewRecordSavedThroughTransactionIsInvisibleUntilCommit() {
        try (Transaction transaction = runway.stage()) {
            Item item = new Item("widget", 1);
            Assert.assertTrue(transaction.save(item));
            Assert.assertEquals(0, runway.count(Item.class));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(1, runway.count(Item.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} created with
     * {@link Transaction#create(Class, Object...)} is bound to the
     * {@link Transaction}, so a direct {@code save()} stages within it.
     * <p>
     * <strong>Start state:</strong> No stored {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and {@code create()} a new {@link Item}
     * through it.</li>
     * <li>Call {@code save()} on the {@link Item} directly.</li>
     * <li>Count {@link Item Items} inside and outside the transaction, then
     * commit and count again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Before the commit the inside count is 1 and
     * the outside count is 0; after the commit the outside count is 1.
     */
    @Test
    public void testCreatedRecordIsBoundToTheTransaction() {
        try (Transaction transaction = runway.stage()) {
            Item item = transaction.create(Item.class, "widget", 1);
            Assert.assertTrue(item.save());
            Assert.assertEquals(1, transaction.count(Item.class));
            Assert.assertEquals(0, runway.count(Item.class));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(1, runway.count(Item.class));
    }

    /**
     * <strong>Goal:</strong> Verify that single-key atomic operations are
     * refused on a {@link Record} that is bound to a {@link Transaction},
     * because the transaction's commit is the unit of atomicity.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}.</li>
     * <li>Call {@code getAndUpdate} and {@code exchange} on the transactional
     * copy.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls throw
     * {@link IllegalStateException}.
     */
    @Test
    public void testAtomicOperationsAreRefusedOnTransactionBoundRecords() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            try {
                txItem.getAndUpdate("score", (Integer score) -> score + 1);
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                txItem.exchange("score", 100);
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {/* expected */}
        }
    }

    /**
     * <strong>Goal:</strong> Verify that reads and saves fall through to the
     * enclosing {@link Runway} after a {@link Transaction} ends.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} and a committed
     * {@link Transaction} that loaded it and staged a score of 2.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Commit the {@link Transaction}.</li>
     * <li>Load the {@link Item} through the ended transaction.</li>
     * <li>Set the score to 5 on the transactional copy and {@code save()}
     * it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The post-commit load observes the committed
     * score of 2, and the post-commit save persists 5 directly to the database.
     */
    @Test
    public void testOperationsAfterTransactionEndsFallThroughToTheDatabase() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Assert.assertTrue(transaction.commit());
            Assert.assertEquals(2,
                    transaction.load(Item.class, item.id()).score);
            txItem.score = 5;
            Assert.assertTrue(txItem.save());
            Assert.assertEquals(5, runway.load(Item.class, item.id()).score);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that save notifications for records saved
     * within a {@link Transaction} fire only after the commit succeeds.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} and a save listener
     * that counts notifications for {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, change it and
     * {@code save()}.</li>
     * <li>Record the notification count, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count is 0 before the commit and rises to
     * 1 after it.
     */
    @Test
    public void testSaveNotificationsFireOnlyAfterCommit()
            throws InterruptedException {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger notified = new AtomicInteger(0);
        runway.properties().onSave(Item.class,
                record -> notified.incrementAndGet());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Assert.assertEquals(0, notified.get());
            Assert.assertTrue(transaction.commit());
        }
        long stop = System.currentTimeMillis() + 5000;
        while (notified.get() == 0 && System.currentTimeMillis() < stop) {
            Thread.sleep(10);
        }
        Assert.assertEquals(1, notified.get());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#run(java.util.function.Consumer) run} commits the
     * transaction after the work completes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.run(work)} where the work loads the {@link Item}
     * through the provided {@link Transaction}, sets the score to 2 and
     * saves.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored score is 2.
     */
    @Test
    public void testRunCommitsWorkAtomically() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        runway.run(transaction -> {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
        });
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#call(java.util.function.Function) call} returns the result
     * of the work after the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.call(work)} where the work loads the {@link Item},
     * sets the score to 2, saves and returns the new score.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns 2 and the stored score is 2.
     */
    @Test
    public void testCallReturnsTheWorkResult() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        int score = runway.call(transaction -> {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            return txItem.score;
        });
        Assert.assertEquals(2, score);
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} created with
     * {@link Transaction#create(Class, Object...)} during
     * {@link Runway#run(java.util.function.Consumer) run} joins the
     * transaction.
     * <p>
     * <strong>Start state:</strong> No stored {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.run(work)} where the work creates a new
     * {@link Item} through the provided {@link Transaction} and calls
     * {@code save()} on it directly.</li>
     * <li>Count the {@link Item Items} through the transaction inside the work,
     * and through the enclosing {@link Runway} after the run.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both counts are 1, because the created record
     * is bound to the transaction and the commit made it durable.
     */
    @Test
    public void testRecordCreatedDuringRunJoinsTheTransaction() {
        runway.run(transaction -> {
            Item item = transaction.create(Item.class, "widget", 1);
            Assert.assertTrue(item.save());
            Assert.assertEquals(1, transaction.count(Item.class));
        });
        Assert.assertEquals(1, runway.count(Item.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} bound to the
     * enclosing {@link Runway} saves directly to the database during
     * {@link Runway#run(java.util.function.Consumer) run} instead of joining
     * the transaction.
     * <p>
     * <strong>Start state:</strong> No stored {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.run(work)} where the work saves a new {@link Item}
     * that is assigned to the {@link Runway}.</li>
     * <li>Count the {@link Item Items} through the enclosing {@link Runway}
     * inside the work, before the transaction commits.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count inside the work is already 1,
     * because the save wrote directly to the database instead of staging within
     * the transaction.
     */
    @Test
    public void testRunwayBoundRecordSavesOutsideTheRunTransaction() {
        runway.run(transaction -> {
            Item item = new Item("widget", 1);
            item.assign(runway);
            Assert.assertTrue(item.save());
            Assert.assertEquals(1, runway.count(Item.class));
        });
        Assert.assertEquals(1, runway.count(Item.class));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#run(java.util.function.Consumer) run} aborts the
     * transaction and propagates the exception when the work throws.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Run {@code runway.run(work)} where the work changes and saves the
     * {@link Item} and then throws.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exception propagates and the stored score
     * remains 1.
     */
    @Test
    public void testRunAbortsWhenWorkThrows() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try {
            runway.run(transaction -> {
                Item txItem = transaction.load(Item.class, item.id());
                txItem.score = 2;
                txItem.save();
                throw new RuntimeException("boom");
            });
            Assert.fail("Expected a RuntimeException");
        }
        catch (RuntimeException e) {
            Assert.assertEquals("boom", e.getMessage());
        }
        Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Transaction} is confined to
     * the thread that started it.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} and an open
     * {@link Transaction} started on the test thread.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Submit a load through the {@link Transaction} to a different
     * thread.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The other thread's load throws
     * {@link IllegalStateException}.
     */
    @Test
    public void testTransactionIsConfinedToOwnerThread() throws Exception {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Transaction transaction = runway.stage()) {
            Future<?> future = executor
                    .submit(() -> transaction.load(Item.class, item.id()));
            try {
                future.get();
                Assert.fail("Expected an IllegalStateException");
            }
            catch (ExecutionException e) {
                Assert.assertTrue(
                        e.getCause() instanceof IllegalStateException);
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a linked {@link Record} loaded within
     * a {@link Transaction} is bound to it, so a save through the link stays
     * invisible until the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Basket} that links to a
     * saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Basket} through a {@link Transaction} and navigate to
     * its {@link Item}.</li>
     * <li>Set the {@link Item Item's} score to 2 and {@code save()} it.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway} before and
     * after the commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The outside load observes 1 before the commit
     * and 2 after it.
     */
    @Test
    public void testLinkedRecordLoadedWithinTransactionSavesThroughIt() {
        Item item = new Item("widget", 1);
        Basket basket = new Basket("bin", item);
        basket.assign(runway);
        Assert.assertTrue(basket.save());
        try (Transaction transaction = runway.stage()) {
            Basket txBasket = transaction.load(Basket.class, basket.id());
            txBasket.item.score = 2;
            Assert.assertTrue(txBasket.item.save());
            Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Transaction} query resolved
     * with {@link Criteria} observes the transaction's own uncommitted writes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} named "widget" with a
     * score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Run {@code findUnique} through the transaction for a score of 2.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The transactional query matches the
     * {@link Item}.
     */
    @Test
    public void testCriteriaQueryWithinTransactionObservesStagedWrites() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Item found = transaction.findUnique(Item.class, Criteria.where()
                    .key("score").operator(Operator.EQUALS).value(2).build());
            Assert.assertNotNull(found);
            Assert.assertEquals(item.id(), found.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} loaded within a
     * {@link Transaction} routes its reads through the transaction and observes
     * staged state.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * a saved {@link Viewer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Load the {@link Viewer} through the transaction and load the
     * {@link Item} through that {@link Viewer}.</li>
     * <li>Load the {@link Viewer} outside the transaction and load the
     * {@link Item} through it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The transactional {@link Viewer} observes 2;
     * the outside {@link Viewer} observes 1.
     */
    @Test
    public void testAudienceLoadedWithinTransactionObservesStagedState() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        Viewer viewer = new Viewer("alice");
        viewer.assign(runway);
        Assert.assertTrue(viewer.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Viewer txViewer = transaction.load(Viewer.class, viewer.id());
            Assert.assertEquals(2, txViewer.load(Item.class, item.id()).score);
            Viewer outside = runway.load(Viewer.class, viewer.id());
            Assert.assertEquals(1, outside.load(Item.class, item.id()).score);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that access rules still apply when an
     * {@link Audience} operates within a {@link Transaction}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Viewer Viewers} and a
     * saved {@link Secret} that only the first {@link Viewer} can discover.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load both {@link Viewer Viewers} through a {@link Transaction}.</li>
     * <li>Load the {@link Secret} through each transactional
     * {@link Viewer}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The designated {@link Viewer} loads the
     * {@link Secret}; the other {@link Viewer} gets {@code null}.
     */
    @Test
    public void testAudienceAccessRulesApplyWithinTransaction() {
        Viewer alice = new Viewer("alice");
        alice.assign(runway);
        Assert.assertTrue(alice.save());
        Viewer bob = new Viewer("bob");
        bob.assign(runway);
        Assert.assertTrue(bob.save());
        Secret secret = new Secret("classified", alice);
        secret.assign(runway);
        Assert.assertTrue(secret.save());
        try (Transaction transaction = runway.stage()) {
            Viewer txAlice = transaction.load(Viewer.class, alice.id());
            Viewer txBob = transaction.load(Viewer.class, bob.id());
            Assert.assertNotNull(txAlice.load(Secret.class, secret.id()));
            Assert.assertNull(txBob.load(Secret.class, secret.id()));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that reads performed through an
     * {@link Audience} within a {@link Transaction} join the conflict
     * footprint.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * a saved {@link Viewer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Viewer} through a {@link Transaction} and load the
     * {@link Item} through that {@link Viewer}.</li>
     * <li>Write a score of 99 through a separate client connection.</li>
     * <li>Set the score to 50 through the audience-loaded copy, {@code save()},
     * and attempt to {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save or the commit fails with a conflict,
     * and the stored score is 99.
     */
    @Test
    public void testAudienceReadsWithinTransactionJoinConflictFootprint() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        Viewer viewer = new Viewer("alice");
        viewer.assign(runway);
        Assert.assertTrue(viewer.save());
        Transaction transaction = runway.stage();
        boolean conflicted;
        try {
            Viewer txViewer = transaction.load(Viewer.class, viewer.id());
            Item txItem = txViewer.load(Item.class, item.id());
            client.set("score", 99, item.id());
            txItem.score = 50;
            txItem.save();
            conflicted = !transaction.commit();
        }
        catch (TransactionException e) {
            conflicted = true;
        }
        finally {
            transaction.close();
        }
        Assert.assertTrue(conflicted);
        Assert.assertEquals(99, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} created by an
     * {@link Audience} that operates within a {@link Transaction} is bound to
     * the transaction.
     * <p>
     * <strong>Start state:</strong> A saved {@link Viewer} and no stored
     * {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Viewer} through a {@link Transaction}.</li>
     * <li>Create a new {@link Item} with {@code viewer.create()} and
     * {@code save()} it directly.</li>
     * <li>Count {@link Item Items} inside and outside the transaction, then
     * commit and count again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Before the commit the inside count is 1 and
     * the outside count is 0; after the commit the outside count is 1.
     */
    @Test
    public void testAudienceCreatedRecordJoinsTheAudienceTransaction() {
        Viewer viewer = new Viewer("alice");
        viewer.assign(runway);
        Assert.assertTrue(viewer.save());
        try (Transaction transaction = runway.stage()) {
            Viewer txViewer = transaction.load(Viewer.class, viewer.id());
            Item item = txViewer.create(Item.class, "widget", 1);
            Assert.assertTrue(item.save());
            Assert.assertEquals(1, transaction.count(Item.class));
            Assert.assertEquals(0, runway.count(Item.class));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(1, runway.count(Item.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link DeferredReference} first
     * accessed within a {@link Transaction} resolves within its snapshot.
     * <p>
     * <strong>Start state:</strong> A saved {@link Crate} with a lazy link to a
     * saved {@link Item} that has a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Load the {@link Crate} through the transaction and access its lazy
     * {@link Item}.</li>
     * <li>Load the {@link Crate} outside the transaction and access its lazy
     * {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The transactional access observes 2; the
     * outside access observes 1.
     */
    @Test
    public void testDeferredReferenceResolvesWithinTransaction() {
        Item item = new Item("widget", 1);
        Crate crate = new Crate("bin", item);
        crate.assign(runway);
        Assert.assertTrue(runway.save(crate, item));
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Crate txCrate = transaction.load(Crate.class, crate.id());
            Assert.assertEquals(2, txCrate.item.get().score);
            Crate outside = runway.load(Crate.class, crate.id());
            Assert.assertEquals(1, outside.item.get().score);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link DeferredReference} accessed
     * within a {@link Transaction} joins the conflict footprint and binds its
     * {@link Record} to the transaction.
     * <p>
     * <strong>Start state:</strong> A saved {@link Crate} with a lazy link to a
     * saved {@link Item} that has a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Crate} through a {@link Transaction} and access its
     * lazy {@link Item}.</li>
     * <li>Write a score of 99 through a separate client connection.</li>
     * <li>Set the score to 50 on the lazily loaded copy, {@code save()}, and
     * attempt to {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save or the commit fails with a conflict,
     * and the stored score is 99.
     */
    @Test
    public void testDeferredReferenceJoinsConflictFootprint() {
        Item item = new Item("widget", 1);
        Crate crate = new Crate("bin", item);
        crate.assign(runway);
        Assert.assertTrue(runway.save(crate, item));
        Transaction transaction = runway.stage();
        boolean conflicted;
        try {
            Crate txCrate = transaction.load(Crate.class, crate.id());
            Item txItem = txCrate.item.get();
            client.set("score", 99, item.id());
            txItem.score = 50;
            txItem.save();
            conflicted = !transaction.commit();
        }
        catch (TransactionException e) {
            conflicted = true;
        }
        finally {
            transaction.close();
        }
        Assert.assertTrue(conflicted);
        Assert.assertEquals(99, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link DeferredReference} first
     * accessed after its {@link Transaction} ends resolves through the
     * enclosing {@link Runway}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Crate} with a lazy link to a
     * saved {@link Item} that has a score of 1, and a {@link Transaction} that
     * loaded the {@link Crate} and committed without an access.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Crate} through a {@link Transaction} and commit
     * without an access of the lazy {@link Item}.</li>
     * <li>Access the lazy {@link Item}, set the score to 5 and
     * {@code save()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The access observes the committed score of 1,
     * and the save persists 5 directly to the database.
     */
    @Test
    public void testDeferredReferenceResolvesThroughRunwayAfterCommit() {
        Item item = new Item("widget", 1);
        Crate crate = new Crate("bin", item);
        crate.assign(runway);
        Assert.assertTrue(runway.save(crate, item));
        try (Transaction transaction = runway.stage()) {
            Crate txCrate = transaction.load(Crate.class, crate.id());
            Assert.assertTrue(transaction.commit());
            Item lazy = txCrate.item.get();
            Assert.assertEquals(1, lazy.score);
            lazy.score = 5;
            Assert.assertTrue(lazy.save());
            Assert.assertEquals(5, runway.load(Item.class, item.id()).score);
        }
    }

    /**
     * A container with a lazy link to an {@link Item}.
     *
     * @author Jeff Nelson
     */
    public static class Crate extends Record {

        /**
         * The display name.
         */
        String name;

        /**
         * The lazy link to the contained {@link Item}.
         */
        DeferredReference<Item> item;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param item the contained {@link Item}
         */
        public Crate(String name, Item item) {
            this.name = name;
            this.item = new DeferredReference<>(item);
        }
    }

    /**
     * A named {@link Audience} that can perform database operations.
     *
     * @author Jeff Nelson
     */
    public static class Viewer extends Record implements Audience {

        /**
         * The display name.
         */
        String name;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         */
        public Viewer(String name) {
            this.name = name;
        }
    }

    /**
     * An access-controlled record that only its designated {@link Viewer} can
     * discover.
     *
     * @author Jeff Nelson
     */
    public static class Secret extends Record implements AccessControl {

        /**
         * The display label.
         */
        String label;

        /**
         * The only {@link Viewer} that can discover this {@link Secret}.
         */
        Viewer visibleTo;

        /**
         * Construct a new instance.
         *
         * @param label the display label
         * @param visibleTo the only {@link Viewer} that can discover this
         *            {@link Secret}
         */
        public Secret(String label, Viewer visibleTo) {
            this.label = label;
            this.visibleTo = visibleTo;
        }

        @Override
        public boolean $isCreatableBy(Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(Audience audience) {
            return audience.equals(visibleTo);
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public java.util.Set<String> $readableBy(Audience audience) {
            return $isDiscoverableBy(audience) ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public java.util.Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public java.util.Set<String> $writableBy(Audience audience) {
            return $isDiscoverableBy(audience) ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public java.util.Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }
    }

    /**
     * A named unit of inventory with an orderable score.
     *
     * @author Jeff Nelson
     */
    public static class Item extends Record {

        /**
         * The display name.
         */
        String name;

        /**
         * The orderable score.
         */
        int score;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param score the initial score
         */
        public Item(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }

    /**
     * A container that links to a single {@link Item}.
     *
     * @author Jeff Nelson
     */
    public static class Basket extends Record {

        /**
         * The display name.
         */
        String name;

        /**
         * The linked {@link Item}.
         */
        Item item;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param item the linked {@link Item}
         */
        public Basket(String name, Item item) {
            this.name = name;
            this.item = item;
        }
    }

}
