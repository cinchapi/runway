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

import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record.ConstraintViolationException;
import com.cinchapi.runway.access.AccessControl;
import com.cinchapi.runway.access.Audience;
import com.cinchapi.runway.meta.Metadata;

/**
 * Tests for {@link Runway#stage()},
 * {@link Runway#run(java.util.function.Consumer) run} and
 * {@link Runway#supply(java.util.function.Function) supply}: the
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
     * <strong>Goal:</strong> Verify that single-key atomic operations resume
     * against the enclosing {@link Runway} after the {@link Transaction} that a
     * {@link Record} is bound to ends.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction} and commit.</li>
     * <li>Call {@code getAndUpdate} on the transactional copy to increment the
     * score.</li>
     * <li>Call {@code exchange} on the transactional copy to swap the score to
     * 100.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both operations succeed and a load through the
     * enclosing {@link Runway} observes a score of 100.
     */
    @Test
    public void testAtomicOperationsResumeAfterTransactionEnds() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            Assert.assertTrue(transaction.commit());
            Assert.assertEquals(1, (int) txItem.getAndUpdate("score",
                    (Integer score) -> score + 1));
            Assert.assertTrue(txItem.exchange("score", 100));
        }
        Assert.assertEquals(100, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a save that fails within a
     * {@link Transaction} cannot commit the writes that were staged before the
     * failure.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction} and change the
     * score to 2.</li>
     * <li>Save the {@link Item} together with a {@link Registration} whose
     * {@link Required} name is empty, so the save throws after the {@link Item
     * Item's} writes are staged.</li>
     * <li>Catch the exception and attempt to {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The commit is refused with an
     * {@link IllegalStateException} and the partial save does not persist: a
     * load through the enclosing {@link Runway} still observes a score of 1.
     */
    @Test
    public void testFailedSaveWithinTransactionCannotCommitPartialWrites() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Registration invalid = new Registration(null);
            try {
                transaction.save(txItem, invalid);
                Assert.fail("Expected the save to throw");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                transaction.commit();
                Assert.fail("Expected the commit to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
        }
        Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a failed save poisons a
     * {@link Transaction}, so every subsequent operation is refused except
     * {@code abort()}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction} and change the
     * score to 2.</li>
     * <li>Save the {@link Item} together with a {@link Registration} whose
     * {@link Required} name is empty, so the save throws.</li>
     * <li>Attempt a load, a save, a {@code create()} and an {@code afterCommit}
     * registration through the poisoned transaction.</li>
     * <li>Call {@code abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each attempted operation throws an
     * {@link IllegalStateException}, the abort succeeds and a load through the
     * enclosing {@link Runway} still observes a score of 1.
     */
    @Test
    public void testFailedSaveWithinTransactionRefusesFurtherOperations() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Registration invalid = new Registration(null);
            try {
                transaction.save(txItem, invalid);
                Assert.fail("Expected the save to throw");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                transaction.load(Item.class, item.id());
                Assert.fail("Expected the load to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                transaction.save(txItem);
                Assert.fail("Expected the save to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                transaction.create(Item.class, "gadget", 3);
                Assert.fail("Expected the create to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                transaction.afterCommit(() -> {});
                Assert.fail("Expected the hook registration to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            transaction.abort();
        }
        Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record#refresh() refresh} on
     * a {@link Record} bound to a poisoned {@link Transaction} is refused, so
     * the record cannot absorb writes from the failed save.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction} and change the
     * score to 2.</li>
     * <li>Save the {@link Item} together with a {@link Registration} whose
     * {@link Required} name is empty, so the save throws.</li>
     * <li>Call {@code refresh()} on the transactional copy.</li>
     * <li>Call {@code abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The refresh throws an
     * {@link IllegalStateException}, the abort succeeds and a load through the
     * enclosing {@link Runway} still observes a score of 1.
     */
    @Test
    public void testRefreshIsRefusedWhenTransactionIsPoisoned() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Registration invalid = new Registration(null);
            try {
                transaction.save(txItem, invalid);
                Assert.fail("Expected the save to throw");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                txItem.refresh();
                Assert.fail("Expected the refresh to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            transaction.abort();
        }
        Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Unique} violation within a
     * {@link Transaction} throws from the save call and poisons the
     * transaction, so the duplicate can never commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Handle} named "alpha".
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a second {@link Handle} named "alpha" through a
     * {@link Transaction} and {@code save()} it.</li>
     * <li>Attempt a load through the poisoned transaction.</li>
     * <li>Call {@code abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link ConstraintViolationException}, the subsequent load is refused with
     * an {@link IllegalStateException} and only one {@link Handle} named
     * "alpha" exists after the abort.
     */
    @Test
    public void testUniqueViolationWithinTransactionPoisonsIt() {
        Handle handle = new Handle("alpha");
        handle.assign(runway);
        Assert.assertTrue(handle.save());
        try (Transaction transaction = runway.stage()) {
            Handle duplicate = transaction.create(Handle.class, "alpha");
            try {
                duplicate.save();
                Assert.fail("Expected the save to throw");
            }
            catch (ConstraintViolationException e) {/* expected */}
            try {
                transaction.load(Handle.class, handle.id());
                Assert.fail("Expected the load to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            transaction.abort();
        }
        Assert.assertEquals(1, runway.count(Handle.class, Criteria.where()
                .key("name").operator(Operator.EQUALS).value("alpha").build()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code preventStaleWrites} save of a
     * {@link Record} that an earlier save in the same {@link Transaction}
     * staged succeeds instead of a false stale rejection.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Set the score to 3 and {@code save(true)}.</li>
     * <li>Commit the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves return {@code true} and the stored
     * score is 3 after the commit.
     */
    @Test
    public void testPreventStaleWritesAllowsResaveWithinTransaction() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            txItem.score = 3;
            Assert.assertTrue(txItem.save(true));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(3, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code preventStaleWrites} save
     * within a {@link Transaction} still rejects a {@link Record} that was
     * externally modified before the transaction started.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 that
     * is loaded through the enclosing {@link Runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Write a score of 99 through a separate client connection.</li>
     * <li>Start a {@link Transaction}, set the score to 50 on the loaded copy
     * and save it through the transaction with {@code preventStaleWrites}.</li>
     * <li>Call {@code abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a {@link StaleDataException}
     * and the stored score remains 99 after the abort.
     */
    @Test
    public void testPreventStaleWritesDetectsPreTransactionModification() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        Item loaded = runway.load(Item.class, item.id());
        client.set("score", 99, item.id());
        try (Transaction transaction = runway.stage()) {
            loaded.score = 50;
            try {
                transaction.save(true, loaded);
                Assert.fail("Expected the save to be rejected as stale");
            }
            catch (StaleDataException e) {/* expected */}
            transaction.abort();
        }
        Assert.assertEquals(99, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code preventStaleWrites} save of a
     * {@link Record} whose changes an earlier linked-graph save staged succeeds
     * instead of a false stale rejection.
     * <p>
     * <strong>Start state:</strong> A saved {@link Basket} that links to a
     * saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Basket} through a {@link Transaction} and set the
     * linked {@link Item Item's} score to 5.</li>
     * <li>Save the {@link Basket}, which stages the linked {@link Item Item's}
     * change.</li>
     * <li>Save the linked {@link Item} directly with
     * {@code preventStaleWrites}, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves return {@code true} and the stored
     * score is 5 after the commit.
     */
    @Test
    public void testPreventStaleWritesAllowsSaveAfterLinkedGraphStaging() {
        Item item = new Item("widget", 1);
        Basket basket = new Basket("tote", item);
        basket.assign(runway);
        Assert.assertTrue(runway.save(basket, item));
        try (Transaction transaction = runway.stage()) {
            Basket txBasket = transaction.load(Basket.class, basket.id());
            txBasket.item.score = 5;
            Assert.assertTrue(txBasket.save());
            Assert.assertTrue(txBasket.item.save(true));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(5, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code preventStaleWrites} save
     * still rejects a pre-transaction external modification of a {@link Record}
     * that an earlier save in the transaction processed without changes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Basket} that links to a
     * saved {@link Item} with a score of 1, with both loaded through the
     * enclosing {@link Runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Write a score of 99 to the {@link Item} through a separate client
     * connection.</li>
     * <li>Start a {@link Transaction}, change the {@link Basket Basket's} name
     * and save the {@link Basket}, which processes the unchanged linked
     * {@link Item}.</li>
     * <li>Set the score to 50 on the linked {@link Item} and save it with
     * {@code preventStaleWrites}.</li>
     * <li>Call {@code abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The flagged save throws a
     * {@link StaleDataException} and the stored score remains 99 after the
     * abort.
     */
    @Test
    public void testPreventStaleWritesDetectsModificationOfUnstagedRecord() {
        Item item = new Item("widget", 1);
        Basket basket = new Basket("tote", item);
        basket.assign(runway);
        Assert.assertTrue(runway.save(basket, item));
        Basket loaded = runway.load(Basket.class, basket.id());
        client.set("score", 99, item.id());
        try (Transaction transaction = runway.stage()) {
            loaded.name = "satchel";
            Assert.assertTrue(transaction.save(loaded));
            loaded.item.score = 50;
            try {
                loaded.item.save(true);
                Assert.fail("Expected the save to be rejected as stale");
            }
            catch (StaleDataException e) {/* expected */}
            transaction.abort();
        }
        Assert.assertEquals(99, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code preventStaleWrites} save of a
     * second in-memory instance succeeds when the only newer revisions are the
     * ones the {@link Transaction} itself staged.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * two copies of it loaded through the enclosing {@link Runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Set the score to 2 on the first copy and save it through a
     * {@link Transaction}.</li>
     * <li>Set the score to 3 on the second copy and save it through the
     * transaction with {@code preventStaleWrites}.</li>
     * <li>Commit the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves return {@code true} and the stored
     * score is 3 after the commit.
     */
    @Test
    public void testPreventStaleWritesIgnoresTheTransactionsOwnWrites() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        Item first = runway.load(Item.class, item.id());
        Item second = runway.load(Item.class, item.id());
        try (Transaction transaction = runway.stage()) {
            first.score = 2;
            Assert.assertTrue(transaction.save(first));
            second.score = 3;
            Assert.assertTrue(transaction.save(true, second));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(3, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Transaction#save(Record...)}
     * call that is rejected because a {@link Record} overrides the save
     * pipeline stages nothing and does not poison the transaction.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction} and set the score
     * to 2.</li>
     * <li>Save the {@link Item} and a {@link Bypass} together through the
     * transaction.</li>
     * <li>After the rejection, load the {@link Item} through the transaction,
     * save the transactional copy again and commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The rejected save throws an
     * {@link IllegalStateException} and stages nothing, the transaction remains
     * usable and the commit makes the score of 2 durable.
     */
    @Test
    public void testRejectedSaveOfOverrideSaveRecordDoesNotPoisonTransaction() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Bypass bypass = new Bypass("shortcut");
            try {
                transaction.save(txItem, bypass);
                Assert.fail("Expected the save to be rejected");
            }
            catch (IllegalStateException e) {/* expected */}
            Assert.assertEquals(1,
                    transaction.load(Item.class, item.id()).score);
            Assert.assertTrue(txItem.save());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#supply(java.util.function.Function) supply} on a
     * {@link Runway}-bound {@link Record} runs the work in a new transaction
     * that commits after the work completes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * a saved {@link Item} with a score of 5.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code claimScore(5)} on the first {@link Item}, which uses
     * {@code supply} to claim a score only if no other {@link Item} holds
     * it.</li>
     * <li>Call {@code claimScore(7)} on the first {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The claim of 5 returns {@code false} and the
     * stored score remains 1 at that point; the claim of 7 returns {@code true}
     * and the stored score is 7.
     */
    @Test
    public void testRecordSupplyStartsAndCommitsATransactionWhenRunwayBound() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        Item other = new Item("gadget", 5);
        other.assign(runway);
        Assert.assertTrue(other.save());
        Assert.assertFalse(item.claimScore(5));
        Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
        Assert.assertTrue(item.claimScore(7));
        Assert.assertEquals(7, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#supply(java.util.function.Function) supply} on a
     * {@link Record} bound to an open {@link Transaction} joins that
     * transaction instead of starting a new one.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction} and call
     * {@code claimScore(7)} on the transactional copy.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway} before the
     * commit.</li>
     * <li>Commit the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The claim returns {@code true} but the
     * non-transactional load still observes 1 before the commit, which proves
     * the work joined the open transaction; after the commit, the stored score
     * is 7.
     */
    @Test
    public void testRecordSupplyJoinsTheOpenTransactionWhenTransactionBound() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            Assert.assertTrue(txItem.claimScore(7));
            Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(7, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#run(java.util.function.Consumer) run} on a {@link Runway}-
     * bound {@link Record} executes work with no result within a transaction
     * that commits after the work completes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code run} on the {@link Item} with work that loads the
     * {@link Item} through the provided {@link Transaction}, sets the score to
     * 9 and saves it.</li>
     * <li>Within the work, after the save, load the {@link Item} through the
     * enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load within the work still observes 1,
     * which proves the save staged within the transaction; after {@code run}
     * returns, the stored score is 9.
     */
    @Test
    public void testRecordRunCommitsWorkWhenRunwayBound() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        item.run(transaction -> {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 9;
            Assert.assertTrue(txItem.save());
            Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
        });
        Assert.assertEquals(9, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Runway}-bound {@link Record}
     * joins the managed transaction that
     * {@link Record#run(java.util.function.Consumer) run} starts, so a direct
     * {@code save()} within the work stages instead of an immediate commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code run} on the {@link Item} with work that sets the score to
     * 9 on the {@link Item} itself and calls {@code save()} on it.</li>
     * <li>Within the work, after the save, load the {@link Item} through the
     * enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load within the work still observes 1,
     * which proves the save staged within the managed transaction; after
     * {@code run} returns, the stored score is 9.
     */
    @Test
    public void testRecordRunReceiverJoinsTheManagedTransaction() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        item.run(transaction -> {
            item.score = 9;
            Assert.assertTrue(item.save());
            Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
        });
        Assert.assertEquals(9, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that an
     * {@link Transaction#afterCommit(Runnable) afterCommit} hook runs exactly
     * once, only after the commit succeeds.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, change it and
     * {@code save()}.</li>
     * <li>Register an {@code afterCommit} hook that increments a counter.</li>
     * <li>Check the counter, then {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The counter is 0 before the commit and 1 after
     * it.
     */
    @Test
    public void testAfterCommitHookRunsOnceAfterCommit() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger effects = new AtomicInteger(0);
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            transaction.afterCommit(effects::incrementAndGet);
            Assert.assertEquals(0, effects.get());
            Assert.assertTrue(transaction.commit());
            Assert.assertEquals(1, effects.get());
        }
        Assert.assertEquals(1, effects.get());
    }

    /**
     * <strong>Goal:</strong> Verify that an
     * {@link Transaction#afterAbort(Runnable) afterAbort} hook runs when the
     * transaction ends without a successful commit, while an
     * {@link Transaction#afterCommit(Runnable) afterCommit} hook never runs.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and register an {@code afterCommit} hook
     * and an {@code afterAbort} hook.</li>
     * <li>Exit the try-with-resources block without a commit, so
     * {@code close()} aborts.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code afterAbort} counter is 1 and the
     * {@code afterCommit} counter is 0.
     */
    @Test
    public void testAfterAbortHookRunsWhenTransactionEndsWithoutCommit() {
        AtomicInteger commits = new AtomicInteger(0);
        AtomicInteger aborts = new AtomicInteger(0);
        try (Transaction transaction = runway.stage()) {
            transaction.afterCommit(commits::incrementAndGet);
            transaction.afterAbort(aborts::incrementAndGet);
        }
        Assert.assertEquals(0, commits.get());
        Assert.assertEquals(1, aborts.get());
    }

    /**
     * <strong>Goal:</strong> Verify that an
     * {@link Transaction#afterCommit(Runnable) afterCommit} hook registered by
     * work within {@link Runway#run(java.util.function.Consumer) run} fires
     * exactly once, even when a conflict forces the work to retry.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.run(work)} where the work loads the {@link Item},
     * saves a change and registers an {@code afterCommit} hook.</li>
     * <li>On the first attempt only, modify the {@link Item} outside of the
     * transaction after the transactional read, so the first commit attempt
     * fails and the work retries.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The work runs more than once, the hook counter
     * is exactly 1 and the stored score is 2.
     */
    @Test
    public void testAfterCommitHookRunsOnceDespiteConflictRetry() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicInteger effects = new AtomicInteger(0);
        runway.run(transaction -> {
            Item txItem = transaction.load(Item.class, item.id());
            if(attempts.getAndIncrement() == 0) {
                // Invalidate the transaction's snapshot so the first commit
                // attempt fails and the work retries.
                Item outside = runway.load(Item.class, item.id());
                outside.name = "conflict";
                Assert.assertTrue(outside.save());
            }
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            transaction.afterCommit(effects::incrementAndGet);
        });
        Assert.assertTrue(attempts.get() > 1);
        Assert.assertEquals(1, effects.get());
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#run(java.util.function.Consumer) run} does not re-run the
     * work when an {@code afterCommit} hook throws a
     * {@link TransactionException} after a successful commit.
     * <p>
     * <strong>Start state:</strong> No stored {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.run(work)} where the work creates and saves an
     * {@link Item} and registers an {@code afterCommit} hook that throws a
     * {@link TransactionException}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The hook's exception propagates, the work runs
     * exactly once and the {@link Item} is durable.
     */
    @Test
    public void testRunDoesNotRetryWhenAfterCommitHookThrowsTransactionException() {
        AtomicInteger runs = new AtomicInteger(0);
        try {
            runway.run(transaction -> {
                runs.incrementAndGet();
                Item item = transaction.create(Item.class, "widget", 1);
                Assert.assertTrue(item.save());
                transaction.afterCommit(() -> {
                    throw new TransactionException();
                });
            });
            Assert.fail("Expected the afterCommit hook's exception to"
                    + " propagate");
        }
        catch (TransactionException e) {
            // Expected: the writes are durable and the exception propagates
            // without a retry.
        }
        Assert.assertEquals(1, runs.get());
        Assert.assertEquals(1, runway.count(Item.class));
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#run(java.util.function.Consumer) run} does not re-run the
     * work when an {@code afterAbort} hook throws a
     * {@link TransactionException} after a failed commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.run(work)} where the work registers an
     * {@code afterAbort} hook that throws a {@link TransactionException}, loads
     * the {@link Item} and saves a change.</li>
     * <li>Within the work, modify the {@link Item} outside of the transaction
     * after the transactional read, so the commit fails and the hook runs.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The hook's exception propagates, the work runs
     * exactly once and the staged change is discarded.
     */
    @Test
    public void testRunDoesNotRetryWhenAfterAbortHookThrowsTransactionException() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger runs = new AtomicInteger(0);
        try {
            runway.run(transaction -> {
                runs.incrementAndGet();
                transaction.afterAbort(() -> {
                    throw new TransactionException();
                });
                Item txItem = transaction.load(Item.class, item.id());
                Item outside = runway.load(Item.class, item.id());
                outside.name = "conflict";
                Assert.assertTrue(outside.save());
                txItem.score = 2;
                Assert.assertTrue(txItem.save());
            });
            Assert.fail("Expected the afterAbort hook's exception to"
                    + " propagate");
        }
        catch (TransactionException e) {
            // Expected: the hook's failure propagates without a retry.
        }
        Assert.assertEquals(1, runs.get());
        Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#run(java.util.function.Consumer) run} preserves the work's
     * exception when an {@code afterAbort} hook also throws while the failed
     * attempt is closed.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.run(work)} where the work registers an
     * {@code afterAbort} hook that throws an {@link IllegalStateException} and
     * then throws a {@link RuntimeException} itself.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The caller receives the work's exception, and
     * the hook's exception is attached to it as a suppressed exception.
     */
    @Test
    public void testRunPreservesTheWorkExceptionWhenAnAfterAbortHookThrows() {
        try {
            runway.run(transaction -> {
                transaction.afterAbort(() -> {
                    throw new IllegalStateException("hook");
                });
                throw new RuntimeException("boom");
            });
            Assert.fail("Expected the work's exception to propagate");
        }
        catch (RuntimeException e) {
            Assert.assertEquals("boom", e.getMessage());
            Assert.assertEquals(1, e.getSuppressed().length);
            Assert.assertEquals("hook", e.getSuppressed()[0].getMessage());
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
     * <li>Within the work, after the save, load the {@link Item} through the
     * enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load within the work still observes 1,
     * which proves the save staged within the transaction; after {@code run}
     * returns, the stored score is 2.
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
            Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
        });
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#supply(java.util.function.Function) supply} returns the
     * result of the work after the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.supply(work)} where the work loads the
     * {@link Item}, sets the score to 2, saves and returns the new score.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns 2 and the stored score is 2.
     */
    @Test
    public void testSupplyReturnsTheWorkResult() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        int score = runway.supply(transaction -> {
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
     * <strong>Goal:</strong> Verify that {@link Transaction#save(Record...)}
     * binds the linked {@link Record Records} that are reachable from the saved
     * roots, so a linked {@link Record Record's} direct {@code save()} stages
     * within the transaction.
     * <p>
     * <strong>Start state:</strong> A saved {@link Basket} that links to an
     * {@link Item} with a score of 1, both bound to the enclosing
     * {@link Runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and save the {@link Basket} through
     * it.</li>
     * <li>Set the linked {@link Item Item's} score to 2 and call {@code save()}
     * on the {@link Item} directly.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway} before the
     * commit, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Before the commit the non-transactional load
     * observes 1; after the commit it observes 2.
     */
    @Test
    public void testLinkedRecordSavedThroughTransactionIsBoundToIt() {
        Item item = new Item("widget", 1);
        Basket basket = new Basket("bin", item);
        basket.assign(runway);
        Assert.assertTrue(basket.save());
        try (Transaction transaction = runway.stage()) {
            Assert.assertTrue(transaction.save(basket));
            item.score = 2;
            Assert.assertTrue(item.save());
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
     * <strong>Goal:</strong> Verify that the {@link Record} cached inside an
     * already resolved {@link DeferredReference} binds to a {@link Transaction}
     * along with its owner, so its saves stage within the transaction.
     * <p>
     * <strong>Start state:</strong> A saved {@link Crate} with a lazy link to a
     * saved {@link Item} that has a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Crate} through the enclosing {@link Runway} and
     * access its lazy {@link Item}.</li>
     * <li>Save the {@link Crate} through a {@link Transaction}.</li>
     * <li>Set the score to 7 on the lazily loaded copy and {@code save()}.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway} before the
     * commit.</li>
     * <li>Commit the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The non-transactional load still observes 1
     * before the commit; after the commit, the stored score is 7.
     */
    @Test
    public void testResolvedDeferredReferenceJoinsTheTransactionWithItsOwner() {
        Item item = new Item("widget", 1);
        Crate crate = new Crate("bin", item);
        crate.assign(runway);
        Assert.assertTrue(runway.save(crate, item));
        Crate loaded = runway.load(Crate.class, crate.id());
        Item lazy = loaded.item.get();
        try (Transaction transaction = runway.stage()) {
            Assert.assertTrue(transaction.save(loaded));
            lazy.score = 7;
            Assert.assertTrue(lazy.save());
            Assert.assertEquals(1, runway.load(Item.class, item.id()).score);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(7, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link Record#onLoad() onLoad}
     * hook of a {@link Record} loaded through a {@link Transaction} runs while
     * the record is bound to it, so reads within the hook observe staged state.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * a saved {@link Probe} that references the {@link Item Item's} id.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Load the {@link Probe} through the transaction; its {@code onLoad()}
     * hook loads the {@link Item} through the {@link Probe Probe's} own binding
     * and captures the observed score.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The hook observes the staged score of 2.
     */
    @Test
    public void testOnLoadHookObservesStagedStateWithinTransaction() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        Probe probe = new Probe(item.id());
        probe.assign(runway);
        Assert.assertTrue(probe.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Probe txProbe = transaction.load(Probe.class, probe.id());
            Assert.assertEquals(2, txProbe.observedScore);
            transaction.abort();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a filter predicate evaluated within a
     * {@link Transaction} runs against transaction-bound {@link Record
     * Records}, so a {@link DeferredReference} the predicate resolves observes
     * staged state.
     * <p>
     * <strong>Start state:</strong> A saved {@link Crate} with a lazy link to a
     * saved {@link Item} that has a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, set the score to
     * 2 and {@code save()}.</li>
     * <li>Fetch {@link Crate Crates} through the transaction with a filter that
     * resolves each {@link Crate Crate's} lazy {@link Item} and matches a score
     * of 2.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The filter matches the {@link Crate}, because
     * its lazy {@link Item} resolves within the transaction.
     */
    @Test
    public void testFilterPredicateObservesStagedStateWithinTransaction() {
        Item item = new Item("widget", 1);
        Crate crate = new Crate("bin", item);
        crate.assign(runway);
        Assert.assertTrue(runway.save(crate, item));
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Set<Crate> matches = transaction.fetch(Selection.of(Crate.class)
                    .filter($crate -> $crate.item.get().score == 2));
            Assert.assertEquals(1, matches.size());
            transaction.abort();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a deletion staged within a
     * {@link Transaction} is invisible outside of it until the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, call
     * {@code deleteOnSave()} and {@code save()}.</li>
     * <li>Load the {@link Item} through the transaction and through the
     * enclosing {@link Runway} before the commit, and through the
     * {@link Runway} after it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Before the commit, the transactional load
     * returns {@code null} and the outside load returns the {@link Item}; after
     * the commit, the outside load returns {@code null}.
     */
    @Test
    public void testDeletionWithinTransactionIsInvisibleUntilCommit() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.deleteOnSave();
            Assert.assertTrue(txItem.save());
            Assert.assertNull(transaction.load(Item.class, item.id()));
            Assert.assertNotNull(runway.load(Item.class, item.id()));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertNull(runway.load(Item.class, item.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Transaction#abort() abort}
     * discards a staged deletion and that no delete notification fires.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} and a delete listener
     * that counts notifications for {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, call
     * {@code deleteOnSave()} and {@code save()}.</li>
     * <li>Call {@code abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Item} still exists outside of the
     * transaction and the delete listener never fires.
     */
    @Test
    public void testAbortDiscardsStagedDeletion() throws InterruptedException {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger deletes = new AtomicInteger(0);
        runway.properties().onDelete(Item.class,
                record -> deletes.incrementAndGet());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.deleteOnSave();
            Assert.assertTrue(txItem.save());
            transaction.abort();
        }
        Assert.assertNotNull(runway.load(Item.class, item.id()));
        Thread.sleep(250);
        Assert.assertEquals(0, deletes.get());
    }

    /**
     * <strong>Goal:</strong> Verify that delete notifications for records
     * deleted within a {@link Transaction} fire only after the commit succeeds.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} and a delete listener
     * that counts notifications for {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, call
     * {@code deleteOnSave()} and {@code save()}.</li>
     * <li>Record the notification count, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count is 0 before the commit and rises to
     * 1 after it.
     */
    @Test
    public void testDeleteNotificationsFireOnlyAfterCommit()
            throws InterruptedException {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger deletes = new AtomicInteger(0);
        runway.properties().onDelete(Item.class,
                record -> deletes.incrementAndGet());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.deleteOnSave();
            Assert.assertTrue(txItem.save());
            Assert.assertEquals(0, deletes.get());
            Assert.assertTrue(transaction.commit());
        }
        long stop = System.currentTimeMillis() + 5000;
        while (deletes.get() == 0 && System.currentTimeMillis() < stop) {
            Thread.sleep(10);
        }
        Assert.assertEquals(1, deletes.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link CascadeDelete} companion
     * deletion resolves within the {@link Transaction}, so the parent and the
     * companion disappear together at the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Kit} whose
     * {@link CascadeDelete} field links to a saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Kit} through a {@link Transaction}, call
     * {@code deleteOnSave()} and {@code save()}.</li>
     * <li>Load the {@link Item} through the enclosing {@link Runway} before the
     * commit, and load both records after it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Item} still exists before the
     * commit; after the commit, the {@link Kit} and the {@link Item} are both
     * gone.
     */
    @Test
    public void testCascadeDeleteResolvesWithinTransaction() {
        Item part = new Item("part", 1);
        Kit kit = new Kit("toolkit", part);
        kit.assign(runway);
        Assert.assertTrue(runway.save(kit, part));
        try (Transaction transaction = runway.stage()) {
            Kit txKit = transaction.load(Kit.class, kit.id());
            txKit.deleteOnSave();
            Assert.assertTrue(txKit.save());
            Assert.assertNotNull(runway.load(Item.class, part.id()));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertNull(runway.load(Kit.class, kit.id()));
        Assert.assertNull(runway.load(Item.class, part.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link JoinDelete} lookup resolves
     * within the {@link Transaction}, so a record that joins the deletion
     * disappears with its target at the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Coupon} whose
     * {@link JoinDelete} field links to a saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, call
     * {@code deleteOnSave()} and {@code save()}.</li>
     * <li>Load the {@link Coupon} through the enclosing {@link Runway} before
     * the commit, and load both records after it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Coupon} still exists before the
     * commit; after the commit, the {@link Item} and the {@link Coupon} are
     * both gone.
     */
    @Test
    public void testJoinDeleteResolvesWithinTransaction() {
        Item item = new Item("widget", 1);
        Coupon coupon = new Coupon("promo", item);
        coupon.assign(runway);
        Assert.assertTrue(runway.save(coupon, item));
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.deleteOnSave();
            Assert.assertTrue(txItem.save());
            Assert.assertNotNull(runway.load(Coupon.class, coupon.id()));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertNull(runway.load(Item.class, item.id()));
        Assert.assertNull(runway.load(Coupon.class, coupon.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link CaptureDelete} lookup
     * resolves within the {@link Transaction}, so a stored reference to the
     * deleted record is removed at the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Shelf} whose
     * {@link CaptureDelete} field links to a saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction}, call
     * {@code deleteOnSave()} and {@code save()}.</li>
     * <li>Load the {@link Shelf} through the enclosing {@link Runway} before
     * the commit, and load both records after it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Shelf Shelf's} reference is intact
     * before the commit; after the commit, the reference is {@code null} and
     * the {@link Item} is gone.
     */
    @Test
    public void testCaptureDeleteRemovesStoredReferenceAtCommit() {
        Item item = new Item("widget", 1);
        Shelf shelf = new Shelf("front", item);
        shelf.assign(runway);
        Assert.assertTrue(runway.save(shelf, item));
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.deleteOnSave();
            Assert.assertTrue(txItem.save());
            Assert.assertNotNull(runway.load(Shelf.class, shelf.id()).display);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertNull(runway.load(Shelf.class, shelf.id()).display);
        Assert.assertNull(runway.load(Item.class, item.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a deletion staged in one save is final
     * for the later saves of the same {@link Transaction}, so an id-equal
     * instance saved afterward adopts the deletion instead of restoring the
     * record.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load two id-equal copies of the {@link Item} through a
     * {@link Transaction}.</li>
     * <li>Delete the first copy with {@code deleteOnSave()} and
     * {@code save()}.</li>
     * <li>Modify the second copy and {@code save()} it in a separate save
     * call.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The commit succeeds and the {@link Item} does
     * not exist afterward.
     */
    @Test
    public void testDeletionIsFinalAcrossSavesWithinTransaction() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item doomed = transaction.load(Item.class, item.id());
            Item copy = transaction.load(Item.class, item.id());
            doomed.deleteOnSave();
            Assert.assertTrue(doomed.save());
            copy.name = "revived";
            Assert.assertTrue(copy.save());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertNull(runway.load(Item.class, item.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a record changed in one save and
     * deleted in a later save of the same {@link Transaction} dispatches only a
     * delete notification at the commit, because the commit is one durable
     * event.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item}, a save listener and a
     * delete listener that count notifications for {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load two id-equal copies of the {@link Item} through a
     * {@link Transaction}.</li>
     * <li>Change and {@code save()} the first copy.</li>
     * <li>Delete the second copy with {@code deleteOnSave()} and
     * {@code save()}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Item} does not exist after the
     * commit, the delete listener fires exactly once and the save listener
     * never fires.
     */
    @Test
    public void testChangeBeforeDeletionWithinTransactionDispatchesOnlyTheDeletion()
            throws InterruptedException {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger saves = new AtomicInteger(0);
        AtomicInteger deletes = new AtomicInteger(0);
        runway.properties().onSave(Item.class,
                record -> saves.incrementAndGet());
        runway.properties().onDelete(Item.class,
                record -> deletes.incrementAndGet());
        try (Transaction transaction = runway.stage()) {
            Item changed = transaction.load(Item.class, item.id());
            Item doomed = transaction.load(Item.class, item.id());
            changed.score = 2;
            Assert.assertTrue(changed.save());
            doomed.deleteOnSave();
            Assert.assertTrue(doomed.save());
            Assert.assertTrue(transaction.commit());
        }
        long stop = System.currentTimeMillis() + 5000;
        while (deletes.get() == 0 && System.currentTimeMillis() < stop) {
            Thread.sleep(10);
        }
        Thread.sleep(250);
        Assert.assertEquals(1, deletes.get());
        Assert.assertEquals(0, saves.get());
        Assert.assertNull(runway.load(Item.class, item.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link CaptureDelete} reference
     * staged after a deletion in an earlier save of the same
     * {@link Transaction} is removed before the commit, so the commit never
     * persists a reference to a deleted record.
     * <p>
     * <strong>Start state:</strong> A saved {@link Shelf} with no displayed
     * {@link Item} and a saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Shelf} and two id-equal copies of the {@link Item}
     * through a {@link Transaction}.</li>
     * <li>Delete one {@link Item} copy with {@code deleteOnSave()} and
     * {@code save()}.</li>
     * <li>Set the {@link Shelf Shelf's} reference to the other copy and
     * {@code save()} the {@link Shelf} in a separate save call.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> After the commit, the stored and in-memory
     * {@link Shelf} references are {@code null} and the {@link Item} is gone.
     */
    @Test
    public void testReferenceStagedAfterDeletionWithinTransactionIsRemoved() {
        Item item = new Item("widget", 1);
        Shelf shelf = new Shelf("front", null);
        shelf.assign(runway);
        Assert.assertTrue(runway.save(shelf, item));
        Shelf txShelf;
        try (Transaction transaction = runway.stage()) {
            txShelf = transaction.load(Shelf.class, shelf.id());
            Item copy = transaction.load(Item.class, item.id());
            Item doomed = transaction.load(Item.class, item.id());
            doomed.deleteOnSave();
            Assert.assertTrue(doomed.save());
            txShelf.display = copy;
            Assert.assertTrue(txShelf.save());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertNull(txShelf.display);
        Assert.assertNull(runway.load(Shelf.class, shelf.id()).display);
        Assert.assertNull(runway.load(Item.class, item.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Transaction#abort() abort}
     * discards cached audit metadata, so the record does not report a revision
     * that never committed.
     * <p>
     * <strong>Start state:</strong> A saved {@link Receipt}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record the {@link Receipt Receipt's} last update timestamp.</li>
     * <li>Load the {@link Receipt} through a {@link Transaction}, change it and
     * {@code save()}.</li>
     * <li>Read the last update timestamp through the transactional copy, which
     * observes the staged revision.</li>
     * <li>Call {@code abort()} and read the last update timestamp again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The read within the transaction observes a
     * newer timestamp than the committed one; the read after the abort reports
     * the committed timestamp.
     */
    @Test
    public void testAbortDiscardsCachedAuditMetadata() {
        Receipt receipt = new Receipt("original");
        receipt.assign(runway);
        Assert.assertTrue(receipt.save());
        Timestamp before = receipt.lastUpdatedAt();
        try (Transaction transaction = runway.stage()) {
            Receipt txReceipt = transaction.load(Receipt.class, receipt.id());
            txReceipt.memo = "staged";
            Assert.assertTrue(txReceipt.save());
            Timestamp staged = txReceipt.lastUpdatedAt();
            Assert.assertTrue(staged.getMicros() > before.getMicros());
            transaction.abort();
            Assert.assertEquals(before, txReceipt.lastUpdatedAt());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Transaction} releases its
     * staged state after it ends, so one retained {@link Record} does not pin
     * every record the transaction saved.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load both {@link Item Items} through a {@link Transaction}, change
     * and {@code save()} each one, and commit.</li>
     * <li>Keep a strong reference to the first {@link Item} and only a
     * {@link WeakReference} to the second.</li>
     * <li>Run the garbage collector until the weak reference clears or a
     * timeout passes.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The weak reference clears, because the ended
     * transaction that the retained {@link Item} is bound to no longer holds
     * the other record.
     */
    @Test
    public void testEndedTransactionDoesNotRetainStagedRecords()
            throws InterruptedException {
        Item kept = new Item("kept", 1);
        Item other = new Item("other", 2);
        kept.assign(runway);
        Assert.assertTrue(runway.save(kept, other));
        Item bound;
        WeakReference<Item> weak;
        try (Transaction transaction = runway.stage()) {
            bound = transaction.load(Item.class, kept.id());
            Item txOther = transaction.load(Item.class, other.id());
            bound.score = 10;
            Assert.assertTrue(bound.save());
            txOther.score = 20;
            Assert.assertTrue(txOther.save());
            weak = new WeakReference<>(txOther);
            Assert.assertTrue(transaction.commit());
        }
        long stop = System.currentTimeMillis() + 5000;
        while (weak.get() != null && System.currentTimeMillis() < stop) {
            System.gc();
            Thread.sleep(10);
        }
        Assert.assertNull(weak.get());
        Assert.assertEquals(10, bound.score);
    }

    /**
     * <strong>Goal:</strong> Verify that the creation permission check in
     * {@link Audience#create(Class, Object...)} runs within the same database
     * context that the {@link Audience} operates against, so a check that reads
     * through the new record observes the transaction's staged state.
     * <p>
     * <strong>Start state:</strong> A saved {@link Viewer} and no stored
     * {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Viewer} through a {@link Transaction}.</li>
     * <li>Create and {@code save()} an {@link Item} through the transaction, so
     * it is staged but not committed.</li>
     * <li>Call {@code create(Gated.class, ...)} on the viewer, whose permission
     * check requires at least one visible {@link Item}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The creation succeeds, because the check
     * observes the staged {@link Item} within the transaction.
     */
    @Test
    public void testAudienceCreateAuthorizesWithinTransaction() {
        Viewer viewer = new Viewer("alice");
        viewer.assign(runway);
        Assert.assertTrue(viewer.save());
        try (Transaction transaction = runway.stage()) {
            Viewer txViewer = transaction.load(Viewer.class, viewer.id());
            Item item = transaction.create(Item.class, "widget", 1);
            Assert.assertTrue(item.save());
            Gated gated = txViewer.create(Gated.class, "pass");
            Assert.assertNotNull(gated);
            transaction.abort();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Transaction} refuses to save
     * a {@link Record} that is bound to a different open {@link Transaction},
     * and that the refusal leaves both transactions intact.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a first {@link Transaction}.</li>
     * <li>Start a second {@link Transaction} and attempt to save the loaded
     * {@link Item} through it.</li>
     * <li>After the refusal, change the {@link Item} and {@code save()} it,
     * then commit the first transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second transaction's save throws an
     * {@link IllegalStateException}, and the {@link Item} still saves and
     * commits through the first transaction.
     */
    @Test
    public void testTransactionSaveRefusesRecordBoundToAnotherOpenTransaction() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction tx1 = runway.stage()) {
            Item txItem = tx1.load(Item.class, item.id());
            try (Transaction tx2 = runway.stage()) {
                try {
                    tx2.save(txItem);
                    Assert.fail("Expected the save to be refused");
                }
                catch (IllegalStateException e) {/* expected */}
                tx2.abort();
            }
            txItem.score = 2;
            Assert.assertTrue(txItem.save());
            Assert.assertTrue(tx1.commit());
        }
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a direct {@link Runway#save(Record...)
     * save} refuses a {@link Record} that is bound to an open
     * {@link Transaction}, so the transaction's atomic boundary cannot be
     * bypassed by accident.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Item} through a {@link Transaction} and change
     * it.</li>
     * <li>Attempt to save the loaded {@link Item} directly through the
     * enclosing {@link Runway}.</li>
     * <li>After the refusal, {@code save()} the {@link Item} and commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The direct save throws an
     * {@link IllegalStateException}, and the change commits through the
     * transaction.
     */
    @Test
    public void testDirectSaveRefusesRecordBoundToAnOpenTransaction() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        try (Transaction transaction = runway.stage()) {
            Item txItem = transaction.load(Item.class, item.id());
            txItem.score = 2;
            try {
                runway.save(txItem);
                Assert.fail("Expected the direct save to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            Assert.assertTrue(txItem.save());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#run(java.util.function.Consumer) run} refuses to start work
     * when the record's {@link Transaction} is poisoned, so no side effect of
     * the work executes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Counter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Counter} through a {@link Transaction}.</li>
     * <li>Poison the transaction with a save of a {@link Registration} whose
     * {@link Required} name is empty.</li>
     * <li>Call {@code bump()} on the transactional copy.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws an
     * {@link IllegalStateException} and the work never starts.
     */
    @Test
    public void testRecordWorkIsRefusedBeforeItRunsWhenTransactionIsPoisoned() {
        Counter counter = new Counter(0);
        counter.assign(runway);
        Assert.assertTrue(counter.save());
        try (Transaction transaction = runway.stage()) {
            Counter txCounter = transaction.load(Counter.class, counter.id());
            Registration invalid = new Registration(null);
            try {
                transaction.save(invalid);
                Assert.fail("Expected the save to throw");
            }
            catch (IllegalStateException e) {/* expected */}
            try {
                txCounter.bump();
                Assert.fail("Expected the work to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
            Assert.assertFalse(txCounter.workStarted);
            transaction.abort();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#run(java.util.function.Consumer) run} refuses to start work
     * from a thread that does not own the record's open {@link Transaction}, so
     * no side effect of the work executes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Counter} and an open
     * {@link Transaction} started on the test thread.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Counter} through the {@link Transaction}.</li>
     * <li>Submit a {@code bump()} of the transactional copy to a different
     * thread.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The other thread's call throws an
     * {@link IllegalStateException} and the work never starts.
     */
    @Test
    public void testRecordWorkIsRefusedBeforeItRunsOnAnotherThread()
            throws Exception {
        Counter counter = new Counter(0);
        counter.assign(runway);
        Assert.assertTrue(counter.save());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Transaction transaction = runway.stage()) {
            Counter txCounter = transaction.load(Counter.class, counter.id());
            Future<?> future = executor.submit(() -> txCounter.bump());
            try {
                future.get();
                Assert.fail("Expected an IllegalStateException");
            }
            catch (ExecutionException e) {
                Assert.assertTrue(
                        e.getCause() instanceof IllegalStateException);
            }
            Assert.assertFalse(txCounter.workStarted);
            transaction.abort();
        }
        finally {
            executor.shutdownNow();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the changed copy of a record receives
     * the commit's lifecycle consequences when a clean id-equal copy is saved
     * in a later save call of the same {@link Transaction}, so the clean copy
     * is never falsely marked as synchronized.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * a save listener that captures the notified instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load two id-equal copies of the {@link Item} through a
     * {@link Transaction}.</li>
     * <li>Change and {@code save()} the second copy, then {@code save()} the
     * unchanged first copy in a separate call, then commit.</li>
     * <li>After the commit, change a different field on the unchanged copy and
     * save it with stale-write prevention.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The notification delivers the changed copy,
     * the stale copy's save throws a {@link StaleDataException} and the
     * committed score survives.
     */
    @Test
    public void testChangedCopySpeaksForTheCommitDespiteLaterCleanSave()
            throws InterruptedException {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicReference<Item> notified = new AtomicReference<>();
        runway.properties().onSave(Item.class, notified::set);
        Item stale;
        Item changed;
        try (Transaction transaction = runway.stage()) {
            stale = transaction.load(Item.class, item.id());
            changed = transaction.load(Item.class, item.id());
            changed.score = 2;
            Assert.assertTrue(changed.save());
            Assert.assertTrue(stale.save());
            Assert.assertTrue(transaction.commit());
        }
        long stop = System.currentTimeMillis() + 5000;
        while (notified.get() == null && System.currentTimeMillis() < stop) {
            Thread.sleep(10);
        }
        Assert.assertSame(changed, notified.get());
        stale.name = "renamed";
        try {
            stale.save(true);
            Assert.fail("Expected the stale copy's save to be rejected");
        }
        catch (StaleDataException e) {/* expected */}
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that the changed copy of a record receives
     * the commit's lifecycle consequences when a clean id-equal copy is part of
     * the same save call, so the clean copy is never falsely marked as
     * synchronized.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * a save listener that captures the notified instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load two id-equal copies of the {@link Item} through a
     * {@link Transaction}.</li>
     * <li>Change the second copy and save both copies in one call, with the
     * unchanged copy listed last, then commit.</li>
     * <li>After the commit, change a different field on the unchanged copy and
     * save it with stale-write prevention.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The notification delivers the changed copy,
     * the stale copy's save throws a {@link StaleDataException} and the
     * committed score survives.
     */
    @Test
    public void testChangedCopySpeaksForTheCommitDespiteCleanCopyInSameSave()
            throws InterruptedException {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicReference<Item> notified = new AtomicReference<>();
        runway.properties().onSave(Item.class, notified::set);
        Item stale;
        Item changed;
        try (Transaction transaction = runway.stage()) {
            stale = transaction.load(Item.class, item.id());
            changed = transaction.load(Item.class, item.id());
            changed.score = 2;
            Assert.assertTrue(transaction.save(changed, stale));
            Assert.assertTrue(transaction.commit());
        }
        long stop = System.currentTimeMillis() + 5000;
        while (notified.get() == null && System.currentTimeMillis() < stop) {
            Thread.sleep(10);
        }
        Assert.assertSame(changed, notified.get());
        stale.name = "renamed";
        try {
            stale.save(true);
            Assert.fail("Expected the stale copy's save to be rejected");
        }
        catch (StaleDataException e) {/* expected */}
        Assert.assertEquals(2, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that the later of two changed id-equal
     * copies receives the commit's lifecycle consequences, because its writes
     * land last.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1 and
     * a save listener that captures the notified instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load two id-equal copies of the {@link Item} through a
     * {@link Transaction}.</li>
     * <li>Change and {@code save()} the first copy, then change and
     * {@code save()} the second copy, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The notification delivers the second copy and
     * the stored score is the second copy's value.
     */
    @Test
    public void testLaterChangedCopySpeaksForTheCommit()
            throws InterruptedException {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicReference<Item> notified = new AtomicReference<>();
        runway.properties().onSave(Item.class, notified::set);
        Item second;
        try (Transaction transaction = runway.stage()) {
            Item first = transaction.load(Item.class, item.id());
            second = transaction.load(Item.class, item.id());
            first.score = 2;
            Assert.assertTrue(first.save());
            second.score = 3;
            Assert.assertTrue(second.save());
            Assert.assertTrue(transaction.commit());
        }
        long stop = System.currentTimeMillis() + 5000;
        while (notified.get() == null && System.currentTimeMillis() < stop) {
            Thread.sleep(10);
        }
        Assert.assertSame(second, notified.get());
        Assert.assertEquals(3, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Runway#supply(java.util.function.Function) supply} retries the
     * work when a conflicting outside write invalidates the transaction in the
     * middle of the work, so the conflict never escapes to the caller.
     * <p>
     * <strong>Start state:</strong> A saved {@link Item} with a score of 1.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.supply(work)} where the work reads the
     * {@link Item} through the transaction.</li>
     * <li>On the first attempt only, commit a conflicting change to the
     * {@link Item} outside of the transaction.</li>
     * <li>Read the {@link Item} through the transaction again, count the
     * completion, derive a new score from the read and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The work runs more than once, only the
     * successful attempt completes, and the result derives from the outside
     * write.
     */
    @Test
    public void testWorkRetriesWhenTheTransactionIsInvalidatedMidWork() {
        Item item = new Item("widget", 1);
        item.assign(runway);
        Assert.assertTrue(item.save());
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicInteger completed = new AtomicInteger(0);
        int result = runway.supply(transaction -> {
            int attempt = attempts.incrementAndGet();
            transaction.load(Item.class, item.id());
            if(attempt == 1) {
                Item outside = runway.load(Item.class, item.id());
                outside.score = 5;
                Assert.assertTrue(outside.save());
            }
            Item current = transaction.load(Item.class, item.id());
            completed.incrementAndGet();
            current.score = current.score + 1;
            Assert.assertTrue(current.save());
            return current.score;
        });
        Assert.assertTrue(attempts.get() > 1);
        Assert.assertEquals(1, completed.get());
        Assert.assertEquals(6, result);
        Assert.assertEquals(6, runway.load(Item.class, item.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link RetryExhaustedException} from
     * {@link Runway#run(java.util.function.Consumer) run} carries the final
     * conflict as its cause.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} whose
     * {@link AtomicRetryPolicy} permits a single retry, and a saved
     * {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code run(work)} where every attempt reads the {@link Item}
     * through the transaction, commits a conflicting outside write, and then
     * operates on the transaction again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The retries exhaust, and the thrown
     * {@link RetryExhaustedException} has a {@link TransactionException} cause.
     */
    @Test
    public void testRetryExhaustionCarriesTheFinalConflictAsCause()
            throws Exception {
        try (Runway contentious = runwayBuilder()
                .atomicRetryPolicy(AtomicRetryPolicy.create(1, 1)).build()) {
            Item item = new Item("widget", 1);
            item.assign(contentious);
            Assert.assertTrue(item.save());
            try {
                contentious.run(transaction -> {
                    transaction.load(Item.class, item.id());
                    Item outside = contentious.load(Item.class, item.id());
                    outside.score = outside.score + 1;
                    Assert.assertTrue(outside.save());
                    Item doomed = transaction.load(Item.class, item.id());
                    doomed.score = doomed.score + 1;
                    Assert.assertTrue(doomed.save());
                });
                Assert.fail("Expected the retries to exhaust");
            }
            catch (RetryExhaustedException e) {
                Assert.assertTrue(e.getCause() instanceof TransactionException);
            }
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

        /**
         * Set the score to {@code score} within this {@link Item Item's}
         * transactional scope, only if no other {@link Item} currently holds
         * it.
         *
         * @param score the score to claim
         * @return {@code true} if the score is claimed
         */
        public boolean claimScore(int score) {
            return supply(transaction -> {
                Criteria taken = Criteria.where().key("score")
                        .operator(Operator.EQUALS).value(score).build();
                Item holder = transaction.findUnique(Item.class, taken);
                if(holder == null) {
                    this.score = score;
                    return transaction.save(this);
                }
                else {
                    return false;
                }
            });
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

    /**
     * A {@link Record} whose {@code name} is {@link Required}, so a save with
     * an empty name throws.
     *
     * @author Jeff Nelson
     */
    public static class Registration extends Record {

        /**
         * The required display name.
         */
        @Required
        String name;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         */
        public Registration(String name) {
            this.name = name;
        }
    }

    /**
     * A {@link Record} whose name must be {@link Unique}.
     *
     * @author Jeff Nelson
     */
    public static class Handle extends Record {

        /**
         * The unique display name.
         */
        @Unique
        String name;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         */
        public Handle(String name) {
            this.name = name;
        }
    }

    /**
     * A {@link Record} that {@link #overrideSave() overrides} the save
     * pipeline, so it cannot save within a {@link Transaction}.
     *
     * @author Jeff Nelson
     */
    public static class Bypass extends Record {

        /**
         * The display name.
         */
        String name;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         */
        public Bypass(String name) {
            this.name = name;
        }

        @Override
        protected Supplier<Boolean> overrideSave() {
            return () -> true;
        }
    }

    /**
     * A {@link Record} whose {@link Record#onLoad() onLoad} hook reads the
     * {@link Item} named by {@link #itemId} through the record's own binding
     * and captures the observed score.
     *
     * @author Jeff Nelson
     */
    public static class Probe extends Record {

        /**
         * The id of the {@link Item} that {@code onLoad()} observes.
         */
        long itemId;

        /**
         * The score that {@code onLoad()} observed, or 0 before a load.
         */
        transient int observedScore;

        /**
         * Construct a new instance.
         *
         * @param itemId the id of the {@link Item} to observe on load
         */
        public Probe(long itemId) {
            this.itemId = itemId;
        }

        @Override
        protected void onLoad() {
            observedScore = db.load(Item.class, itemId).score;
        }
    }

    /**
     * A {@link Record} whose {@link Item} part is deleted along with it.
     *
     * @author Jeff Nelson
     */
    public static class Kit extends Record {

        /**
         * The display name.
         */
        String name;

        /**
         * The contained part, deleted when this {@link Kit} is deleted.
         */
        @CascadeDelete
        Item part;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param part the contained part
         */
        public Kit(String name, Item part) {
            this.name = name;
            this.part = part;
        }
    }

    /**
     * A {@link Record} that joins the deletion of its {@link Item}.
     *
     * @author Jeff Nelson
     */
    public static class Coupon extends Record {

        /**
         * The display name.
         */
        String name;

        /**
         * The promoted {@link Item}; this {@link Coupon} is deleted when the
         * item is deleted.
         */
        @JoinDelete
        Item item;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param item the promoted {@link Item}
         */
        public Coupon(String name, Item item) {
            this.name = name;
            this.item = item;
        }
    }

    /**
     * A {@link Record} whose reference to a deleted {@link Item} is removed.
     *
     * @author Jeff Nelson
     */
    public static class Shelf extends Record {

        /**
         * The display name.
         */
        String name;

        /**
         * The displayed {@link Item}; the reference is removed when the item is
         * deleted.
         */
        @CaptureDelete
        Item display;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param display the displayed {@link Item}
         */
        public Shelf(String name, Item display) {
            this.name = name;
            this.display = display;
        }
    }

    /**
     * A {@link Record} with {@link Metadata}, so a test can read its audit
     * history.
     *
     * @author Jeff Nelson
     */
    public static class Receipt extends Record implements Metadata {

        /**
         * The memo text.
         */
        String memo;

        /**
         * Construct a new instance.
         *
         * @param memo the memo text
         */
        public Receipt(String memo) {
            this.memo = memo;
        }
    }

    /**
     * A {@link Record} whose {@code bump()} runs work within its transactional
     * scope and reports whether the work started.
     *
     * @author Jeff Nelson
     */
    public static class Counter extends Record {

        /**
         * The tallied count.
         */
        int count;

        /**
         * Whether the {@link #bump()} work began to run.
         */
        transient boolean workStarted = false;

        /**
         * Construct a new instance.
         *
         * @param count the initial count
         */
        public Counter(int count) {
            this.count = count;
        }

        /**
         * Increment the count within this {@link Counter Counter's}
         * transactional scope.
         */
        public void bump() {
            run(transaction -> {
                workStarted = true;
                count++;
                save();
            });
        }
    }

    /**
     * An access-controlled {@link Record} whose creation is permitted only when
     * at least one {@link Item} is visible to the creation check.
     *
     * @author Jeff Nelson
     */
    public static class Gated extends Record implements AccessControl {

        /**
         * The display label.
         */
        String label;

        /**
         * Construct a new instance.
         *
         * @param label the display label
         */
        public Gated(String label) {
            this.label = label;
        }

        @Override
        public boolean $isCreatableBy(Audience audience) {
            return db.count(Item.class) > 0;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public java.util.Set<String> $readableBy(Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public java.util.Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public java.util.Set<String> $writableBy(Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public java.util.Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }
    }

}
