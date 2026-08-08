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
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record.ConstraintViolationException;
import com.cinchapi.runway.access.AccessControl;
import com.cinchapi.runway.access.Audience;

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
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored score is 9 after {@code run}
     * returns.
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

}
