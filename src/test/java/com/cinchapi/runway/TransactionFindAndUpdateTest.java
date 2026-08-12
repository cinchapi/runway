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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.FindUniqueAndUpdateTest.Item;
import com.cinchapi.runway.FindUniqueAndUpdateTest.SpecialItem;

/**
 * Tests for the {@code find*AndUpdate} operations on a
 * {@link TransactionInterface}, where the find and the write stage within an
 * open transaction and commit with it, and run atomically against the enclosing
 * {@link Runway} after the transaction ends. Each test runs under both
 * Command-API modes (bulk enabled and disabled), so the matrix drives the
 * lookup through both of its read paths.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class TransactionFindAndUpdateTest extends RunwayBaseClientServerTest {

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
    public TransactionFindAndUpdateTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} on a
     * {@link Transaction} stages the update within the transaction, so the
     * update is invisible before the commit and durable after it.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} with
     * distinct codes, exactly one of which matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with {@code code == 2} and an
     * operator on {@code owner} that returns {@code "worker"}.</li>
     * <li>Query the {@link Item} through the enclosing {@link Runway} before
     * the commit, then {@code commit()} and query again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Item} carries
     * {@code owner == "worker"}, the pre-commit query observes the old owner,
     * and the post-commit query observes the new one.
     */
    @Test
    public void testFindUniqueAndUpdateStagesWithinTransaction() {
        runway.save(new Item(1), new Item(2), new Item(3));
        Item item;
        try (Transaction transaction = runway.stage()) {
            item = transaction.findUniqueAndUpdate(Item.class, code(2), "owner",
                    owner -> "worker");
            Assert.assertNotNull(item);
            Assert.assertEquals("worker", item.owner);
            Assert.assertEquals("unassigned",
                    runway.load(Item.class, item.id()).owner);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("worker", runway.load(Item.class, item.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} on a
     * {@link Transaction} returns {@code null} and never invokes the operator
     * when no record matches.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items}, none of
     * which matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with {@code code == 99} and an
     * operator that records that it ran, then {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}, the operator never
     * runs, and the {@link Transaction} commits.
     */
    @Test
    public void testFindUniqueAndUpdateReturnsNullAndSkipsOperatorWhenNoMatch() {
        runway.save(new Item(1), new Item(2), new Item(3));
        AtomicBoolean ran = new AtomicBoolean(false);
        try (Transaction transaction = runway.stage()) {
            Item item = transaction.findUniqueAndUpdate(Item.class, code(99),
                    "owner", owner -> {
                        ran.set(true);
                        return "worker";
                    });
            Assert.assertNull(item);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertFalse(ran.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} on a
     * {@link Transaction} throws {@link DuplicateEntryException} when more than
     * one record matches, without an update and without a poisoned transaction.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Item Items} that share the
     * same code.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with the shared code and catch
     * the expected exception.</li>
     * <li>{@code commit()} the same {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown,
     * the {@link Transaction} still commits, and both owners are unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateThrowsWhenMultipleMatch() {
        Item one = new Item(7);
        Item two = new Item(7);
        runway.save(one, two);
        try (Transaction transaction = runway.stage()) {
            boolean threw = false;
            try {
                transaction.findUniqueAndUpdate(Item.class, code(7), "owner",
                        owner -> "worker");
            }
            catch (DuplicateEntryException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("unassigned",
                runway.load(Item.class, one.id()).owner);
        Assert.assertEquals("unassigned",
                runway.load(Item.class, two.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} on a
     * {@link Transaction} matches a record that was saved within the same
     * transaction and is not yet durable.
     * <p>
     * <strong>Start state:</strong> No saved {@link Item Items}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction} and save a new {@link Item} within
     * it.</li>
     * <li>Call {@code findUniqueAndUpdate} on the same {@link Transaction} for
     * the staged {@link Item Item's} code, then {@code commit()}.</li>
     * <li>Re-load the {@link Item} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The staged {@link Item} is matched and
     * updated, and the re-loaded {@link Item} persists the new owner.
     */
    @Test
    public void testFindUniqueAndUpdateSeesRecordStagedWithinTransaction() {
        Item staged = new Item(9);
        try (Transaction transaction = runway.stage()) {
            transaction.save(staged);
            Item item = transaction.findUniqueAndUpdate(Item.class, code(9),
                    "owner", owner -> "worker");
            Assert.assertNotNull(item);
            Assert.assertEquals(staged.id(), item.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("worker",
                runway.load(Item.class, staged.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that an update operator that returns
     * {@code null} is rejected before anything is staged, and that the
     * rejection does not poison the open {@link Transaction}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Item}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with an operator that returns
     * {@code null} and catch the expected exception.</li>
     * <li>{@code commit()} the same {@link Transaction}.</li>
     * <li>Re-load the {@link Item} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown,
     * the {@link Transaction} still commits, and the owner is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRejectsNullReplacement() {
        Item item = new Item(1);
        runway.save(item);
        try (Transaction transaction = runway.stage()) {
            boolean threw = false;
            try {
                transaction.findUniqueAndUpdate(Item.class, code(1), "owner",
                        owner -> null);
            }
            catch (IllegalArgumentException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("unassigned",
                runway.load(Item.class, item.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that an update operator cannot end the
     * {@link Transaction}: a lifecycle call from within the operator is
     * refused, so the update can never escape the transaction and persist
     * through the enclosing {@link Runway}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Item} that matches the
     * criteria and an open {@link Transaction}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueAndUpdate} on the {@link Transaction} with an
     * operator that calls {@code abort()}, and catch the expected
     * exception.</li>
     * <li>{@code commit()} the same {@link Transaction}.</li>
     * <li>Re-load the {@link Item} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown,
     * the {@link Transaction} remains open and commits, and the owner is
     * unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRefusesOperatorThatEndsTransaction() {
        Item item = new Item(1);
        runway.save(item);
        try (Transaction transaction = runway.stage()) {
            boolean threw = false;
            try {
                transaction.findUniqueAndUpdate(Item.class, code(1), "owner",
                        owner -> {
                            transaction.abort();
                            return "worker";
                        });
            }
            catch (IllegalStateException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("unassigned",
                runway.load(Item.class, item.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} on a
     * {@link Transaction} that already ended runs atomically against the
     * enclosing {@link Runway} and is durable when the call returns.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} and a
     * {@link Transaction} that has committed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction} and {@code commit()} it immediately.</li>
     * <li>Call {@code findUniqueAndUpdate} on the ended
     * {@link Transaction}.</li>
     * <li>Re-load the {@link Item} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Item} carries the new
     * owner and the re-loaded {@link Item} persists it.
     */
    @Test
    public void testFindUniqueAndUpdateResumesAgainstRunwayAfterTransactionEnds() {
        runway.save(new Item(1), new Item(2), new Item(3));
        Transaction transaction = runway.stage();
        Assert.assertTrue(transaction.commit());
        Item item = transaction.findUniqueAndUpdate(Item.class, code(2),
                "owner", owner -> "worker");
        Assert.assertNotNull(item);
        Assert.assertEquals("worker", item.owner);
        Assert.assertEquals("worker", runway.load(Item.class, item.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} on a
     * {@link Transaction} updates the first match under the supplied order and
     * leaves the later matches unchanged.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Item Items} that all
     * match the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Call {@code findFirstAndUpdate} on it with an ascending code order,
     * then {@code commit()}.</li>
     * <li>Re-load the {@link Item Items} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Item} with the lowest code carries
     * the new owner and the others are unchanged.
     */
    @Test
    public void testFindFirstAndUpdateUpdatesFirstMatchUnderOrder() {
        Item one = new Item(1);
        Item two = new Item(2);
        Item three = new Item(3);
        runway.save(one, two, three);
        try (Transaction transaction = runway.stage()) {
            Item item = transaction.findFirstAndUpdate(Item.class,
                    owner("unassigned"), Order.by("code").ascending(), "owner",
                    owner -> "worker");
            Assert.assertNotNull(item);
            Assert.assertEquals(one.id(), item.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("worker", runway.load(Item.class, one.id()).owner);
        Assert.assertEquals("unassigned",
                runway.load(Item.class, two.id()).owner);
        Assert.assertEquals("unassigned",
                runway.load(Item.class, three.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyUniqueAndUpdate} on a
     * {@link Transaction} matches across the class hierarchy.
     * <p>
     * <strong>Start state:</strong> One saved {@link SpecialItem}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Call {@code findAnyUniqueAndUpdate} on it through the {@link Item}
     * hierarchy, then {@code commit()}.</li>
     * <li>Re-load the {@link SpecialItem} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Record} is the
     * {@link SpecialItem} and the re-loaded {@link SpecialItem} persists the
     * new owner.
     */
    @Test
    public void testFindAnyUniqueAndUpdateMatchesAcrossHierarchy() {
        SpecialItem special = new SpecialItem(5);
        runway.save(special);
        try (Transaction transaction = runway.stage()) {
            Item item = transaction.findAnyUniqueAndUpdate(Item.class, code(5),
                    "owner", owner -> "worker");
            Assert.assertNotNull(item);
            Assert.assertEquals(special.id(), item.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("worker",
                runway.load(SpecialItem.class, special.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyFirstAndUpdate} on a
     * {@link Transaction} updates the first match under the order across the
     * class hierarchy.
     * <p>
     * <strong>Start state:</strong> One saved {@link SpecialItem} and one saved
     * {@link Item}, both of which match the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Call {@code findAnyFirstAndUpdate} on it with an ascending code
     * order, then {@code commit()}.</li>
     * <li>Re-load both records from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link SpecialItem}, which is first by
     * code, carries the new owner and the {@link Item} is unchanged.
     */
    @Test
    public void testFindAnyFirstAndUpdateUpdatesFirstMatchAcrossHierarchy() {
        SpecialItem special = new SpecialItem(1);
        Item item = new Item(2);
        runway.save(special, item);
        try (Transaction transaction = runway.stage()) {
            Item updated = transaction.findAnyFirstAndUpdate(Item.class,
                    owner("unassigned"), Order.by("code").ascending(), "owner",
                    owner -> "worker");
            Assert.assertNotNull(updated);
            Assert.assertEquals(special.id(), updated.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("worker",
                runway.load(SpecialItem.class, special.id()).owner);
        Assert.assertEquals("unassigned",
                runway.load(Item.class, item.id()).owner);
    }

    /**
     * Return a {@link Criteria} that matches every {@link Item} whose
     * {@code code} equals the given {@code value}.
     *
     * @param value the code to match
     * @return the {@code code == value} {@link Criteria}
     */
    private static Criteria code(int value) {
        return Criteria.where().key("code").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * Return a {@link Criteria} that matches every {@link Item} whose
     * {@code owner} equals the given {@code value}.
     *
     * @param value the owner to match
     * @return the {@code owner == value} {@link Criteria}
     */
    private static Criteria owner(String value) {
        return Criteria.where().key("owner").operator(Operator.EQUALS)
                .value(value).build();
    }

}
