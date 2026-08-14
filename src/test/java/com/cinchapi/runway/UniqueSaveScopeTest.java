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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Random;

/**
 * Tests for the scope of save-time {@link Unique} enforcement: which records
 * count as candidates for a violation, and which concurrent writers the check
 * conflicts with.
 * <p>
 * Each test runs under both Command-API modes (bulk enabled and disabled), so
 * the tests exercise both save-time read paths.
 * </p>
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class UniqueSaveScopeTest extends RunwayBaseClientServerTest {

    /**
     * Return the parameter matrix that runs each test once per Command-API
     * mode.
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
    public UniqueSaveScopeTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that a save that enforces a {@link Unique}
     * constraint commits alongside the concurrent creation of an unrelated
     * {@link Record} of the same class.
     * <p>
     * <strong>Start state:</strong> No saved {@link Account Accounts}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and save an {@link Account} through it,
     * so the uniqueness check joins the transaction's conflict footprint.</li>
     * <li>Save a second {@link Account} with a different address outside the
     * transaction, which commits before the transaction does.</li>
     * <li>Call {@code commit()} on the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both the outside save and the commit succeed,
     * and both {@link Account Accounts} are stored.
     */
    @Test
    public void testCommitSucceedsWhenUnrelatedRecordOfSameClassIsCreatedConcurrently() {
        try (Transaction transaction = runway.transaction()) {
            transaction.save(new Account(Random.getSimpleString()));
            Account unrelated = new Account(Random.getSimpleString());
            unrelated.assign(runway);
            Assert.assertTrue(unrelated.save());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(2, runway.count(Account.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a save that duplicates a
     * {@link Unique} value on a {@link Record} of the same class fails with the
     * constraint violation attributed to the duplicating {@link Record}.
     * <p>
     * <strong>Start state:</strong> No saved {@link Account Accounts}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Account} with an address.</li>
     * <li>Save a second {@link Account} with the same address.</li>
     * <li>Call {@code throwSupressedExceptions()} on the second {@link Account}
     * and catch the recorded violation.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second save fails, the violation names the
     * constrained key and the duplicating {@link Record Record's} class, and
     * only one {@link Account} is stored.
     */
    @Test
    public void testSaveFailsWhenDuplicateValueExistsInSameClass() {
        String address = Random.getSimpleString();
        Assert.assertTrue(runway.save(new Account(address)));
        Account duplicate = new Account(address);
        Assert.assertFalse(runway.save(duplicate));
        String message = null;
        try {
            duplicate.throwSupressedExceptions();
        }
        catch (SuppressedRunwayException e) {
            message = e.getMessage();
        }
        Assert.assertEquals(
                "address must be unique in " + Account.class.getName(),
                message);
        Assert.assertEquals(1, runway.count(Account.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a named compound {@link Unique}
     * constraint still fails a save that duplicates every member of the
     * constraint.
     * <p>
     * <strong>Start state:</strong> No saved {@link Booking Bookings}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Booking} with a room and a time.</li>
     * <li>Save a second {@link Booking} with the same room and time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second save fails and only one
     * {@link Booking} is stored.
     */
    @Test
    public void testSaveFailsWhenNamedCompoundConstraintIsDuplicated() {
        String room = Random.getSimpleString();
        String time = Random.getSimpleString();
        Assert.assertTrue(runway.save(new Booking(room, time)));
        Assert.assertFalse(runway.save(new Booking(room, time)));
        Assert.assertEquals(1, runway.count(Booking.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a candidate record that stores the
     * constrained value but no class is not a member of the constraint's scope
     * and does not fail the save.
     * <p>
     * <strong>Start state:</strong> A raw record that holds an address and
     * nothing else, written through a direct
     * {@link com.cinchapi.concourse.Concourse Concourse} connection so it
     * carries no class.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Add the address to a record through the raw client.</li>
     * <li>Save an {@link Account} with that same address.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the {@link Account} is
     * stored.
     */
    @Test
    public void testSaveSucceedsWhenCandidateStoresNoClass() {
        String address = Random.getSimpleString();
        client.add("address", address, Time.now());
        Account account = new Account(address);
        Assert.assertTrue(runway.save(account));
        Assert.assertEquals(1, runway.count(Account.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Unique} constraint with
     * {@code any = true} ignores a {@link Record} of a class outside the
     * hierarchy window that holds the same value.
     * <p>
     * <strong>Start state:</strong> No saved {@link Asset Assets} and no saved
     * {@link Gadget Gadgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Gadget}, which is outside the {@link Asset} hierarchy,
     * with a tag.</li>
     * <li>Save an {@link ImageAsset} with the same tag.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves succeed, the {@link Asset}
     * hierarchy holds one {@link Record}, and one {@link Gadget} is stored.
     */
    @Test
    public void testSaveSucceedsWhenAnyConstraintValueExistsOutsideHierarchyWindow() {
        String tag = Random.getSimpleString();
        Assert.assertTrue(runway.save(new Gadget(tag)));
        Assert.assertTrue(runway.save(new ImageAsset(tag)));
        Assert.assertEquals(1, runway.countAny(Asset.class));
        Assert.assertEquals(1, runway.count(Gadget.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a named compound {@link Unique}
     * constraint permits a save that agrees on one member and differs on
     * another.
     * <p>
     * <strong>Start state:</strong> No saved {@link Booking Bookings}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Booking} with a room and a time.</li>
     * <li>Save a second {@link Booking} with the same room and a different
     * time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves succeed and two {@link Booking
     * Bookings} are stored.
     */
    @Test
    public void testSaveSucceedsWhenNamedCompoundConstraintDiffersInOneMember() {
        String room = Random.getSimpleString();
        Assert.assertTrue(runway.save(new Booking(room, "09:00")));
        Assert.assertTrue(runway.save(new Booking(room, "10:00")));
        Assert.assertEquals(2, runway.count(Booking.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a class-scoped {@link Unique}
     * constraint ignores a {@link Record} of another class that holds the same
     * value under the same key.
     * <p>
     * <strong>Start state:</strong> No saved {@link Account Accounts} and no
     * saved {@link Ledger Ledgers}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Account} with an address.</li>
     * <li>Save a {@link Ledger} with the same address.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves succeed and each class stores one
     * {@link Record}.
     */
    @Test
    public void testSaveSucceedsWhenSameValueExistsInDifferentClass() {
        String address = Random.getSimpleString();
        Assert.assertTrue(runway.save(new Account(address)));
        Assert.assertTrue(runway.save(new Ledger(address)));
        Assert.assertEquals(1, runway.count(Account.class));
        Assert.assertEquals(1, runway.count(Ledger.class));
    }

    /**
     * A {@link Record} whose identity is a class-scoped {@link Unique} address.
     *
     * @author Jeff Nelson
     */
    public static class Account extends Record {

        /**
         * The identity address.
         */
        @Unique
        String address;

        /**
         * Construct a new instance.
         *
         * @param address the identity address
         */
        public Account(String address) {
            this.address = address;
        }
    }

    /**
     * An abstract {@link Record} whose identity is a {@link Unique} tag that
     * spans the class hierarchy.
     *
     * @author Jeff Nelson
     */
    public static abstract class Asset extends Record {

        /**
         * The identity tag, shared across every {@link Asset} subclass.
         */
        @Unique(any = true)
        String tag;

        /**
         * Construct a new instance.
         *
         * @param tag the identity tag
         */
        public Asset(String tag) {
            this.tag = tag;
        }
    }

    /**
     * A {@link Record} whose identity is a named compound {@link Unique}
     * constraint.
     *
     * @author Jeff Nelson
     */
    public static class Booking extends Record {

        /**
         * The room member of the constraint.
         */
        @Unique(name = "reservation")
        String room;

        /**
         * The time member of the constraint.
         */
        @Unique(name = "reservation")
        String time;

        /**
         * Construct a new instance.
         *
         * @param room the room member of the constraint
         * @param time the time member of the constraint
         */
        public Booking(String room, String time) {
            this.room = room;
            this.time = time;
        }
    }

    /**
     * A {@link Record} outside the {@link Asset} hierarchy that stores its own
     * tag under the same key.
     *
     * @author Jeff Nelson
     */
    public static class Gadget extends Record {

        /**
         * The identity tag.
         */
        @Unique
        String tag;

        /**
         * Construct a new instance.
         *
         * @param tag the identity tag
         */
        public Gadget(String tag) {
            this.tag = tag;
        }
    }

    /**
     * A concrete {@link Asset} subclass.
     *
     * @author Jeff Nelson
     */
    public static class ImageAsset extends Asset {

        /**
         * Construct a new instance.
         *
         * @param tag the identity tag
         */
        public ImageAsset(String tag) {
            super(tag);
        }
    }

    /**
     * A {@link Record} that is unrelated to {@link Account} and stores its own
     * address under the same key.
     *
     * @author Jeff Nelson
     */
    public static class Ledger extends Record {

        /**
         * The identity address.
         */
        @Unique
        String address;

        /**
         * Construct a new instance.
         *
         * @param address the identity address
         */
        public Ledger(String address) {
            this.address = address;
        }
    }

}
