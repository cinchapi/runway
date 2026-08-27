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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Lists;

/**
 * Tests for {@link Runway#intern(Record) intern}, its
 * {@link TransactionInterface#intern(Record) TransactionInterface} counterpart
 * and {@link Record#intern()}. The tests cover how a {@link Record Record's}
 * {@link Unique} constraints define the identity that the lookup and the create
 * converge on. Each test runs under both Command-API modes (bulk enabled and
 * disabled), so the tests exercise both read paths of the transactional find.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class InternTest extends RunwayBaseClientServerTest {

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
    public InternTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} saves and returns the
     * given {@link Record} itself when no record shares its identity.
     * <p>
     * <strong>Start state:</strong> No saved {@link User Users}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} with a new {@link User}.</li>
     * <li>Re-load the returned {@link User} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link User} is the same instance
     * that was passed in, the re-loaded {@link User} shows the persisted state
     * and exactly one {@link User} exists.
     */
    @Test
    public void testInternSavesAndReturnsSameRecordWhenNoMatch() {
        User user = new User("ann@example.com", "Ann");
        User interned = runway.intern(user);
        Assert.assertSame(user, interned);
        User loaded = runway.load(User.class, user.id());
        Assert.assertEquals("ann@example.com", loaded.email);
        Assert.assertEquals("Ann", loaded.name);
        Assert.assertEquals(1, runway.count(User.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} returns the existing
     * {@link Record} when another record shares the given {@link Record
     * Record's} identity, even though the two differ in a non-identity field.
     * <p>
     * <strong>Start state:</strong> One saved {@link User}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link User} with a distinct name.</li>
     * <li>Call {@code intern} with a new {@link User} that has the same email
     * but a different name.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link User} has the saved
     * record's id and name, and no additional {@link User} exists.
     */
    @Test
    public void testInternReturnsExistingRecordWhenIdentityMatches() {
        User existing = new User("ann@example.com", "Ann");
        runway.save(existing);
        User probe = new User("ann@example.com", "Impostor");
        User interned = runway.intern(probe);
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals("Ann", interned.name);
        Assert.assertEquals(1, runway.count(User.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} of an already saved
     * {@link Record} returns its canonical stored state without creating
     * anything.
     * <p>
     * <strong>Start state:</strong> One saved {@link User}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link User}.</li>
     * <li>Call {@code intern} with the same instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link User} has the saved
     * record's id and exactly one {@link User} exists.
     */
    @Test
    public void testInternIsIdempotentForSavedRecord() {
        User user = new User("ann@example.com", "Ann");
        runway.save(user);
        User interned = runway.intern(user);
        Assert.assertEquals(user.id(), interned.id());
        Assert.assertEquals(1, runway.count(User.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a named compound {@link Unique}
     * constraint matches as one identity, so a record that agrees on every
     * member field is returned instead of created.
     * <p>
     * <strong>Start state:</strong> One saved {@link Point}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Point} at (1, 2) with a distinct label.</li>
     * <li>Call {@code intern} with a new {@link Point} at (1, 2) and a
     * different label.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Point} has the saved
     * record's id and label, and no additional {@link Point} exists.
     */
    @Test
    public void testInternMatchesCompoundConstraintWhenAllFieldsAgree() {
        Point existing = new Point(1, 2, "origin");
        runway.save(existing);
        Point probe = new Point(1, 2, "copy");
        Point interned = runway.intern(probe);
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals("origin", interned.label);
        Assert.assertEquals(1, runway.count(Point.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a partial agreement on a named
     * compound {@link Unique} constraint is not a match, so {@code intern}
     * creates a new {@link Record}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Point}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Point} at (1, 2).</li>
     * <li>Call {@code intern} with a new {@link Point} at (1, 3).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Point} is the new instance
     * and two {@link Point Points} exist.
     */
    @Test
    public void testInternCreatesWhenCompoundConstraintPartiallyDiffers() {
        runway.save(new Point(1, 2, "origin"));
        Point probe = new Point(1, 3, "other");
        Point interned = runway.intern(probe);
        Assert.assertSame(probe, interned);
        Assert.assertEquals(2, runway.count(Point.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} with several
     * independent {@link Unique} constraints is a match only when every
     * constraint agrees, and that a partial collision fails the create loudly
     * instead of returning the colliding record.
     * <p>
     * <strong>Start state:</strong> One saved {@link Account}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Account} with a distinct email and handle.</li>
     * <li>Call {@code intern} with a new {@link Account} that has the same
     * email but a different handle.</li>
     * <li>Catch the expected exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link SuppressedRunwayException} is thrown
     * and only the original {@link Account} exists.
     */
    @Test
    public void testInternFailsLoudlyOnPartialIdentityCollision() {
        Account existing = new Account("e@example.com", "handle1", "bio");
        runway.save(existing);
        Account probe = new Account("e@example.com", "handle2", "other");
        boolean threw = false;
        try {
            runway.intern(probe);
        }
        catch (SuppressedRunwayException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals(1, runway.count(Account.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} throws
     * {@link DuplicateEntryException} when more than one record shares the
     * identity, without creating another record.
     * <p>
     * <strong>Start state:</strong> Two saved {@link User Users} whose emails
     * are rewritten to the same value through the raw client, bypassing the
     * {@link Unique} enforcement that a save applies.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link User Users} with distinct emails.</li>
     * <li>Set both email values to the same address with
     * {@code client.set(...)}.</li>
     * <li>Call {@code intern} with a new {@link User} that has the shared
     * email, and catch the expected exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown
     * and exactly the two original {@link User Users} exist.
     */
    @Test
    public void testInternThrowsWhenMultipleRecordsShareIdentity() {
        User one = new User("a@example.com", "Ann");
        User two = new User("b@example.com", "Bea");
        runway.save(one, two);
        client.set("email", "dup@example.com", one.id());
        client.set("email", "dup@example.com", two.id());
        boolean threw = false;
        try {
            runway.intern(new User("dup@example.com", "Probe"));
        }
        catch (DuplicateEntryException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals(2, runway.count(User.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} refuses a
     * {@link Record} whose class declares no {@link Unique} constraint.
     * <p>
     * <strong>Start state:</strong> No saved {@link Plain Plains}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} with a new {@link Plain}.</li>
     * <li>Catch the expected exception, then load every {@link Plain}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and no {@link Plain} exists in the database.
     */
    @Test
    public void testInternRefusesRecordWithoutUniqueConstraints() {
        boolean threw = false;
        try {
            runway.intern(new Plain("anonymous"));
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.load(Plain.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} refuses a
     * {@link Record} whose every {@link Unique} value is {@code null}, because
     * a {@code null} value does not participate in identity.
     * <p>
     * <strong>Start state:</strong> No saved {@link User Users}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} with a new {@link User} whose email is
     * {@code null}.</li>
     * <li>Catch the expected exception, then load every {@link User}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and no {@link User} exists in the database.
     */
    @Test
    public void testInternRefusesRecordWhoseUniqueValuesAreAllNull() {
        boolean threw = false;
        try {
            runway.intern(new User(null, "Ann"));
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.load(User.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code null} {@link Unique} value
     * does not participate in identity, so the lookup matches on the remaining
     * constraints.
     * <p>
     * <strong>Start state:</strong> One saved {@link Account}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Account} with an email and a handle.</li>
     * <li>Call {@code intern} with a new {@link Account} that has a
     * {@code null} email and the same handle.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Account} has the saved
     * record's id and no additional {@link Account} exists.
     */
    @Test
    public void testInternSkipsNullConstraintAndMatchesOnRemaining() {
        Account existing = new Account("e@example.com", "handle1", "bio");
        runway.save(existing);
        Account probe = new Account(null, "handle1", "other");
        Account interned = runway.intern(probe);
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals(1, runway.count(Account.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a sequence-valued {@link Unique}
     * constraint matches element-wise, so a record that shares any element with
     * an existing record is returned instead of created, consistent with the
     * collision that a save of it would raise.
     * <p>
     * <strong>Start state:</strong> One saved {@link Profile}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Profile} with aliases "a" and "b".</li>
     * <li>Call {@code intern} with a new {@link Profile} whose aliases are "b"
     * and "c".</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Profile} has the saved
     * record's id and no additional {@link Profile} exists.
     */
    @Test
    public void testInternMatchesSequenceConstraintOnAnyElementCollision() {
        Profile existing = new Profile("a", "b");
        runway.save(existing);
        Profile probe = new Profile("b", "c");
        Profile interned = runway.intern(probe);
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals(1, runway.count(Profile.class));
    }

    /**
     * <strong>Goal:</strong> Verify that, within a caller-owned
     * {@link Transaction}, {@code intern} stages the created record so it is
     * invisible outside the transaction until the commit and visible after it.
     * <p>
     * <strong>Start state:</strong> No saved {@link User Users}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#startTransaction()} in
     * a try-with-resources block.</li>
     * <li>Call {@code intern} on the transaction with a new {@link User}.</li>
     * <li>Query for the record through the enclosing {@link Runway} before the
     * commit, then {@code commit()} and query again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The pre-commit query observes no match, the
     * commit succeeds, and the post-commit query returns the created
     * {@link User}.
     */
    @Test
    public void testInternStagesCreateWithinOpenTransaction() {
        long id;
        try (Transaction transaction = runway.startTransaction()) {
            User user = new User("ann@example.com", "Ann");
            User interned = transaction.intern(user);
            Assert.assertSame(user, interned);
            Assert.assertNull(
                    runway.findUnique(User.class, email("ann@example.com")));
            Assert.assertTrue(transaction.commit());
            id = interned.id();
        }
        User visible = runway.findUnique(User.class, email("ann@example.com"));
        Assert.assertNotNull(visible);
        Assert.assertEquals(id, visible.id());
    }

    /**
     * <strong>Goal:</strong> Verify that an {@code intern} refusal for a
     * {@link Record} with no usable identity happens before anything is staged,
     * so the caller-owned {@link Transaction} remains usable.
     * <p>
     * <strong>Start state:</strong> No saved records.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#startTransaction()} in
     * a try-with-resources block.</li>
     * <li>Call {@code intern} with a new {@link Plain} and catch the expected
     * rejection.</li>
     * <li>Call {@code intern} with a new {@link User}, then {@code commit()}
     * the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The rejection is an
     * {@link IllegalArgumentException}, the later operation and the commit
     * succeed, and the {@link User} is durable while no {@link Plain} exists.
     */
    @Test
    public void testInternRefusalLeavesTransactionUsable() {
        try (Transaction transaction = runway.startTransaction()) {
            boolean threw = false;
            try {
                transaction.intern(new Plain("anonymous"));
            }
            catch (IllegalArgumentException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            transaction.intern(new User("ann@example.com", "Ann"));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertNotNull(
                runway.findUnique(User.class, email("ann@example.com")));
        Assert.assertTrue(runway.load(Plain.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#intern()} saves and
     * returns the {@link Record} itself when no record shares its identity.
     * <p>
     * <strong>Start state:</strong> No saved {@link User Users}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Assign a new {@link User} to the {@link Runway}.</li>
     * <li>Call {@code intern()} on the {@link User}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link User} is the same
     * instance, its state is persisted and exactly one {@link User} exists.
     */
    @Test
    public void testRecordInternSavesAndReturnsSameRecordWhenNoMatch() {
        User user = new User("ann@example.com", "Ann");
        user.assign(runway);
        User interned = user.intern();
        Assert.assertSame(user, interned);
        Assert.assertEquals("Ann", runway.load(User.class, user.id()).name);
        Assert.assertEquals(1, runway.count(User.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#intern()} returns the
     * existing {@link Record} when another record shares the calling
     * {@link Record Record's} identity.
     * <p>
     * <strong>Start state:</strong> One saved {@link User}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link User} with a distinct name.</li>
     * <li>Assign a new {@link User} with the same email but a different name to
     * the {@link Runway}, then call {@code intern()} on it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link User} has the saved
     * record's id and name, and no additional {@link User} exists.
     */
    @Test
    public void testRecordInternReturnsExistingRecordWhenIdentityMatches() {
        User existing = new User("ann@example.com", "Ann");
        runway.save(existing);
        User probe = new User("ann@example.com", "Impostor");
        probe.assign(runway);
        User interned = probe.intern();
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals("Ann", interned.name);
        Assert.assertEquals(1, runway.count(User.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#intern()} on a
     * {@link Record} bound to an open {@link Transaction} stages within it, so
     * the create is invisible outside the transaction until the commit and
     * visible after it.
     * <p>
     * <strong>Start state:</strong> No saved {@link User Users}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#startTransaction()} in
     * a try-with-resources block.</li>
     * <li>Create a {@link User} through the transaction, then call
     * {@code intern()} on it.</li>
     * <li>Query for the record through the enclosing {@link Runway} before the
     * commit, then {@code commit()} and query again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link User} is the same
     * instance, the pre-commit query observes no match, the commit succeeds,
     * and the post-commit query returns the created {@link User}.
     */
    @Test
    public void testRecordInternJoinsOpenTransactionWhenBound() {
        long id;
        try (Transaction transaction = runway.startTransaction()) {
            User user = transaction.create(User.class, "ann@example.com",
                    "Ann");
            User interned = user.intern();
            Assert.assertSame(user, interned);
            Assert.assertNull(
                    runway.findUnique(User.class, email("ann@example.com")));
            Assert.assertTrue(transaction.commit());
            id = interned.id();
        }
        User visible = runway.findUnique(User.class, email("ann@example.com"));
        Assert.assertNotNull(visible);
        Assert.assertEquals(id, visible.id());
    }

    /**
     * <strong>Goal:</strong> Verify the convergence guarantee: two threads that
     * race to intern the same identity both receive the same record, and only
     * one record exists afterwards.
     * <p>
     * <strong>Start state:</strong> No saved {@link RacingUser RacingUsers} and
     * a rendezvous latch that both workers must reach before either can
     * complete its create.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start two threads that, gated by a common latch, each call
     * {@code intern} with a new {@link RacingUser} that has the same email, and
     * capture any {@link Throwable} a worker throws.</li>
     * <li>Use a {@code beforeSave} rendezvous so both workers observe no match
     * and stage a create before either commits, so the create race is
     * guaranteed rather than schedule-dependent.</li>
     * <li>{@code join()} both threads, then count every {@link RacingUser} with
     * the email.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Neither worker hangs or fails, both threads
     * receive a non-null {@link RacingUser} with the same id, and exactly one
     * record exists.
     */
    @Test
    public void testInternConvergesConcurrentCallersOnOneRecord()
            throws InterruptedException {
        RacingUser.bothStagedCreate = new CountDownLatch(2);
        try {
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch go = new CountDownLatch(1);
            AtomicReference<RacingUser> result1 = new AtomicReference<>();
            AtomicReference<RacingUser> result2 = new AtomicReference<>();
            AtomicReference<Throwable> failure1 = new AtomicReference<>();
            AtomicReference<Throwable> failure2 = new AtomicReference<>();
            Thread t1 = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    result1.set(runway
                            .intern(new RacingUser("race@example.com", "One")));
                }
                catch (Throwable t) {
                    failure1.set(t);
                }
            });
            Thread t2 = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    result2.set(runway
                            .intern(new RacingUser("race@example.com", "Two")));
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
            Assert.assertEquals(1, runway.count(RacingUser.class));
        }
        finally {
            RacingUser.bothStagedCreate = null;
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a repeat {@code intern} of the same
     * identity within the same {@link Transaction} observes the staged create
     * instead of a second create, and that an abort discards the staged record.
     * <p>
     * <strong>Start state:</strong> No saved {@link User Users}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#startTransaction()} in
     * a try-with-resources block.</li>
     * <li>Call {@code intern} twice with two new {@link User Users} that share
     * the same email but have different names.</li>
     * <li>Leave the block without a commit, then load every {@link User}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same record id, the
     * second call returns the first {@link User User's} staged state, and no
     * {@link User} exists after the abort.
     */
    @Test
    public void testInternObservesStagedCreateWithinTransaction() {
        try (Transaction transaction = runway.startTransaction()) {
            User first = transaction.intern(new User("ann@example.com", "Ann"));
            User second = transaction
                    .intern(new User("ann@example.com", "Other"));
            Assert.assertEquals(first.id(), second.id());
            Assert.assertEquals("Ann", second.name);
        }
        Assert.assertTrue(runway.load(User.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a partial identity collision within a
     * caller-owned {@link Transaction} throws from the staged save and poisons
     * the transaction, so the duplicate can never commit.
     * <p>
     * <strong>Start state:</strong> One saved {@link Account}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Account} with a distinct email and handle.</li>
     * <li>Start a {@link Transaction} with {@link Runway#startTransaction()} in
     * a try-with-resources block.</li>
     * <li>Call {@code intern} with a new {@link Account} that has the same
     * email but a different handle, and catch the expected exception.</li>
     * <li>Attempt to {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link SuppressedRunwayException}, the commit attempt is refused with an
     * {@link IllegalStateException}, and only the original {@link Account}
     * exists after the abort.
     */
    @Test
    public void testInternPartialCollisionPoisonsTransaction() {
        runway.save(new Account("e@example.com", "handle1", "bio"));
        try (Transaction transaction = runway.startTransaction()) {
            boolean threw = false;
            try {
                transaction
                        .intern(new Account("e@example.com", "handle2", "x"));
            }
            catch (SuppressedRunwayException e) {
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
        Assert.assertEquals(1, runway.count(Account.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} resumes against the
     * enclosing {@link Runway} after the {@link Transaction} ends.
     * <p>
     * <strong>Start state:</strong> No saved {@link User Users} and a committed
     * {@link Transaction}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#startTransaction()} in
     * a try-with-resources block and {@code commit()} it immediately.</li>
     * <li>Call {@code intern} on the ended transaction with a new
     * {@link User}.</li>
     * <li>Query for the record through the enclosing {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The create persists directly through the
     * {@link Runway}: the query returns the interned {@link User} with the same
     * id.
     */
    @Test
    public void testInternResumesAgainstRunwayAfterTransactionEnds() {
        try (Transaction transaction = runway.startTransaction()) {
            Assert.assertTrue(transaction.commit());
            User user = transaction.intern(new User("ann@example.com", "Ann"));
            Assert.assertNotNull(user);
            User visible = runway.findUnique(User.class,
                    email("ann@example.com"));
            Assert.assertNotNull(visible);
            Assert.assertEquals(user.id(), visible.id());
        }
    }

    /**
     * Return a {@link Criteria} that matches every {@link User} whose
     * {@code email} equals the given {@code value}.
     *
     * @param value the email to match
     * @return the {@code email == value} {@link Criteria}
     */
    private static Criteria email(String value) {
        return Criteria.where().key("email").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * A {@link Record} whose identity is a single {@link Unique} email.
     *
     * @author Jeff Nelson
     */
    public static class User extends Record {

        /**
         * The identity email.
         */
        @Unique
        String email;

        /**
         * A non-identity display name.
         */
        String name;

        /**
         * Construct a new instance.
         *
         * @param email the identity email
         * @param name the display name
         */
        public User(String email, String name) {
            this.email = email;
            this.name = name;
        }
    }

    /**
     * A {@link Record} whose identity is a named compound {@link Unique}
     * constraint across {@code x} and {@code y}.
     *
     * @author Jeff Nelson
     */
    public static class Point extends Record {

        /**
         * The x coordinate of the compound identity.
         */
        @Unique(name = "coordinate")
        int x;

        /**
         * The y coordinate of the compound identity.
         */
        @Unique(name = "coordinate")
        int y;

        /**
         * A non-identity label.
         */
        String label;

        /**
         * Construct a new instance.
         *
         * @param x the x coordinate
         * @param y the y coordinate
         * @param label the label
         */
        public Point(int x, int y, String label) {
            this.x = x;
            this.y = y;
            this.label = label;
        }
    }

    /**
     * A {@link Record} with two independent {@link Unique} constraints.
     *
     * @author Jeff Nelson
     */
    public static class Account extends Record {

        /**
         * The first independent identity.
         */
        @Unique
        String email;

        /**
         * The second independent identity.
         */
        @Unique
        String handle;

        /**
         * A non-identity bio.
         */
        String bio;

        /**
         * Construct a new instance.
         *
         * @param email the identity email
         * @param handle the identity handle
         * @param bio the bio
         */
        public Account(String email, String handle, String bio) {
            this.email = email;
            this.handle = handle;
            this.bio = bio;
        }
    }

    /**
     * A {@link Record} with no {@link Unique} constraint, so it has no identity
     * to {@code intern} by.
     *
     * @author Jeff Nelson
     */
    public static class Plain extends Record {

        /**
         * A non-identity name.
         */
        String name;

        /**
         * Construct a new instance.
         *
         * @param name the name
         */
        public Plain(String name) {
            this.name = name;
        }
    }

    /**
     * A {@link Record} whose identity is a sequence-valued {@link Unique}
     * constraint.
     *
     * @author Jeff Nelson
     */
    public static class Profile extends Record {

        /**
         * The identity aliases.
         */
        @Unique
        List<String> aliases;

        /**
         * Construct a new instance.
         *
         * @param aliases the identity aliases
         */
        public Profile(String... aliases) {
            this.aliases = Lists.newArrayList(aliases);
        }
    }

    /**
     * A {@link User}-like {@link Record} whose {@code beforeSave} hook blocks
     * on {@link #bothStagedCreate} when the latch is set, so a test can force
     * two workers to observe no match and stage a create before either commits.
     *
     * @author Jeff Nelson
     */
    public static class RacingUser extends Record {

        /**
         * When non-null, every save rendezvouses on this latch before it
         * proceeds. The owning test must set the latch before it runs and clear
         * it afterwards.
         */
        static volatile CountDownLatch bothStagedCreate = null;

        /**
         * The identity email.
         */
        @Unique
        String email;

        /**
         * A non-identity display name.
         */
        String name;

        /**
         * Construct a new instance.
         *
         * @param email the identity email
         * @param name the display name
         */
        public RacingUser(String email, String name) {
            this.email = email;
            this.name = name;
        }

        @Override
        protected void beforeSave() {
            CountDownLatch latch = bothStagedCreate;
            if(latch != null) {
                latch.countDown();
                try {
                    if(!latch.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("Both workers should observe"
                                + " no match before either creates");
                    }
                }
                catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            else {
                // No rendezvous is active, so the save proceeds immediately.
            }
        }
    }

}
