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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record.ConstraintViolationException;
import com.google.common.collect.Lists;

/**
 * Tests for {@link Runway#intern(Record) intern}, its
 * {@link TransactionInterface#intern(Record) TransactionInterface} counterpart
 * and {@link Record#intern()}. The tests cover how a {@link Record Record's}
 * {@link Unique} constraints define the identity that the lookup and the create
 * converge on.
 *
 * @author Jeff Nelson
 */
public class InternTest extends RunwayBaseClientServerTest {

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
     * <strong>Expected:</strong> A {@link ConstraintViolationException} is
     * thrown and only the original {@link Account} exists.
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
        catch (ConstraintViolationException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals(1, runway.count(Account.class));
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
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
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
        try (Transaction transaction = runway.stage()) {
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
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
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
        try (Transaction transaction = runway.stage()) {
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
     * <li>Start a {@link Transaction} with {@link Runway#stage()} in a
     * try-with-resources block.</li>
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
        try (Transaction transaction = runway.stage()) {
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

}
