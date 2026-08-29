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
package com.cinchapi.runway.access;

import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Transaction;

/**
 * Tests that a refused {@code create} or {@code intern} through an
 * {@link Audience} leaves every binding as it was before the call.
 *
 * @author Jeff Nelson
 */
public class AudienceBindingRestoreTest extends AudienceAccessControlBaseTest {

    /**
     * Return a saved {@link Owner} that is bound to the test {@link #runway}.
     *
     * @param label the display label
     * @return the {@link Owner}
     */
    private Owner createOwner(String label) {
        Owner owner = new Owner();
        owner.label = label;
        owner.assign(runway);
        Assert.assertTrue(owner.save());
        return owner;
    }

    /**
     * <strong>Goal:</strong> Verify that a refused {@code create} through an
     * anonymous {@link Audience} that holds an open {@link Transaction} leaves
     * a caller-owned constructor argument bound as it was, so a later direct
     * save persists through its original binding.
     * <p>
     * <strong>Start state:</strong> One saved {@link Owner} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Call {@code create(Secret.class, owner)} through
     * {@code Audience.anonymous(transaction)} and catch the expected
     * refusal.</li>
     * <li>Change the {@link Owner} and {@code save()} it directly.</li>
     * <li>Load the {@link Owner} through the {@link #runway} while the
     * transaction is still open, then {@code abort()} the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save is visible outside the transaction
     * immediately, and the abort does not discard it.
     */
    @Test
    public void testRefusedAnonymousCreateRestoresArgumentBindingWithinHeldTransaction() {
        Owner owner = createOwner("acme");
        try (Transaction transaction = runway.startTransaction()) {
            Audience anonymous = Audience.anonymous(transaction);
            try {
                anonymous.create(Secret.class, owner);
                Assert.fail("Expected a RestrictedAccessException");
            }
            catch (RestrictedAccessException e) {
                // expected
            }
            owner.label = "changed";
            Assert.assertTrue(owner.save());
            Assert.assertEquals("changed",
                    runway.load(Owner.class, owner.id()).label);
            transaction.abort();
        }
        Assert.assertEquals("changed",
                runway.load(Owner.class, owner.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a refused {@code create} through a
     * {@link Record} audience that operates within an open {@link Transaction}
     * leaves a caller-owned constructor argument bound as it was.
     * <p>
     * <strong>Start state:</strong> One saved {@link Owner} and one saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code startTransaction()} on the {@link Candidate}
     * audience.</li>
     * <li>Call {@code create(Secret.class, owner)} on the returned view and
     * catch the expected refusal.</li>
     * <li>Change the {@link Owner} and {@code save()} it directly.</li>
     * <li>Load the {@link Owner} through the {@link #runway} while the
     * transaction is still open, then {@code abort()} the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save is visible outside the transaction
     * immediately, and the abort does not discard it.
     */
    @Test
    public void testRefusedRecordAudienceCreateRestoresArgumentBindingWithinTransaction() {
        Owner owner = createOwner("acme");
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@example.com";
        candidate.assign(runway);
        Assert.assertTrue(candidate.save());
        try (Transaction transaction = candidate.startTransaction()) {
            try {
                transaction.create(Secret.class, owner);
                Assert.fail("Expected a RestrictedAccessException");
            }
            catch (RestrictedAccessException e) {
                // expected
            }
            owner.label = "changed";
            Assert.assertTrue(owner.save());
            Assert.assertEquals("changed",
                    runway.load(Owner.class, owner.id()).label);
            transaction.abort();
        }
        Assert.assertEquals("changed",
                runway.load(Owner.class, owner.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a refused {@code create} through an
     * anonymous {@link Audience} on the managed path leaves a caller-owned
     * constructor argument bound as it was, instead of bound to a discarded
     * transaction.
     * <p>
     * <strong>Start state:</strong> One saved {@link Owner} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code create(Secret.class, owner)} on
     * {@link Audience#anonymous()} and catch the expected refusal.</li>
     * <li>Read the {@link Owner Owner's} binding.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Owner} is still bound to the
     * {@link #runway}.
     */
    @Test
    public void testRefusedCreateOnManagedPathRestoresArgumentBinding() {
        Owner owner = createOwner("acme");
        try {
            Audience.anonymous().create(Secret.class, owner);
            Assert.fail("Expected a RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // expected
        }
        Assert.assertSame(runway, Reflection.get("binding", owner));
    }

    /**
     * <strong>Goal:</strong> Verify that a refused {@code intern} through an
     * anonymous {@link Audience} that holds an open {@link Transaction} leaves
     * the probe {@link Record Record's} binding as it was.
     * <p>
     * <strong>Start state:</strong> One saved {@link Owner} bound to the
     * {@link #runway}, referenced by an unsaved {@link Secret} probe.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Call {@code intern} on {@code Audience.anonymous(transaction)} with
     * the {@link Secret} probe and catch the expected refusal.</li>
     * <li>Change the {@link Owner} and {@code save()} it directly.</li>
     * <li>Load the {@link Owner} through the {@link #runway} while the
     * transaction is still open, then {@code abort()} the transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save is visible outside the transaction
     * immediately, and the abort does not discard it.
     */
    @Test
    public void testRefusedInternRestoresProbeBindingWithinHeldTransaction() {
        Owner owner = createOwner("acme");
        Secret probe = new Secret(owner);
        try (Transaction transaction = runway.startTransaction()) {
            Audience anonymous = Audience.anonymous(transaction);
            try {
                anonymous.intern(probe);
                Assert.fail("Expected a RestrictedAccessException");
            }
            catch (RestrictedAccessException e) {
                // expected
            }
            owner.label = "changed";
            Assert.assertTrue(owner.save());
            Assert.assertEquals("changed",
                    runway.load(Owner.class, owner.id()).label);
            transaction.abort();
        }
        Assert.assertEquals("changed",
                runway.load(Owner.class, owner.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@code intern} that finds a
     * duplicate keeps the probe bound to the transaction, consistent with an
     * unmediated {@code intern}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Call {@code intern} on an {@link Admin} audience with a probe that
     * duplicates the saved {@link Employer Employer's} identity.</li>
     * <li>Read the probe's binding.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The intern returns the saved {@link Employer}
     * and the probe is bound to the transaction.
     */
    @Test
    public void testInternKeepsProbeBoundWhenDuplicateFound() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.assign(runway);
        Assert.assertTrue(acme.save());
        Admin admin = new Admin();
        admin.name = "Root";
        admin.email = "root@example.com";
        admin.assign(runway);
        Assert.assertTrue(admin.save());
        try (Transaction transaction = runway.startTransaction()) {
            Reflection.call(transaction, "join", admin);
            Employer probe = new Employer();
            probe.name = "Acme";
            Employer interned = admin.intern(probe);
            Assert.assertEquals(acme.id(), interned.id());
            Assert.assertSame(transaction, Reflection.get("binding", probe));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a failed {@code intern} through a
     * {@link Record} audience leaves the probe {@link Record Record's} author
     * marker as it was, so a later direct save is not attributed to the
     * audience.
     * <p>
     * <strong>Start state:</strong> One saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} on the {@link Admin} audience with an
     * {@link Employer} probe whose {@link com.cinchapi.runway.Unique Unique}
     * field is {@code null} and catch the expected failure.</li>
     * <li>Read the probe's author marker.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The intern fails with an
     * {@link IllegalArgumentException} and the probe's author marker is still
     * {@code null}.
     */
    @Test
    public void testFailedInternRestoresAuthorMarker() {
        Admin admin = new Admin();
        admin.name = "Root";
        admin.email = "root@example.com";
        admin.assign(runway);
        Assert.assertTrue(admin.save());
        Employer probe = new Employer();
        try {
            admin.intern(probe);
            Assert.fail("Expected an IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            // expected
        }
        Assert.assertNull(Reflection.get("_author", probe));
    }

    /**
     * A caller-owned {@link Record} that is passed as a constructor argument.
     *
     * @author Jeff Nelson
     */
    protected static class Owner extends Record {

        /**
         * The display label.
         */
        public String label;

    }

    /**
     * A {@link Record} that no {@link Audience} may create.
     *
     * @author Jeff Nelson
     */
    protected static class Secret extends Record implements AccessControl {

        /**
         * The {@link Owner} the secret belongs to.
         */
        public Owner owner;

        /**
         * Construct a new instance.
         *
         * @param owner the {@link Owner} the secret belongs to
         */
        public Secret(Owner owner) {
            this.owner = owner;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

    }

}
