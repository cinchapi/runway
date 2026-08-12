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

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Record.Revision;
import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.Unique;

/**
 * Tests for {@link Audience#intern(Record) intern} performed through an
 * {@link Audience}, covering how the create permission of the interned
 * {@link Record} and the visibility of an existing match gate the operation.
 *
 * @author Jeff Nelson
 */
public class AudienceAccessControlInternTest
        extends AudienceAccessControlBaseTest {

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} that is permitted
     * to create the {@link Record} interns it, so the record persists and the
     * same instance is returned.
     * <p>
     * <strong>Start state:</strong> No saved {@link Employer Employers} and an
     * {@link Admin} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Assign an {@link Admin} to the {@link com.cinchapi.runway.Runway
     * Runway}.</li>
     * <li>Call {@code intern} on the {@link Admin} with a new
     * {@link Employer}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Employer} is the same
     * instance, it is persisted and exactly one {@link Employer} exists.
     */
    @Test
    public void testInternSavesRecordWhenAudienceMayCreate() {
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.assign(runway);
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "widgets";
        Employer interned = admin.intern(acme);
        Assert.assertSame(acme, interned);
        Assert.assertEquals("widgets",
                runway.load(Employer.class, acme.id()).description);
        Assert.assertEquals(1, runway.count(Employer.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} through an
     * {@link Audience} returns the existing {@link Record} that shares the
     * identity, without a second create.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer} and an
     * {@link Admin} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Employer} with a distinct description.</li>
     * <li>Call {@code intern} on the {@link Admin} with a new {@link Employer}
     * that has the same name but a different description.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Employer} has the saved
     * record's id and description, and no additional {@link Employer} exists.
     */
    @Test
    public void testInternReturnsExistingRecordWhenIdentityMatches() {
        Employer existing = new Employer();
        existing.name = "Acme";
        existing.description = "widgets";
        runway.save(existing);
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.assign(runway);
        Employer probe = new Employer();
        probe.name = "Acme";
        probe.description = "other";
        Employer interned = admin.intern(probe);
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals("widgets", interned.description);
        Assert.assertEquals(1, runway.count(Employer.class));
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} that is not
     * permitted to create the {@link Record} cannot intern it, even before any
     * database work happens.
     * <p>
     * <strong>Start state:</strong> No saved {@link Employer Employers}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} on a {@link Candidate} with a new
     * {@link Employer}.</li>
     * <li>Catch the expected exception, then load every {@link Employer}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown
     * and no {@link Employer} exists in the database.
     */
    @Test
    public void testInternRefusedWhenAudienceMayNotCreate() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.assign(runway);
        Employer probe = new Employer();
        probe.name = "Acme";
        boolean threw = false;
        try {
            candidate.intern(probe);
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.load(Employer.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that the anonymous {@link Audience}, which
     * has no transactional scope, does not support {@code intern}, consistent
     * with the atomic update operations.
     * <p>
     * <strong>Start state:</strong> No saved {@link Employer Employers}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} on {@link Audience#anonymous()} with a new
     * {@link Employer}.</li>
     * <li>Catch the expected exception, then load every {@link Employer}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown and no {@link Employer} exists in the database.
     */
    @Test
    public void testInternUnsupportedForAnonymousAudience() {
        Employer probe = new Employer();
        probe.name = "Acme";
        boolean threw = false;
        try {
            Audience.anonymous().intern(probe);
        }
        catch (UnsupportedOperationException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.load(Employer.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} refuses an identity
     * that is claimed by an existing {@link Record} that is not visible to the
     * {@link Audience}, without changing anything.
     * <p>
     * <strong>Start state:</strong> One saved {@link Badge} and a
     * {@link Candidate} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Badge} that only an {@link Admin} may see.</li>
     * <li>Call {@code intern} on a {@link Candidate} with a new {@link Badge}
     * that has the same serial, and catch the expected exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown
     * and exactly the original {@link Badge} exists.
     */
    @Test
    public void testInternRefusedWhenExistingMatchNotVisible() {
        Badge existing = new Badge();
        existing.serial = "X-1";
        runway.save(existing);
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.assign(runway);
        Badge probe = new Badge();
        probe.serial = "X-1";
        boolean threw = false;
        try {
            candidate.intern(probe);
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals(1, runway.count(Badge.class));
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link Audience} is recorded as
     * the author of a {@link Record} that {@code intern} saves.
     * <p>
     * <strong>Start state:</strong> One saved {@link Admin} and no saved
     * {@link Employer Employers}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Admin}.</li>
     * <li>Call {@code intern} on the {@link Admin} with a new
     * {@link Employer}.</li>
     * <li>Audit the {@link Employer} and inspect the revision for the
     * {@code name} key.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The revision is attributed and its author is
     * the {@link Admin}.
     */
    @Test
    public void testInternStampsAudienceAsAuthorOfCreatedRecord() {
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@example.com";
        runway.save(admin);
        Employer acme = new Employer();
        acme.name = "Acme";
        Employer interned = admin.intern(acme);
        Assert.assertSame(acme, interned);
        Map<Timestamp, Map<String, Revision>> audit = acme.audit();
        Assert.assertFalse(audit.isEmpty());
        Timestamp firstSave = audit.keySet().iterator().next();
        Revision revision = audit.get(firstSave).get("name");
        Assert.assertNotNull(revision);
        Assert.assertTrue(revision.isAttributed());
        Assert.assertEquals(admin, revision.author());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} through an
     * {@link Audience} that is bound to an open {@link Transaction} stages the
     * create within it, so the record is invisible outside the transaction
     * until the commit and visible after it.
     * <p>
     * <strong>Start state:</strong> One saved {@link Admin} and no saved
     * {@link Employer Employers}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with
     * {@link com.cinchapi.runway.Runway#stage() stage} in a try-with-resources
     * block and load the {@link Admin} through it.</li>
     * <li>Call {@code intern} on the loaded {@link Admin} with a new
     * {@link Employer}.</li>
     * <li>Query for the {@link Employer} through the enclosing
     * {@link com.cinchapi.runway.Runway Runway} before the commit, then
     * {@code commit()} and query again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Employer} is the same
     * instance, the pre-commit query observes no match, the commit succeeds,
     * and the post-commit query returns the created {@link Employer}.
     */
    @Test
    public void testInternStagesCreateWithinAudienceOpenTransaction() {
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@example.com";
        runway.save(admin);
        long id;
        try (Transaction transaction = runway.stage()) {
            Admin audience = transaction.load(Admin.class, admin.id());
            Employer acme = new Employer();
            acme.name = "Acme";
            Employer interned = audience.intern(acme);
            Assert.assertSame(acme, interned);
            Assert.assertNull(runway.findUnique(Employer.class, name("Acme")));
            Assert.assertTrue(transaction.commit());
            id = interned.id();
        }
        Employer visible = runway.findUnique(Employer.class, name("Acme"));
        Assert.assertNotNull(visible);
        Assert.assertEquals(id, visible.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a hidden-match refusal of
     * {@code intern} through an {@link Audience} that is bound to an open
     * {@link Transaction} stages nothing, so the transaction remains usable.
     * <p>
     * <strong>Start state:</strong> One saved {@link Badge}, which only an
     * {@link Admin} may see, and one saved {@link Candidate}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with
     * {@link com.cinchapi.runway.Runway#stage() stage} in a try-with-resources
     * block and load the {@link Candidate} through it.</li>
     * <li>Call {@code intern} on the loaded {@link Candidate} with a new
     * {@link Badge} that has the same serial, and catch the expected
     * exception.</li>
     * <li>{@code commit()} the same {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown,
     * the {@link Transaction} still commits, and exactly the original
     * {@link Badge} exists.
     */
    @Test
    public void testInternHiddenMatchRefusalLeavesTransactionUsable() {
        Badge existing = new Badge();
        existing.serial = "X-1";
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@example.com";
        runway.save(existing, candidate);
        try (Transaction transaction = runway.stage()) {
            Candidate audience = transaction.load(Candidate.class,
                    candidate.id());
            Badge probe = new Badge();
            probe.serial = "X-1";
            boolean threw = false;
            try {
                audience.intern(probe);
            }
            catch (RestrictedAccessException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(1, runway.count(Badge.class));
    }

    /**
     * Return a {@link Criteria} that matches every {@link Employer} whose
     * {@code name} equals the given {@code value}.
     *
     * @param value the name to match
     * @return the {@code name == value} {@link Criteria}
     */
    private static Criteria name(String value) {
        return Criteria.where().key("name").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * An access controlled {@link Record} with a {@link Unique} identity that
     * any {@link Audience} may create but only an {@link Admin} may see.
     *
     * @author Jeff Nelson
     */
    public static class Badge extends Record implements AccessControl {

        /**
         * The identity serial.
         */
        @Unique
        public String serial;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return true;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return audience instanceof Admin ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return audience instanceof Admin ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }
    }

}
