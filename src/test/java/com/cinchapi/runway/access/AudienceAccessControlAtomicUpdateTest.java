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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.Unique;

/**
 * Tests for the atomic {@code find*AndUpdate} operations performed through an
 * {@link Audience}, covering how each operation matches among the {@link Record
 * Records} visible to the {@link Audience}, how the write permission for the
 * updated key gates the update, and how the replacement is validated. The
 * operations run in the transactional scope of the {@link Audience}: they stage
 * within an open {@link Transaction} and otherwise commit in their own
 * transaction.
 *
 * @author Jeff Nelson
 */
public class AudienceAccessControlAtomicUpdateTest
        extends AudienceAccessControlBaseTest {

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} updates a key the {@link Audience} may write on a
     * visible {@link Record}, and that the update stages within the
     * {@link Audience Audience's} {@link Transaction}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer} and one saved
     * {@link EmployerUser} of that {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link EmployerUser} through a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it for the {@code description}
     * key.</li>
     * <li>Query the {@link Employer} through the enclosing
     * {@link com.cinchapi.runway.Runway Runway} before the commit, then
     * {@code commit()} and query again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Employer} carries the
     * replacement description, the pre-commit query observes the old
     * description, and the post-commit query observes the new one.
     */
    @Test
    public void testFindUniqueAndUpdateUpdatesWritableKey() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.email = "hr@acme.example.com";
        user.employer = acme;
        runway.save(acme, user);
        try (Transaction transaction = runway.stage()) {
            EmployerUser audience = transaction.load(EmployerUser.class,
                    user.id());
            Employer updated = audience.findUniqueAndUpdate(Employer.class,
                    name("Acme"), "description", description -> "new");
            Assert.assertNotNull(updated);
            Assert.assertEquals("new", updated.description);
            Assert.assertEquals("old",
                    runway.load(Employer.class, acme.id()).description);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("new",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} returns {@code null} and changes nothing when the
     * {@link Audience} may not write the key.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer} and one saved
     * {@link EmployerUser} of that {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link EmployerUser} through a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it for the {@code industry} key,
     * then {@code commit()}.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} and the industry is
     * unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateReturnsNullWhenKeyNotWritable() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.industry = "manufacturing";
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.email = "hr@acme.example.com";
        user.employer = acme;
        runway.save(acme, user);
        try (Transaction transaction = runway.stage()) {
            EmployerUser audience = transaction.load(EmployerUser.class,
                    user.id());
            Assert.assertNull(audience.findUniqueAndUpdate(Employer.class,
                    name("Acme"), "industry", industry -> "software"));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("manufacturing",
                runway.load(Employer.class, acme.id()).industry);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} returns {@code null} and changes nothing when the
     * {@link Audience} has no writable keys on the matched {@link Record}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Employer Employers} and
     * one saved {@link EmployerUser} of the second one.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the unrelated {@link EmployerUser} through a
     * {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it against the first
     * {@link Employer}, then {@code commit()}.</li>
     * <li>Re-load the first {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} and the description
     * is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateReturnsNullForUnrelatedAudience() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        Employer other = new Employer();
        other.name = "Other";
        EmployerUser user = new EmployerUser();
        user.name = "Outsider";
        user.email = "outsider@other.example.com";
        user.employer = other;
        runway.save(acme, other, user);
        try (Transaction transaction = runway.stage()) {
            EmployerUser audience = transaction.load(EmployerUser.class,
                    user.id());
            Assert.assertNull(audience.findUniqueAndUpdate(Employer.class,
                    name("Acme"), "description", description -> "hijacked"));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("old",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that a match that is not visible to the
     * {@link Audience} behaves as no match: the result is {@code null} and
     * nothing is updated.
     * <p>
     * <strong>Start state:</strong> One saved {@link Application} whose parties
     * do not include the acting {@link Candidate}, and one saved unrelated
     * {@link Candidate}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the unrelated {@link Candidate} through a
     * {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it for the {@code status} key,
     * then {@code commit()}.</li>
     * <li>Re-load the {@link Application} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} and the status is
     * unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateReturnsNullWhenMatchNotVisible() {
        Application application = application("Acme", "Engineer", "a");
        Candidate outsider = new Candidate();
        outsider.name = "Outsider";
        outsider.email = "outsider@example.com";
        runway.save(application, outsider);
        try (Transaction transaction = runway.stage()) {
            Candidate audience = transaction.load(Candidate.class,
                    outsider.id());
            Assert.assertNull(audience.findUniqueAndUpdate(Application.class,
                    status("submitted"), "status", status -> "hacked"));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("submitted",
                runway.load(Application.class, application.id()).status);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} returns {@code null} when no {@link Record} matches at
     * all.
     * <p>
     * <strong>Start state:</strong> One saved {@link Admin} and no saved
     * {@link Employer Employers}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Admin} through a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with a criteria that matches
     * nothing.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testFindUniqueAndUpdateReturnsNullWhenNoMatch() {
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@example.com";
        runway.save(admin);
        try (Transaction transaction = runway.stage()) {
            Admin audience = transaction.load(Admin.class, admin.id());
            Assert.assertNull(audience.findUniqueAndUpdate(Employer.class,
                    name("Ghost"), "description", description -> "x"));
            Assert.assertTrue(transaction.commit());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} bound to a
     * {@link com.cinchapi.runway.Runway Runway} performs the atomic update in
     * its own transaction, so the result is durable when the call returns.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer} and one saved
     * {@link EmployerUser} of that {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueAndUpdate} on the {@link EmployerUser} for the
     * {@code description} key.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Employer} carries the
     * replacement description and the re-loaded {@link Employer} persists it.
     */
    @Test
    public void testFindUniqueAndUpdateCommitsInOwnTransactionAgainstRunway() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.email = "hr@acme.example.com";
        user.employer = acme;
        runway.save(acme, user);
        Employer updated = user.findUniqueAndUpdate(Employer.class,
                name("Acme"), "description", description -> "new");
        Assert.assertNotNull(updated);
        Assert.assertEquals("new", updated.description);
        Assert.assertEquals("new",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} whose
     * {@link Transaction} already ended resumes the atomic update against the
     * enclosing {@link com.cinchapi.runway.Runway Runway}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer}, and one saved
     * {@link EmployerUser} that was loaded through a {@link Transaction} that
     * has committed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link EmployerUser} through a {@link Transaction} and
     * {@code commit()} it immediately.</li>
     * <li>Call {@code findUniqueAndUpdate} on the {@link EmployerUser}.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Employer} carries the
     * replacement description and the re-loaded {@link Employer} persists it.
     */
    @Test
    public void testFindUniqueAndUpdateResumesAgainstRunwayAfterTransactionEnds() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.email = "hr@acme.example.com";
        user.employer = acme;
        runway.save(acme, user);
        EmployerUser audience;
        try (Transaction transaction = runway.stage()) {
            audience = transaction.load(EmployerUser.class, user.id());
            Assert.assertTrue(transaction.commit());
        }
        Employer updated = audience.findUniqueAndUpdate(Employer.class,
                name("Acme"), "description", description -> "late");
        Assert.assertNotNull(updated);
        Assert.assertEquals("late",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that the anonymous {@link Audience}, which
     * has no transactional scope, does not support the atomic update
     * operations.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueAndUpdate} on {@link Audience#anonymous()} and
     * catch the expected exception.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link UnsupportedOperationException} is
     * thrown and the description is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateUnsupportedForAnonymousAudience() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        runway.save(acme);
        boolean threw = false;
        try {
            Audience.anonymous().findUniqueAndUpdate(Employer.class,
                    name("Acme"), "description", description -> "hijacked");
        }
        catch (UnsupportedOperationException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals("old",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that the first-match variants target the
     * first visible {@link Record} under the order, so a hidden record that is
     * globally first neither blocks the update nor receives it.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Application Applications}
     * for different {@link Employer Employers}, and one saved
     * {@link EmployerUser} of the second {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link EmployerUser} through a {@link Transaction}.</li>
     * <li>Call {@code findFirstAndUpdate} on it with an ascending order, so the
     * global first is the hidden {@link Application} and the first visible one
     * belongs to the {@link EmployerUser Employer's} own {@link Employer}, then
     * {@code commit()}.</li>
     * <li>Re-load both {@link Application Applications} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call updates the visible
     * {@link Application} and the hidden one is unchanged.
     */
    @Test
    public void testFindFirstAndUpdateTargetsFirstVisibleMatch() {
        Application alpha = application("Alpha", "Engineer", "a");
        Application beta = application("Beta", "Designer", "b");
        EmployerUser user = new EmployerUser();
        user.name = "Beta HR";
        user.email = "hr@beta.example.com";
        user.employer = beta.job.employer;
        runway.save(alpha, beta, user);
        try (Transaction transaction = runway.stage()) {
            EmployerUser audience = transaction.load(EmployerUser.class,
                    user.id());
            Application updated = audience.findFirstAndUpdate(Application.class,
                    status("submitted"), Order.by("coverLetter").ascending(),
                    "status", status -> "reviewed");
            Assert.assertNotNull(updated);
            Assert.assertEquals(beta.id(), updated.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("reviewed",
                runway.load(Application.class, beta.id()).status);
        Assert.assertEquals("submitted",
                runway.load(Application.class, alpha.id()).status);
    }

    /**
     * <strong>Goal:</strong> Verify that the unique variants evaluate
     * uniqueness among the {@link Record Records} visible to the
     * {@link Audience}, so a hidden record that also matches does not make the
     * result ambiguous.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Application Applications}
     * with the same status for different {@link Employer Employers}, and one
     * saved {@link EmployerUser} of the second {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link EmployerUser} through a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with a criteria that both
     * {@link Application Applications} match, then {@code commit()}.</li>
     * <li>Re-load both {@link Application Applications} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call updates the visible
     * {@link Application} and the hidden one is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateMatchesUniquelyAmongVisibleRecords() {
        Application alpha = application("Alpha", "Engineer", "a");
        Application beta = application("Beta", "Designer", "b");
        EmployerUser user = new EmployerUser();
        user.name = "Beta HR";
        user.email = "hr@beta.example.com";
        user.employer = beta.job.employer;
        runway.save(alpha, beta, user);
        try (Transaction transaction = runway.stage()) {
            EmployerUser audience = transaction.load(EmployerUser.class,
                    user.id());
            Application updated = audience.findUniqueAndUpdate(
                    Application.class, status("submitted"), "status",
                    status -> "reviewed");
            Assert.assertNotNull(updated);
            Assert.assertEquals(beta.id(), updated.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("reviewed",
                runway.load(Application.class, beta.id()).status);
        Assert.assertEquals("submitted",
                runway.load(Application.class, alpha.id()).status);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyUniqueAndUpdate} through
     * an {@link Audience} matches across the class hierarchy and updates a key
     * the {@link Audience} may write.
     * <p>
     * <strong>Start state:</strong> One saved {@link Candidate} and one saved
     * {@link Admin}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Admin} through a {@link Transaction}.</li>
     * <li>Call {@code findAnyUniqueAndUpdate} on it through the {@link User}
     * hierarchy for the {@code phone} key, then {@code commit()}.</li>
     * <li>Re-load the {@link Candidate} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Record} is the
     * {@link Candidate} and the re-loaded {@link Candidate} persists the phone.
     */
    @Test
    public void testFindAnyUniqueAndUpdateUpdatesAcrossHierarchy() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@example.com";
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@example.com";
        runway.save(candidate, admin);
        try (Transaction transaction = runway.stage()) {
            Admin audience = transaction.load(Admin.class, admin.id());
            User updated = audience.findAnyUniqueAndUpdate(User.class,
                    email("jane@example.com"), "phone", phone -> "555-0100");
            Assert.assertNotNull(updated);
            Assert.assertEquals(candidate.id(), updated.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("555-0100",
                runway.load(Candidate.class, candidate.id()).phone);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyFirstAndUpdate} through
     * an {@link Audience} updates the first match under the order.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Candidate Candidates} that
     * share a location, and one saved {@link Admin}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Admin} through a {@link Transaction}.</li>
     * <li>Call {@code findAnyFirstAndUpdate} on it through the {@link User}
     * hierarchy with an ascending email order for the {@code phone} key, then
     * {@code commit()}.</li>
     * <li>Re-load both {@link Candidate Candidates} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first {@link Candidate} by email carries
     * the phone and the second is unchanged.
     */
    @Test
    public void testFindAnyFirstAndUpdateUpdatesFirstMatch() {
        Candidate first = new Candidate();
        first.name = "Amy";
        first.email = "amy@example.com";
        first.location = "Atlanta";
        Candidate second = new Candidate();
        second.name = "Bob";
        second.email = "bob@example.com";
        second.location = "Atlanta";
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@example.com";
        runway.save(first, second, admin);
        try (Transaction transaction = runway.stage()) {
            Admin audience = transaction.load(Admin.class, admin.id());
            User updated = audience.findAnyFirstAndUpdate(User.class,
                    Criteria.where().key("location").operator(Operator.EQUALS)
                            .value("Atlanta").build(),
                    Order.by("email").ascending(), "phone",
                    phone -> "555-0100");
            Assert.assertNotNull(updated);
            Assert.assertEquals(first.id(), updated.id());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("555-0100",
                runway.load(Candidate.class, first.id()).phone);
        Assert.assertNull(runway.load(Candidate.class, second.id()).phone);
    }

    /**
     * <strong>Goal:</strong> Verify that an update operator that returns
     * {@code null} is rejected before anything is written, and that the
     * rejection does not poison the {@link Audience Audience's} open
     * {@link Transaction}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Memo} and one saved
     * {@link Candidate}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Candidate} through a {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with an operator that returns
     * {@code null} and catch the expected exception.</li>
     * <li>{@code commit()} the same {@link Transaction}.</li>
     * <li>Re-load the {@link Memo} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown,
     * the {@link Transaction} still commits, and the body is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRejectsNullReplacement() {
        Memo memo = new Memo();
        memo.slug = "m1";
        memo.body = "old";
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@example.com";
        runway.save(memo, candidate);
        try (Transaction transaction = runway.stage()) {
            Candidate audience = transaction.load(Candidate.class,
                    candidate.id());
            boolean threw = false;
            try {
                audience.findUniqueAndUpdate(Memo.class, slug("m1"), "body",
                        body -> null);
            }
            catch (IllegalArgumentException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("old", runway.load(Memo.class, memo.id()).body);
    }

    /**
     * <strong>Goal:</strong> Verify that an update operator running through an
     * {@link Audience} cannot end the {@link Audience Audience's} open
     * {@link Transaction}: the lifecycle call is refused, so the update can
     * never escape the transaction and persist through the enclosing
     * {@link com.cinchapi.runway.Runway Runway}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer}, one saved
     * {@link EmployerUser} of that {@link Employer} and an open
     * {@link Transaction}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link EmployerUser} through the {@link Transaction}.</li>
     * <li>Call {@code findUniqueAndUpdate} on it with an operator that calls
     * {@code abort()} on the {@link Transaction}, and catch the expected
     * exception.</li>
     * <li>{@code commit()} the same {@link Transaction}.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown,
     * the {@link Transaction} remains open and commits, and the description is
     * unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRefusesOperatorThatEndsTransaction() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.email = "hr@acme.example.com";
        user.employer = acme;
        runway.save(acme, user);
        try (Transaction transaction = runway.stage()) {
            EmployerUser audience = transaction.load(EmployerUser.class,
                    user.id());
            boolean threw = false;
            try {
                audience.findUniqueAndUpdate(Employer.class, name("Acme"),
                        "description", description -> {
                            transaction.abort();
                            return "new";
                        });
            }
            catch (IllegalStateException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("old",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that a key that does not name an intrinsic
     * field is rejected instead of stored as a dynamic attribute.
     * <p>
     * <strong>Start state:</strong> One saved {@link Memo} and one saved
     * {@link Candidate}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueAndUpdate} on the {@link Candidate} with a
     * misspelled key and catch the expected exception.</li>
     * <li>Re-load the {@link Memo} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the body is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRejectsNonIntrinsicKey() {
        Memo memo = new Memo();
        memo.slug = "m1";
        memo.body = "old";
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@example.com";
        runway.save(memo, candidate);
        boolean threw = false;
        try {
            candidate.findUniqueAndUpdate(Memo.class, slug("m1"), "bodyy",
                    body -> "x");
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals("old", runway.load(Memo.class, memo.id()).body);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Unique} key is not eligible
     * for the atomic update operations through an {@link Audience}, the same as
     * on every other surface.
     * <p>
     * <strong>Start state:</strong> One saved {@link Memo} and one saved
     * {@link Candidate}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueAndUpdate} on the {@link Candidate} for the
     * {@link Unique} {@code slug} key and catch the expected exception.</li>
     * <li>Re-load the {@link Memo} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the slug is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRejectsUniqueKey() {
        Memo memo = new Memo();
        memo.slug = "m1";
        memo.body = "old";
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@example.com";
        runway.save(memo, candidate);
        boolean threw = false;
        try {
            candidate.findUniqueAndUpdate(Memo.class, slug("m1"), "slug",
                    slug -> "m2");
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals("m1", runway.load(Memo.class, memo.id()).slug);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findFirstAndUpdate} through an
     * {@link Audience} refuses a {@code null} {@link Order} with the same
     * exception as the other surfaces.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer} and one saved
     * {@link EmployerUser} of that {@link Employer}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findFirstAndUpdate} on the {@link EmployerUser} with a
     * {@code null} {@link Order} and catch the expected exception.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the description is unchanged.
     */
    @Test
    public void testFindFirstAndUpdateRequiresOrder() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.email = "hr@acme.example.com";
        user.employer = acme;
        runway.save(acme, user);
        boolean threw = false;
        try {
            user.findFirstAndUpdate(Employer.class, name("Acme"), null,
                    "description", description -> "new");
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals("old",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * Return a saved-ready {@link Application} for a new {@link Candidate}
     * against a new {@link Job} at a new {@link Employer} with the given
     * {@code employerName}.
     *
     * @param employerName the name of the {@link Employer}
     * @param jobTitle the title of the {@link Job}
     * @param coverLetter the cover letter, which the tests use for ordering
     * @return the unsaved {@link Application}
     */
    private static Application application(String employerName, String jobTitle,
            String coverLetter) {
        Employer employer = new Employer();
        employer.name = employerName;
        Job job = new Job();
        job.title = jobTitle;
        job.employer = employer;
        Candidate candidate = new Candidate();
        candidate.name = coverLetter + " candidate";
        candidate.email = coverLetter + "@example.com";
        Application application = new Application();
        application.candidate = candidate;
        application.job = job;
        application.coverLetter = coverLetter;
        return application;
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
     * Return a {@link Criteria} that matches every {@link Application} whose
     * {@code status} equals the given {@code value}.
     *
     * @param value the status to match
     * @return the {@code status == value} {@link Criteria}
     */
    private static Criteria status(String value) {
        return Criteria.where().key("status").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * Return a {@link Criteria} that matches every {@link Memo} whose
     * {@code slug} equals the given {@code value}.
     *
     * @param value the slug to match
     * @return the {@code slug == value} {@link Criteria}
     */
    private static Criteria slug(String value) {
        return Criteria.where().key("slug").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * A plain {@link Record} with no {@link AccessControl} rules, so the update
     * validation, rather than an access gate, decides each atomic update.
     *
     * @author Jeff Nelson
     */
    public static class Memo extends Record {

        /**
         * The identity slug.
         */
        @Unique
        public String slug;

        /**
         * The body copy.
         */
        public String body;

    }

}
