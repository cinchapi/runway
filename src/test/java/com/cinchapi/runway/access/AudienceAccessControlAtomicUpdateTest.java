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

/**
 * Tests for the atomic {@code find*AndUpdate} operations performed through an
 * {@link Audience}, covering how the visibility of the matched {@link Record}
 * and the write permission for the updated key gate each operation.
 *
 * @author Jeff Nelson
 */
public class AudienceAccessControlAtomicUpdateTest
        extends AudienceAccessControlBaseTest {

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} updates a key the {@link Audience} may write on a
     * visible {@link Record}.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer} and an
     * {@link EmployerUser} of that {@link Employer} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Employer} with a distinct description.</li>
     * <li>Call {@code findUniqueAndUpdate} on the {@link EmployerUser} for the
     * {@code description} key.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Employer} carries the
     * replacement description and the re-loaded {@link Employer} persists it.
     */
    @Test
    public void testFindUniqueAndUpdateUpdatesWritableKey() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        runway.save(acme);
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.employer = acme;
        user.assign(runway);
        Employer updated = user.findUniqueAndUpdate(Employer.class,
                name("Acme"), "description", description -> "new");
        Assert.assertNotNull(updated);
        Assert.assertEquals("new", updated.description);
        Assert.assertEquals("new",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} refuses a key the {@link Audience} may not write, and
     * changes nothing.
     * <p>
     * <strong>Start state:</strong> One saved {@link Employer} and an
     * {@link EmployerUser} of that {@link Employer} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Employer} with a distinct industry.</li>
     * <li>Call {@code findUniqueAndUpdate} on the {@link EmployerUser} for the
     * {@code industry} key, and catch the expected exception.</li>
     * <li>Re-load the {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown
     * and the industry is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRefusedWhenKeyNotWritable() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.industry = "manufacturing";
        runway.save(acme);
        EmployerUser user = new EmployerUser();
        user.name = "HR Manager";
        user.employer = acme;
        user.assign(runway);
        boolean threw = false;
        try {
            user.findUniqueAndUpdate(Employer.class, name("Acme"), "industry",
                    industry -> "software");
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals("manufacturing",
                runway.load(Employer.class, acme.id()).industry);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} refuses an {@link Audience} with no writable keys on the
     * matched {@link Record}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Employer Employers} and an
     * {@link EmployerUser} of the second one bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the {@link Employer Employers}.</li>
     * <li>Call {@code findUniqueAndUpdate} on the unrelated
     * {@link EmployerUser} against the first {@link Employer}, and catch the
     * expected exception.</li>
     * <li>Re-load the first {@link Employer} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown
     * and the description is unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateRefusedForUnrelatedAudience() {
        Employer acme = new Employer();
        acme.name = "Acme";
        acme.description = "old";
        Employer other = new Employer();
        other.name = "Other";
        runway.save(acme, other);
        EmployerUser user = new EmployerUser();
        user.name = "Outsider";
        user.employer = other;
        user.assign(runway);
        boolean threw = false;
        try {
            user.findUniqueAndUpdate(Employer.class, name("Acme"),
                    "description", description -> "hijacked");
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals("old",
                runway.load(Employer.class, acme.id()).description);
    }

    /**
     * <strong>Goal:</strong> Verify that a match that is not visible to the
     * {@link Audience} behaves as no match: the result is {@code null} and
     * nothing is updated.
     * <p>
     * <strong>Start state:</strong> One saved {@link Application} whose parties
     * do not include the acting {@link Candidate}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Application} for another {@link Candidate}.</li>
     * <li>Call {@code findUniqueAndUpdate} on an unrelated {@link Candidate}
     * for the {@code status} key.</li>
     * <li>Re-load the {@link Application} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} and the status is
     * unchanged.
     */
    @Test
    public void testFindUniqueAndUpdateReturnsNullWhenMatchNotVisible() {
        Application application = application("Acme", "Engineer", "a");
        runway.save(application);
        Candidate outsider = new Candidate();
        outsider.name = "Outsider";
        outsider.email = "outsider@example.com";
        outsider.assign(runway);
        Application result = outsider.findUniqueAndUpdate(Application.class,
                status("submitted"), "status", status -> "hacked");
        Assert.assertNull(result);
        Assert.assertEquals("submitted",
                runway.load(Application.class, application.id()).status);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndUpdate} through an
     * {@link Audience} returns {@code null} when no {@link Record} matches at
     * all.
     * <p>
     * <strong>Start state:</strong> No saved {@link Employer Employers} and an
     * {@link Admin} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findUniqueAndUpdate} on the {@link Admin} with a criteria
     * that matches nothing.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testFindUniqueAndUpdateReturnsNullWhenNoMatch() {
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.assign(runway);
        Assert.assertNull(admin.findUniqueAndUpdate(Employer.class,
                name("Ghost"), "description", description -> "x"));
    }

    /**
     * <strong>Goal:</strong> Verify that the first-match variants gate the
     * {@link Record} that the operation would touch, so a hidden global first
     * behaves as no match even when a later visible record also matches.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Application Applications}
     * for different {@link Employer Employers}, and an {@link EmployerUser} of
     * the second {@link Employer} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Application Applications} whose cover letters order
     * them deterministically.</li>
     * <li>Call {@code findFirstAndUpdate} on the {@link EmployerUser} with an
     * ascending order, so the global first is the hidden
     * {@link Application}.</li>
     * <li>Call {@code findFirstAndUpdate} again with a descending order, so the
     * global first is the visible {@link Application}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The ascending call returns {@code null} and
     * changes nothing; the descending call updates the visible
     * {@link Application}.
     */
    @Test
    public void testFindFirstAndUpdateGatesTheRecordTheOperationWouldTouch() {
        Application alpha = application("Alpha", "Engineer", "a");
        Application beta = application("Beta", "Designer", "b");
        runway.save(alpha, beta);
        EmployerUser user = new EmployerUser();
        user.name = "Beta HR";
        user.employer = beta.job.employer;
        user.assign(runway);
        Application blocked = user.findFirstAndUpdate(Application.class,
                status("submitted"), Order.by("coverLetter").ascending(),
                "status", status -> "reviewed");
        Assert.assertNull(blocked);
        Assert.assertEquals("submitted",
                runway.load(Application.class, alpha.id()).status);
        Assert.assertEquals("submitted",
                runway.load(Application.class, beta.id()).status);
        Application updated = user.findFirstAndUpdate(Application.class,
                status("submitted"), Order.by("coverLetter").descending(),
                "status", status -> "reviewed");
        Assert.assertNotNull(updated);
        Assert.assertEquals(beta.id(), updated.id());
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
     * <strong>Start state:</strong> One saved {@link Candidate} and an
     * {@link Admin} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Candidate}.</li>
     * <li>Call {@code findAnyUniqueAndUpdate} on the {@link Admin} through the
     * {@link User} hierarchy for the {@code phone} key.</li>
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
        runway.save(candidate);
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.assign(runway);
        User updated = admin.findAnyUniqueAndUpdate(User.class,
                email("jane@example.com"), "phone", phone -> "555-0100");
        Assert.assertNotNull(updated);
        Assert.assertEquals(candidate.id(), updated.id());
        Assert.assertEquals("555-0100",
                runway.load(Candidate.class, candidate.id()).phone);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyFirstAndUpdate} through
     * an {@link Audience} updates the first match under the order.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Candidate Candidates} that
     * share a location, and an {@link Admin} bound to the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Candidate Candidates} with the same location and
     * ordered emails.</li>
     * <li>Call {@code findAnyFirstAndUpdate} on the {@link Admin} through the
     * {@link User} hierarchy with an ascending email order for the
     * {@code phone} key.</li>
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
        runway.save(first, second);
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.assign(runway);
        User updated = admin.findAnyFirstAndUpdate(User.class,
                Criteria.where().key("location").operator(Operator.EQUALS)
                        .value("Atlanta").build(),
                Order.by("email").ascending(), "phone", phone -> "555-0100");
        Assert.assertNotNull(updated);
        Assert.assertEquals(first.id(), updated.id());
        Assert.assertEquals("555-0100",
                runway.load(Candidate.class, first.id()).phone);
        Assert.assertNull(runway.load(Candidate.class, second.id()).phone);
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

}
