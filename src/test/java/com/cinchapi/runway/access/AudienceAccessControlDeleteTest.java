/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway.access;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link Audience#delete} method and related access control
 * behaviors.
 * <p>
 * This test suite focuses on the delete operation which enforces deletion
 * permissions.
 * </p>
 *
 * @author Jeff Nelson
 */
public class AudienceAccessControlDeleteTest extends AudienceAccessControlBaseTest {

    @Test
    public void testDeleteOperationOwnershipPermission() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";

        // Candidate should be able to delete themselves
        Assert.assertTrue("Candidate should be able to delete themselves",
                candidate.$isDeletableBy(candidate));

        // Simulate deletion (in real scenario, this would remove from database)
        candidate.deleteAs(candidate);
        // Test passes if no exception is thrown
    }

    @Test
    public void testDeleteOperationDeniedThrowsException() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";

        // One candidate should not be able to delete another
        Assert.assertFalse("Candidate should not be able to delete other candidates",
                candidate2.$isDeletableBy(candidate1));

        try {
            candidate1.delete(candidate2);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testDeleteOperationApplicationOwnership() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";

        Job job = new Job();
        job.title = "Software Engineer";

        Application application1 = new Application();
        application1.candidate = candidate1;
        application1.job = job;

        Application application2 = new Application();
        application2.candidate = candidate2;
        application2.job = job;

        // Candidate should be able to delete their own application
        Assert.assertTrue("Candidate should be able to delete their own application",
                application1.$isDeletableBy(candidate1));
        candidate1.delete(application1);

        // Candidate should not be able to delete another's application
        Assert.assertFalse("Candidate should not be able to delete other's application",
                application2.$isDeletableBy(candidate1));

        try {
            candidate1.delete(application2);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testDeleteOperationEmployerJobDeletion() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;

        // Employer user should be able to delete their company's jobs
        Assert.assertTrue("Employer should be able to delete their company's jobs",
                job.$isDeletableBy(employerUser));
        employerUser.delete(job);

        // Test with different company
        Employer otherCompany = new Employer();
        otherCompany.name = "OtherCorp";

        Job otherJob = new Job();
        otherJob.title = "Frontend Developer";
        otherJob.employer = otherCompany;

        Assert.assertFalse("Employer should not be able to delete other company's jobs",
                otherJob.$isDeletableBy(employerUser));

        try {
            employerUser.delete(otherJob);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testDeleteOperationAdminOverride() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Employer company = new Employer();
        company.name = "TechCorp";

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;

        // Admin should be able to delete any record
        Assert.assertTrue("Admin should be able to delete any user",
                candidate.$isDeletableBy(admin));
        Assert.assertTrue("Admin should be able to delete any employer",
                company.$isDeletableBy(admin));
        Assert.assertTrue("Admin should be able to delete any job",
                job.$isDeletableBy(admin));
        Assert.assertTrue("Admin should be able to delete any application",
                application.$isDeletableBy(admin));

        admin.delete(candidate);
        admin.delete(company);
        admin.delete(job);
        admin.delete(application);
        // Test passes if no exceptions are thrown
    }

    @Test
    public void testDeleteOperationOfferPermissions() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;

        Offer offer = new Offer();
        offer.candidate = candidate;
        offer.job = job;
        offer.application = application;

        // Employer should be able to delete offers for their jobs
        Assert.assertTrue("Employer should be able to delete offers for their jobs",
                offer.$isDeletableBy(employerUser));
        employerUser.delete(offer);

        // Candidate should not be able to delete offers (they can only update status)
        Offer newOffer = new Offer();
        newOffer.candidate = candidate;
        newOffer.job = job;
        newOffer.application = application;

        Assert.assertFalse("Candidate should not be able to delete offers",
                newOffer.$isDeletableBy(candidate));

        try {
            candidate.delete(newOffer);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testDeleteOperationAnonymousRestrictions() {
        Audience anonymous = Audience.anonymous();

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Job job = new Job();
        job.title = "Software Engineer";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;

        // Anonymous should not be able to delete anything
        try {
            anonymous.delete(candidate);
            Assert.fail("Should have thrown RestrictedAccessException for anonymous delete");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }

        try {
            anonymous.delete(job);
            Assert.fail("Should have thrown RestrictedAccessException for anonymous delete");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }

        try {
            anonymous.delete(application);
            Assert.fail("Should have thrown RestrictedAccessException for anonymous delete");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testDeleteOperationEmployerRestrictions() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        // Employer user should not be able to delete the employer entity itself
        Assert.assertFalse("EmployerUser should not be able to delete employers",
                company.$isDeletableBy(employerUser));

        try {
            employerUser.delete(company);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testDeleteOperationCascadingOwnership() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";

        Job job = new Job();
        job.title = "Software Engineer";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;

        Offer offer = new Offer();
        offer.candidate = candidate;
        offer.job = job;
        offer.application = application;

        // Candidate should be able to delete their own application and offer
        Assert.assertTrue("Candidate should be able to delete their own application",
                application.$isDeletableBy(candidate));
        Assert.assertFalse("Candidate should not be able to delete offers",
                offer.$isDeletableBy(candidate));

        candidate.delete(application);
        // Application deleted successfully

        // Candidate should not be able to delete the job even if they applied to it
        Assert.assertFalse("Candidate should not be able to delete jobs",
                job.$isDeletableBy(candidate));

        try {
            candidate.delete(job);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testDeleteOperationMultiTenantIsolation() {
        // Set up two companies
        Employer company1 = new Employer();
        company1.name = "TechCorp";

        Employer company2 = new Employer();
        company2.name = "StartupInc";

        EmployerUser hr1 = new EmployerUser();
        hr1.name = "TechCorp HR";
        hr1.employer = company1;

        EmployerUser hr2 = new EmployerUser();
        hr2.name = "StartupInc HR";
        hr2.employer = company2;

        Job job1 = new Job();
        job1.title = "TechCorp Position";
        job1.employer = company1;

        Job job2 = new Job();
        job2.title = "Startup Position";
        job2.employer = company2;

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Application app1 = new Application();
        app1.candidate = candidate;
        app1.job = job1;

        Application app2 = new Application();
        app2.candidate = candidate;
        app2.job = job2;

        // HR1 should be able to delete their own company's job but not the other's
        Assert.assertTrue("HR1 should be able to delete their company's job",
                job1.$isDeletableBy(hr1));
        Assert.assertFalse("HR1 should not be able to delete other company's job",
                job2.$isDeletableBy(hr1));

        hr1.delete(job1);

        try {
            hr1.delete(job2);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }

        // Similar restrictions should apply to applications
        // Note: Applications are not deletable by employer users in our model
        Assert.assertFalse("HR should not be able to delete applications",
                app1.$isDeletableBy(hr1));
        Assert.assertFalse("HR should not be able to delete applications",
                app2.$isDeletableBy(hr1));
    }

    @Test
    public void testDeleteOperationPermissionVerification() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        // Test all deletion permission methods work correctly
        Assert.assertTrue("Admin should have delete permission on users",
                candidate.$isDeletableBy(admin));
        Assert.assertTrue("User should have delete permission on themselves",
                candidate.$isDeletableBy(candidate));
        Assert.assertFalse("EmployerUser should not have delete permission on candidates",
                candidate.$isDeletableBy(employerUser));

        // Verify the permissions are consistent with the delete operations
        admin.delete(candidate); // Should succeed

        Candidate newCandidate = new Candidate();
        newCandidate.name = "Bob Developer";

        try {
            employerUser.delete(newCandidate);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected - consistent with permission check
        }
    }

}