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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for the {@link Audience#write} method and related access control
 * behaviors.
 * <p>
 * This test suite focuses on the write operation which enforces field-level
 * write permissions.
 * </p>
 *
 * @author Jeff Nelson
 */
public class AudienceAccessControlWriteTest extends AudienceAccessControlBaseTest {

    @Test
    public void testWriteOperationAllowedFields() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.phone = "555-1234";

        // Candidate should be able to write to their own allowed fields
        Map<String, Object> updates = ImmutableMap.of(
                "name", "Jane Senior Developer",
                "phone", "555-5678"
        );

        candidate.write(updates, candidate);

        // Verify the updates were applied
        Assert.assertEquals("Jane Senior Developer", candidate.name);
        Assert.assertEquals("555-5678", candidate.phone);
    }

    @Test
    public void testWriteOperationDeniedFieldsThrowsException() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";

        Map<String, Object> updates = ImmutableMap.of("name", "Modified Name");

        // Should throw exception when trying to write to another user's fields
        try {
            candidate1.write(updates, candidate2);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testWriteOperationPartialAccessThrowsException() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;

        Map<String, Object> updates = ImmutableMap.of(
                "title", "Senior Backend Developer",
                "createdAt", Timestamp.now() // Not writable by employer
        );

        // Should throw exception if any field is not writable
        try {
            employerUser.write(updates, job);
            Assert.fail("Should have thrown RestrictedAccessException for mixed write access");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testWriteOperationApplicationFields() {
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
        application.status = "submitted";

        // Employer should be able to update application status and notes
        Map<String, Object> employerUpdates = ImmutableMap.of(
                "status", "under_review",
                "notes", "Promising candidate"
        );

        employerUser.write(employerUpdates, application);

        Assert.assertEquals("under_review", application.status);
        Assert.assertEquals("Promising candidate", application.notes);

        // Candidate should be able to update their cover letter
        Map<String, Object> candidateUpdates = ImmutableMap.of(
                "coverLetter", "Updated cover letter"
        );

        candidate.write(candidateUpdates, application);
        Assert.assertEquals("Updated cover letter", application.coverLetter);
    }

    @Test
    public void testWriteOperationAdminOverride() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";

        // Admin should be able to write to any field
        Map<String, Object> adminUpdates = ImmutableMap.of(
                "name", "Jane Senior Developer",
                "email", "jane.senior@email.com",
                "lastLogin", Timestamp.now()
        );

        admin.write(adminUpdates, candidate);

        Assert.assertEquals("Jane Senior Developer", candidate.name);
        Assert.assertEquals("jane.senior@email.com", candidate.email);
        Assert.assertNotNull(candidate.lastLogin);
    }

    @Test
    public void testWriteOperationEmptyUpdatesSucceeds() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        // Empty updates should always succeed
        Map<String, Object> emptyUpdates = ImmutableMap.of();
        candidate.write(emptyUpdates, candidate);

        // Should not throw exception
        Assert.assertEquals("Jane Developer", candidate.name);
    }

    @Test
    public void testWriteOperationSingleFieldUpdate() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.phone = "555-1234";

        // Test single field write method
        candidate.write("phone", "555-9999", candidate);

        Assert.assertEquals("555-9999", candidate.phone);
        Assert.assertEquals("Jane Developer", candidate.name); // Should remain unchanged
    }

    @Test
    public void testWriteOperationSingleFieldDenied() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";

        // Should throw exception for single field write when denied
        try {
            candidate1.write("name", "Modified Name", candidate2);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testWriteOperationJobFields() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;
        job.published = false;
        job.salary = 80000.0;

        // Employer should be able to update job details
        Map<String, Object> updates = ImmutableMap.of(
                "title", "Senior Backend Developer",
                "published", true,
                "salary", 100000.0
        );

        employerUser.write(updates, job);

        Assert.assertEquals("Senior Backend Developer", job.title);
        Assert.assertTrue(job.published);
        Assert.assertEquals(100000.0, job.salary, 0.01);
    }

    @Test
    public void testWriteOperationOfferFields() {
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
        offer.status = "pending";

        // Employer should be able to update offer details
        Map<String, Object> employerUpdates = ImmutableMap.of(
                "salary", 120000.0,
                "benefits", "Health, Dental, 401k",
                "status", "extended"
        );

        employerUser.write(employerUpdates, offer);

        Assert.assertEquals(120000.0, offer.salary, 0.01);
        Assert.assertEquals("Health, Dental, 401k", offer.benefits);
        Assert.assertEquals("extended", offer.status);

        // Candidate should be able to accept/reject the offer
        candidate.write("status", "accepted", offer);
        Assert.assertEquals("accepted", offer.status);
    }

    @Test
    public void testWriteOperationEmployerFields() {
        Employer company = new Employer();
        company.name = "TechCorp";
        company.description = "Old description";
        company.website = "old-site.com";
        company.size = 50;

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        // Employer user should be able to update allowed company fields
        Map<String, Object> updates = ImmutableMap.of(
                "description", "Leading technology company",
                "website", "techcorp.com",
                "size", 100
        );

        employerUser.write(updates, company);

        Assert.assertEquals("Leading technology company", company.description);
        Assert.assertEquals("techcorp.com", company.website);
        Assert.assertEquals(Integer.valueOf(100), company.size);

        // Should not be able to update restricted fields like name
        try {
            employerUser.write("name", "NewTechCorp", company);
            Assert.fail("Should have thrown RestrictedAccessException for company name");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testWriteOperationAnonymousRestrictions() {
        Audience anonymous = Audience.anonymous();

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        // Anonymous should not be able to write to any fields
        try {
            anonymous.write("name", "Modified Name", candidate);
            Assert.fail("Should have thrown RestrictedAccessException for anonymous write");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }

        Job job = new Job();
        job.title = "Software Engineer";

        try {
            anonymous.write("title", "Modified Title", job);
            Assert.fail("Should have thrown RestrictedAccessException for anonymous write");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testWriteOperationCrossCompanyRestrictions() {
        Employer company1 = new Employer();
        company1.name = "TechCorp";

        Employer company2 = new Employer();
        company2.name = "StartupInc";

        EmployerUser hr1 = new EmployerUser();
        hr1.name = "TechCorp HR";
        hr1.employer = company1;

        Job job1 = new Job();
        job1.title = "TechCorp Position";
        job1.employer = company1;

        Job job2 = new Job();
        job2.title = "Startup Position";
        job2.employer = company2;

        // HR1 should be able to update their own company's job
        hr1.write("title", "Updated TechCorp Position", job1);
        Assert.assertEquals("Updated TechCorp Position", job1.title);

        // HR1 should not be able to update other company's job
        try {
            hr1.write("title", "Hacked Startup Position", job2);
            Assert.fail("Should have thrown RestrictedAccessException for cross-company write");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

}