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

/**
 * Unit tests for the {@link Audience#create} method and related access control
 * behaviors.
 * <p>
 * This test suite focuses on the create operation which enforces creation
 * permissions.
 * </p>
 *
 * @author Jeff Nelson
 */
public class AudienceAccessControlCreateTest
        extends AudienceAccessControlBaseTest {

    @Test
    public void testCreateOperationUserRegistration() {
        Audience anonymous = Audience.anonymous();

        // Anonymous users should be able to create user accounts (registration)
        Candidate newCandidate = anonymous.create(Candidate.class);
        Assert.assertNotNull("Anonymous should successfully create candidate",
                newCandidate);

        EmployerUser newEmployerUser = anonymous.create(EmployerUser.class);
        Assert.assertNotNull(
                "Anonymous should successfully create employer user",
                newEmployerUser);

        // Anonymous should be able to create admin accounts too (registration)
        Admin newAdmin = anonymous.create(Admin.class);
        Assert.assertNotNull("Anonymous should successfully create admin",
                newAdmin);
    }

    @Test
    public void testCreateOperationJobCreation() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Audience anonymous = Audience.anonymous();

        // Admin should successfully create jobs
        Job adminJob = admin.create(Job.class);
        Assert.assertNotNull("Admin should successfully create job", adminJob);

        // EmployerUser should successfully create jobs
        Job employerJob = employerUser.create(Job.class);
        Assert.assertNotNull("EmployerUser should successfully create job",
                employerJob);

        // Candidate should not be able to create jobs
        try {
            candidate.create(Job.class);
            Assert.fail("Candidate should not be able to create jobs");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        // Anonymous should not be able to create jobs
        try {
            anonymous.create(Job.class);
            Assert.fail("Anonymous should not be able to create jobs");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testCreateOperationApplicationCreation() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Audience anonymous = Audience.anonymous();

        // Admin should successfully create applications
        Application adminApplication = admin.create(Application.class);
        Assert.assertNotNull("Admin should successfully create application",
                adminApplication);

        // Candidate should successfully create applications
        Application candidateApplication = candidate.create(Application.class);
        Assert.assertNotNull("Candidate should successfully create application",
                candidateApplication);

        // EmployerUser should not be able to create applications
        try {
            employerUser.create(Application.class);
            Assert.fail(
                    "EmployerUser should not be able to create applications");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        // Anonymous should not be able to create applications
        try {
            anonymous.create(Application.class);
            Assert.fail("Anonymous should not be able to create applications");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testCreateOperationEmployerCreation() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Audience anonymous = Audience.anonymous();

        // Admin should successfully create employers
        Employer adminEmployer = admin.create(Employer.class);
        Assert.assertNotNull("Admin should successfully create employer",
                adminEmployer);

        // EmployerUser should successfully create employers
        Employer userEmployer = employerUser.create(Employer.class);
        Assert.assertNotNull("EmployerUser should successfully create employer",
                userEmployer);

        // Candidate should not be able to create employers
        try {
            candidate.create(Employer.class);
            Assert.fail("Candidate should not be able to create employers");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        // Anonymous should not be able to create employers
        try {
            anonymous.create(Employer.class);
            Assert.fail("Anonymous should not be able to create employers");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testCreateOperationOfferCreation() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        Audience anonymous = Audience.anonymous();

        // Admin should successfully create offers
        Offer adminOffer = admin.create(Offer.class);
        Assert.assertNotNull("Admin should successfully create offer",
                adminOffer);

        // EmployerUser should successfully create offers
        Offer employerOffer = employerUser.create(Offer.class);
        Assert.assertNotNull("EmployerUser should successfully create offer",
                employerOffer);

        // Candidate should not be able to create offers
        try {
            candidate.create(Offer.class);
            Assert.fail("Candidate should not be able to create offers");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        // Anonymous should not be able to create offers
        try {
            anonymous.create(Offer.class);
            Assert.fail("Anonymous should not be able to create offers");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testCreateOperationComplexScenario() {
        // Test a complete workflow scenario
        Audience anonymous = Audience.anonymous();

        // 1. Anonymous user creates candidate account
        Candidate candidate = anonymous.create(Candidate.class);
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        Assert.assertNotNull("Anonymous should create candidate", candidate);

        // 2. Admin creates employer
        Admin admin = new Admin();
        admin.name = "System Admin";

        Employer company = admin.create(Employer.class);
        company.name = "TechCorp";
        Assert.assertNotNull("Admin should create employer", company);

        // 3. EmployerUser is created and linked to company
        EmployerUser employerUser = admin.create(EmployerUser.class);
        employerUser.name = "HR Manager";
        employerUser.employer = company;
        Assert.assertNotNull("Admin should create employer user", employerUser);

        // 4. EmployerUser creates job
        Job job = employerUser.create(Job.class);
        job.title = "Backend Developer";
        job.employer = company;
        job.published = true;
        Assert.assertNotNull("EmployerUser should create job", job);

        // 5. Candidate creates application
        Application application = candidate.create(Application.class);
        application.candidate = candidate;
        application.job = job;
        application.coverLetter = "I'm interested in this position";
        Assert.assertNotNull("Candidate should create application",
                application);

        // 6. EmployerUser creates offer
        Offer offer = employerUser.create(Offer.class);
        offer.candidate = candidate;
        offer.job = job;
        offer.application = application;
        offer.salary = 100000.0;
        Assert.assertNotNull("EmployerUser should create offer", offer);

        // Verify the entire workflow completed successfully
        Assert.assertEquals("Jane Developer", candidate.name);
        Assert.assertEquals("TechCorp", company.name);
        Assert.assertEquals("HR Manager", employerUser.name);
        Assert.assertEquals("Backend Developer", job.title);
        Assert.assertEquals("I'm interested in this position",
                application.coverLetter);
        Assert.assertEquals(100000.0, offer.salary, 0.01);
    }

    @Test
    public void testCreateOperationWithConstructorArguments() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        // Test create with no constructor arguments
        Candidate candidate = admin.create(Candidate.class);
        Assert.assertNotNull("Should create candidate with no constructor args",
                candidate);

        Job job = admin.create(Job.class);
        Assert.assertNotNull("Should create job with no constructor args", job);

        Application application = admin.create(Application.class);
        Assert.assertNotNull(
                "Should create application with no constructor args",
                application);

        // Test create with constructor arguments (if the classes had
        // constructors that took args)
        // For now, testing that the varargs signature works with no args
        Employer employer = admin.create(Employer.class, new Object[0]);
        Assert.assertNotNull(
                "Should create employer with explicit empty args array",
                employer);
    }

    @Test
    public void testCreateOperationReturnedInstancesAreValid() {
        Admin admin = new Admin();

        // Verify that created instances are actually instances of the expected
        // type
        Candidate candidate = admin.create(Candidate.class);
        Assert.assertTrue("Created instance should be a Candidate",
                candidate instanceof Candidate);
        Assert.assertTrue("Created instance should be a User",
                candidate instanceof User);
        Assert.assertTrue("Created instance should implement AccessControl",
                candidate instanceof AccessControl);
        Assert.assertTrue("Created instance should implement Audience",
                candidate instanceof Audience);

        Job job = admin.create(Job.class);
        Assert.assertTrue("Created instance should be a Job",
                job instanceof Job);
        Assert.assertTrue("Created instance should implement AccessControl",
                job instanceof AccessControl);

        // Verify the instances can be used immediately
        candidate.name = "Test Candidate";
        Assert.assertEquals("Should be able to set and get fields",
                "Test Candidate", candidate.name);

        job.title = "Test Job";
        Assert.assertEquals("Should be able to set and get fields", "Test Job",
                job.title);
    }

    @Test
    public void testCreateOperationCrossRoleRestrictions() {
        Admin admin = new Admin();
        Candidate candidate = new Candidate();
        EmployerUser employerUser = new EmployerUser();

        // Test that different roles can/cannot create different entity types

        // Job creation - should work for admin and employer, fail for candidate
        Job adminJob = admin.create(Job.class);
        Assert.assertNotNull("Admin should successfully create job", adminJob);

        Job employerJob = employerUser.create(Job.class);
        Assert.assertNotNull("EmployerUser should successfully create job",
                employerJob);

        try {
            candidate.create(Job.class);
            Assert.fail("Candidate should not be able to create jobs");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        // Application creation - should work for admin and candidate, fail for
        // employer
        Application adminApplication = admin.create(Application.class);
        Assert.assertNotNull("Admin should successfully create application",
                adminApplication);

        Application candidateApplication = candidate.create(Application.class);
        Assert.assertNotNull("Candidate should successfully create application",
                candidateApplication);

        try {
            employerUser.create(Application.class);
            Assert.fail(
                    "EmployerUser should not be able to create applications");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testCreateOperationAnonymousLimitations() {
        Audience anonymous = Audience.anonymous();

        // Anonymous should only be able to create user accounts
        Candidate candidate = anonymous.create(Candidate.class);
        Assert.assertNotNull("Anonymous should create candidate", candidate);

        EmployerUser employerUser = anonymous.create(EmployerUser.class);
        Assert.assertNotNull("Anonymous should create employer user",
                employerUser);

        // Anonymous should not be able to create business entities
        try {
            anonymous.create(Job.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        try {
            anonymous.create(Employer.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        try {
            anonymous.create(Application.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        try {
            anonymous.create(Offer.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testCreateOperationRoleBasedRestrictions() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        // Candidates should not be able to create business-side entities
        try {
            candidate.create(Job.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        try {
            candidate.create(Employer.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        try {
            candidate.create(Offer.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        // But candidates should be able to create applications
        Application application = candidate.create(Application.class);
        Assert.assertNotNull("Candidate should create application",
                application);

        // EmployerUsers should not be able to create applications
        try {
            employerUser.create(Application.class);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }

        // But should be able to create jobs, employers, and offers
        Job job = employerUser.create(Job.class);
        Assert.assertNotNull("EmployerUser should create job", job);

        Employer employer = employerUser.create(Employer.class);
        Assert.assertNotNull("EmployerUser should create employer", employer);

        Offer offer = employerUser.create(Offer.class);
        Assert.assertNotNull("EmployerUser should create offer", offer);
    }

}