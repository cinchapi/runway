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
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for the {@link Audience#read} method and related access control
 * behaviors.
 * <p>
 * This test suite focuses on the read operation which throws exceptions for
 * denied access instead of filtering data.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("unchecked")
public class AudienceAccessControlReadTest extends AudienceAccessControlBaseTest {

    @Test
    public void testReadOperationAllowedFields() {
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@system.com";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.resume = "Jane's resume content";
        candidate.skills = "Java, Python";

        // Admin should be able to read all fields
        Map<String, Object> result = admin.read(
                ImmutableSet.of("name", "email", "resume", "skills"), candidate);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("name"));
        Assert.assertTrue(result.containsKey("email"));
        Assert.assertTrue(result.containsKey("resume"));
        Assert.assertTrue(result.containsKey("skills"));
        Assert.assertEquals("Jane Developer", result.get("name"));
        Assert.assertEquals("jane@email.com", result.get("email"));
    }

    @Test
    public void testReadOperationDeniedFieldsThrowsException() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice Smith";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob Jones";
        candidate2.email = "bob@email.com";
        candidate2.resume = "Bob's private resume";
        candidate2.skills = "JavaScript, React";

        // Candidate trying to read another candidate's private fields should throw exception
        try {
            candidate1.read(ImmutableSet.of("resume", "skills", "email"), candidate2);
            Assert.fail("Should have thrown RestrictedAccessException");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadOperationPartialAccessThrowsException() {
        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.email = "hr@company.com";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.resume = "Jane's private resume";
        candidate.skills = "Java, Python";

        // Even if some fields are accessible, read() should throw exception if any field is denied
        try {
            employerUser.read(ImmutableSet.of("skills", "email", "resume"), candidate);
            Assert.fail("Should have thrown RestrictedAccessException for mixed access");
        } catch (RestrictedAccessException e) {
            // Expected exception - resume is not accessible to employer users
        }
    }

    @Test
    public void testReadOperationNavigationSuccess() {
        Employer company = new Employer();
        company.name = "TechCorp";
        company.description = "A tech company";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;
        job.salary = 100000.0;

        Candidate candidate = new Candidate();
        candidate.name = "Bob Coder";
        candidate.email = "bob@email.com";
        candidate.skills = "Go, Kubernetes";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;
        application.coverLetter = "Cover letter content";

        // Employer should be able to read accessible fields through navigation
        Map<String, Object> result = employerUser.read(
                ImmutableSet.of("candidate.skills", "candidate.email", "job.salary"), application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("candidate"));
        Assert.assertTrue(result.containsKey("job"));

        Map<String, Object> candidateData = (Map<String, Object>) result.get("candidate");
        Assert.assertTrue(candidateData.containsKey("skills"));
        Assert.assertTrue(candidateData.containsKey("email"));

        Map<String, Object> jobData = (Map<String, Object>) result.get("job");
        Assert.assertTrue(jobData.containsKey("salary"));
    }

    @Test
    public void testReadOperationNavigationFailure() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";

        Job job = new Job();
        job.title = "Developer Role";
        job.published = true;

        Application application = new Application();
        application.candidate = candidate2;
        application.job = job;
        application.coverLetter = "Bob's cover letter";

        // Should throw exception when trying to read through undiscoverable records
        try {
            candidate1.read(ImmutableSet.of("coverLetter", "candidate.name"), application);
            Assert.fail("Should have thrown RestrictedAccessException for undiscoverable record");
        } catch (RestrictedAccessException e) {}
    }

    @Test
    public void testReadVsFrameDistinction() {
        Candidate candidate1 = new Candidate();
        candidate1.email = "alice@email.com";
        candidate1.name = "Alice Smith";

        Candidate candidate2 = new Candidate();
        candidate2.email = "bob@email.com";
        candidate2.name = "Bob Jones";
        candidate2.resume = "Bob's private resume";
        candidate2.skills = "JavaScript, React";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        // Test that read() throws exception while frame() filters results
        try {
            employerUser.read(ImmutableSet.of("skills", "resume", "email"), candidate2);
            Assert.fail("read() should have thrown RestrictedAccessException for mixed access");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }

        // Verify frame() works differently - should return only accessible fields
        Map<String, Object> frameResult = employerUser.frame(
                ImmutableSet.of("skills", "resume", "email"), candidate2);
        Assert.assertNotNull(frameResult);
        Assert.assertTrue("frame() should include accessible skills field",
                frameResult.containsKey("skills"));
        Assert.assertTrue("frame() should include accessible email field",
                frameResult.containsKey("email"));
        Assert.assertFalse("frame() should exclude inaccessible resume field",
                frameResult.containsKey("resume"));
    }

    @Test
    public void testReadOperationWithNavigationMixedAccess() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;
        job.salary = 100000.0;

        Candidate candidate = new Candidate();
        candidate.name = "Bob Coder";
        candidate.email = "bob@email.com";
        candidate.resume = "Bob's private resume";
        candidate.skills = "Go, Kubernetes";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;
        application.coverLetter = "Cover letter content";

        // Should throw exception if any navigated field is denied, even if others are accessible
        try {
            employerUser.read(ImmutableSet.of(
                    "candidate.skills",   // Accessible
                    "candidate.resume",   // NOT accessible for employer
                    "job.salary"         // Accessible
            ), application);
            Assert.fail("Should have thrown RestrictedAccessException for mixed navigation access");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadOperationAccessibleTopLevelInaccessibleNavigation() {
        // Set up scenario where top-level record is accessible but navigation target is not
        Employer company1 = new Employer();
        company1.name = "TechCorp";
        company1.description = "A tech company";

        Employer company2 = new Employer();
        company2.name = "PrivateCorp";
        company2.description = "Private company";
        company2.size = 10; // This field is not readable by candidates

        EmployerUser hrUser = new EmployerUser();
        hrUser.name = "HR Manager";
        hrUser.email = "hr@techcorp.com";
        hrUser.employer = company1;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company2; // Job belongs to company2
        job.published = true;

        Candidate candidate = new Candidate();
        candidate.name = "Alice";
        candidate.email = "alice@email.com";

        // Candidate can discover and read the job (top-level accessible)
        Assert.assertTrue("Job should be discoverable by candidate",
                job.$isDiscoverableBy(candidate));

        // But candidate cannot read private employer fields like 'size'
        Set<String> candidateReadableEmployerFields = company2.$readableBy(candidate);
        Assert.assertFalse("Company size should not be readable by candidate",
                candidateReadableEmployerFields.contains("size"));

        // This should throw exception: job is accessible, but employer.size is not
        try {
            candidate.read(ImmutableSet.of("title", "employer.size"), job);
            Assert.fail("Should have thrown RestrictedAccessException - job accessible but employer.size denied");
        } catch (RestrictedAccessException e) {
            // Expected exception - even though job is accessible to candidate,
            // employer.size is not readable by candidates
        }

        // Verify that the accessible parts work on their own
        Map<String, Object> jobOnlyResult = candidate.read(ImmutableSet.of("title"), job);
        Assert.assertNotNull("Should be able to read just the job title", jobOnlyResult);
        Assert.assertEquals("Backend Developer", jobOnlyResult.get("title"));

        // And verify that employer public fields work
        Map<String, Object> employerPublicResult = candidate.read(ImmutableSet.of("title", "employer.name"), job);
        Assert.assertNotNull("Should be able to read job title and employer name", employerPublicResult);
        Assert.assertTrue(employerPublicResult.containsKey("title"));
        Assert.assertTrue(employerPublicResult.containsKey("employer"));

        System.out.println(employerPublicResult);
        Map<String, Object> employerData = (Map<String, Object>) employerPublicResult.get("employer");
        Assert.assertEquals("PrivateCorp", employerData.get("name"));
    }

    @Test
    public void testReadOperationSelfAccess() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.resume = "Jane's resume content";
        candidate.skills = "Java, Python";
        candidate.yearsExperience = 5;

        // Candidate should be able to read all their own fields
        Map<String, Object> result = candidate.read(
                ImmutableSet.of("name", "email", "resume", "skills", "yearsExperience"),
                candidate);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("name"));
        Assert.assertTrue(result.containsKey("email"));
        Assert.assertTrue(result.containsKey("resume"));
        Assert.assertTrue(result.containsKey("skills"));
        Assert.assertTrue(result.containsKey("yearsExperience"));
        Assert.assertEquals("Jane Developer", result.get("name"));
        Assert.assertEquals("jane@email.com", result.get("email"));
        Assert.assertEquals("Jane's resume content", result.get("resume"));
        Assert.assertEquals("Java, Python", result.get("skills"));
        Assert.assertEquals(Integer.valueOf(5), result.get("yearsExperience"));
    }

    @Test
    public void testReadOperationAnonymousAccess() {
        Audience anonymous = Audience.anonymous();

        Job job = new Job();
        job.title = "Software Engineer";
        job.description = "Great opportunity";
        job.published = true;
        job.salary = 100000.0;

        // Anonymous should be able to read public job fields
        Map<String, Object> result = anonymous.read(
                ImmutableSet.of("title", "description"), job);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("title"));
        Assert.assertTrue(result.containsKey("description"));
        Assert.assertEquals("Software Engineer", result.get("title"));
        Assert.assertEquals("Great opportunity", result.get("description"));

        // Anonymous should not be able to read private fields like salary
        try {
            anonymous.read(ImmutableSet.of("title", "salary"), job);
            Assert.fail("Should have thrown RestrictedAccessException for salary field");
        } catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadOperationWithNonExistentFields() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";

        // Should succeed and return existing fields, ignoring non-existent ones
        Map<String, Object> result = admin.read(
                ImmutableSet.of("name", "nonExistentField"), candidate);

        Assert.assertNotNull("Should return result without throwing exception", result);
        Assert.assertTrue("Should contain existing field", result.containsKey("name"));
        Assert.assertEquals("Jane Developer", result.get("name"));
        Assert.assertFalse("Should not contain non-existent field",
                result.containsKey("nonExistentField"));

        // Test with navigation to non-existent fields
        Job job = new Job();
        job.title = "Software Engineer";
        job.published = true;

        Map<String, Object> navResult = admin.read(
                ImmutableSet.of("title", "nonExistent.field"), job);

        Assert.assertNotNull("Should return result for navigation with non-existent paths", navResult);
        Assert.assertTrue("Should contain existing field", navResult.containsKey("title"));
        Assert.assertEquals("Software Engineer", navResult.get("title"));
        Assert.assertFalse("Should not contain non-existent navigation path",
                navResult.containsKey("nonExistent"));
    }

    @Test
    public void testReadOperationDeepNavigation() {
        Employer company = new Employer();
        company.name = "TechCorp";
        company.location = "San Francisco";

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;
        job.published = true;

        Candidate candidate = new Candidate();
        candidate.name = "Alice Developer";
        candidate.email = "alice@email.com";
        candidate.skills = "Java, Python";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;

        Offer offer = new Offer();
        offer.application = application;
        offer.job = job;
        offer.candidate = candidate;
        offer.salary = 120000.0;

        Admin admin = new Admin();
        admin.name = "System Admin";

        // Admin should be able to read through deep navigation
        Map<String, Object> result = admin.read(ImmutableSet.of(
                "application.job.employer.name",
                "application.job.employer.location",
                "application.candidate.skills",
                "salary"
        ), offer);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("application"));
        Assert.assertTrue(result.containsKey("salary"));

        Map<String, Object> applicationData = (Map<String, Object>) result.get("application");
        Assert.assertTrue(applicationData.containsKey("job"));
        Assert.assertTrue(applicationData.containsKey("candidate"));

        Map<String, Object> jobData = (Map<String, Object>) applicationData.get("job");
        Assert.assertTrue(jobData.containsKey("employer"));

        Map<String, Object> employerData = (Map<String, Object>) jobData.get("employer");
        Assert.assertTrue(employerData.containsKey("name"));
        Assert.assertTrue(employerData.containsKey("location"));
        Assert.assertEquals("TechCorp", employerData.get("name"));
        Assert.assertEquals("San Francisco", employerData.get("location"));

        Map<String, Object> candidateData = (Map<String, Object>) applicationData.get("candidate");
        Assert.assertTrue(candidateData.containsKey("skills"));
        Assert.assertEquals("Java, Python", candidateData.get("skills"));

        Assert.assertEquals(120000.0, result.get("salary"));
    }

}