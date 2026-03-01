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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for the {@link Audience#frame} method and related access control
 * framework behaviors.
 * <p>
 * This test suite focuses on the framing behavior which filters out
 * inaccessible data instead of throwing exceptions.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("unchecked")
public class AudienceAccessControlFrameTest
        extends AudienceAccessControlBaseTest {

    @Test
    public void testAdminCanAccessAllCandidateFields() {
        Admin admin = new Admin();
        admin.email = "admin@company.com";
        admin.name = "System Admin";
        admin.department = "IT";

        Candidate candidate = new Candidate();
        candidate.email = "candidate@email.com";
        candidate.name = "Jane Doe";
        candidate.resume = "Jane's resume";
        candidate.skills = "Full Stack Development";
        candidate.yearsExperience = 5;

        Map<String, Object> result = admin.frame(
                ImmutableSet.of("resume", "skills", "email", "yearsExperience"),
                candidate);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("resume"));
        Assert.assertTrue(result.containsKey("skills"));
        Assert.assertTrue(result.containsKey("email"));
        Assert.assertTrue(result.containsKey("yearsExperience"));
    }

    @Test
    public void testAdminNavigationBypassesAllRestrictions() {
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@system.com";

        Employer company = new Employer();
        company.name = "SecretCorp";
        company.size = 50;

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.resume = "Top secret resume";
        candidate.skills = "Advanced skills";

        Application application = new Application();
        application.candidate = candidate;
        application.notes = "Internal HR notes";

        // Admin should access all fields through navigation
        Map<String, Object> result = admin.frame(ImmutableSet.of(
                "candidate.resume", "candidate.skills", "notes"), application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("candidate"));
        Assert.assertTrue(result.containsKey("notes"));

        Map<String, Object> candidateData = (Map<String, Object>) result
                .get("candidate");
        Assert.assertTrue(candidateData.containsKey("resume"));
        Assert.assertTrue(candidateData.containsKey("skills"));
    }

    @Test
    public void testCandidateCannotAccessOtherCandidatePrivateFields() {
        Candidate candidate1 = new Candidate();
        candidate1.email = "alice@email.com";
        candidate1.name = "Alice Smith";

        Candidate candidate2 = new Candidate();
        candidate2.email = "bob@email.com";
        candidate2.name = "Bob Jones";
        candidate2.resume = "Bob's private resume";
        candidate2.skills = "JavaScript, React";

        Map<String, Object> result = candidate1.frame(
                ImmutableSet.of("resume", "skills", "email"), candidate2);
        Assert.assertFalse(result.containsKey("resume"));
        Assert.assertFalse(result.containsKey("skills"));
        Assert.assertFalse(result.containsKey("email"));
    }

    @Test
    public void testCandidateCannotNavigateToOtherApplications() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";

        Job job = new Job();
        job.title = "Developer Role";
        job.published = true;

        Application otherApplication = new Application();
        otherApplication.candidate = candidate2;
        otherApplication.job = job;
        otherApplication.coverLetter = "Bob's cover letter";

        // candidate1 tries to access candidate2's application details
        Map<String, Object> result = candidate1.frame(
                ImmutableSet.of("coverLetter", "candidate.email"),
                otherApplication);

        // Should return null since candidate1 cannot discover other candidates'
        // applications
        Assert.assertNull(result);
    }

    @Test
    public void testEmployerNavigationToApplicationCandidateData() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.email = "hr@techcorp.com";
        employerUser.employer = company;

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;

        Candidate candidate = new Candidate();
        candidate.name = "Bob Coder";
        candidate.email = "bob@email.com";
        candidate.resume = "Bob's private resume";
        candidate.skills = "Go, Kubernetes";
        candidate.yearsExperience = 4;

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;
        application.coverLetter = "Cover letter content";

        // Employer navigating through application to candidate data
        Map<String, Object> result = employerUser.frame(ImmutableSet
                .of("candidate.skills", "candidate.resume", "candidate.email"),
                application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("candidate"));

        Map<String, Object> candidateData = (Map<String, Object>) result
                .get("candidate");
        Assert.assertTrue(candidateData.containsKey("skills"));
        Assert.assertTrue(candidateData.containsKey("email"));
        Assert.assertFalse(candidateData.containsKey("resume")); // Resume not
                                                                 // accessible
                                                                 // to employers
    }

    @Test
    public void testEmployerUserCanAccessAllowedCandidateFields() {
        Employer company = new Employer();
        company.name = "Tech Corp";
        company.description = "A tech company";

        EmployerUser employerUser = new EmployerUser();
        employerUser.email = "hr@techcorp.com";
        employerUser.name = "HR Manager";
        employerUser.employer = company;
        employerUser.role = "HR";

        Candidate candidate = new Candidate();
        candidate.email = "candidate@email.com";
        candidate.name = "John Developer";
        candidate.resume = "Private resume content";
        candidate.skills = "Java, Spring Boot";
        candidate.yearsExperience = 3;
        candidate.location = "San Francisco";

        Map<String, Object> result = employerUser.frame(
                ImmutableSet.of("skills", "resume", "email", "location"),
                candidate);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("skills"));
        Assert.assertTrue(result.containsKey("email"));
        Assert.assertTrue(result.containsKey("location"));
        Assert.assertFalse(result.containsKey("resume"));
    }

    @Test
    public void testNavigationAccessControlOnApplications() {
        Employer company = new Employer();
        company.name = "TechCorp";
        company.description = "Tech company description";

        Job job = new Job();
        job.title = "Software Engineer";
        job.employer = company;
        job.published = true;
        job.salary = 100000.0;

        Candidate candidate = new Candidate();
        candidate.name = "Alice Developer";
        candidate.email = "alice@email.com";
        candidate.skills = "Java, Python";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;
        application.coverLetter = "I am interested in this position";
        application.status = "submitted";

        // Test candidate accessing their own application with navigation
        Map<String, Object> result = candidate.frame(
                ImmutableSet.of("job.title", "job.salary", "coverLetter"),
                application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("job"));
        Assert.assertTrue(result.containsKey("coverLetter"));

        // Candidate should see job title but not salary (not in job's readable
        // fields for candidates)
        Map<String, Object> jobData = (Map<String, Object>) result.get("job");
        Assert.assertTrue(jobData.containsKey("title"));
        Assert.assertFalse(jobData.containsKey("salary"));
    }

    // ========================================================================
    // BASIC ACCESS CONTROL TESTS
    // ========================================================================

    @Test
    public void testNegativeFieldAccessThrowsException() {
        Candidate candidate1 = new Candidate();
        candidate1.email = "alice@email.com";
        candidate1.name = "Alice Smith";

        Candidate candidate2 = new Candidate();
        candidate2.email = "bob@email.com";
        candidate2.name = "Bob Jones";
        candidate2.resume = "Bob's private resume";
        candidate2.skills = "JavaScript, React";

        // Test that read() throws exception for denied fields
        try {
            candidate1.read(ImmutableSet.of("resume", "skills"), candidate2);
            Assert.fail("Should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {
            // Expected exception
        }
    }

    @Test
    public void testSpecialRuleSetAllKeys() {
        Admin admin = new Admin();
        admin.email = "admin@company.com";
        admin.name = "System Admin";

        Candidate candidate = new Candidate();
        candidate.email = "candidate@email.com";
        candidate.name = "Jane Doe";
        candidate.resume = "Jane's resume";
        candidate.skills = "Full Stack Development";

        // Admin should have access to all fields (ALL_KEYS rule)
        Map<String, Object> result = admin.frame(ImmutableSet.of("email",
                "name", "resume", "skills", "yearsExperience"), candidate);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("email"));
        Assert.assertTrue(result.containsKey("name"));
        Assert.assertTrue(result.containsKey("resume"));
        Assert.assertTrue(result.containsKey("skills"));
    }

    @Test
    public void testSpecialRuleSetNoKeys() {
        Candidate candidate1 = new Candidate();
        candidate1.email = "alice@email.com";
        candidate1.name = "Alice Smith";

        Candidate candidate2 = new Candidate();
        candidate2.email = "bob@email.com";
        candidate2.name = "Bob Jones";
        candidate2.resume = "Bob's private resume";

        // Candidate accessing another candidate's private fields should get
        // NO_KEYS
        Map<String, Object> result = candidate1.frame(
                ImmutableSet.of("resume", "skills", "email"), candidate2);
        Assert.assertFalse(result.containsKey("resume"));
        Assert.assertFalse(result.containsKey("skills"));
        Assert.assertFalse(result.containsKey("email"));
    }

    @Test
    public void testAnonymousVsAuthenticatedAccess() {
        Candidate candidate = new Candidate();
        candidate.email = "candidate@email.com";
        candidate.name = "Jane Doe";
        candidate.resume = "Jane's resume";

        Job job = new Job();
        job.title = "Software Engineer";
        job.description = "Great opportunity";
        job.published = true;
        job.salary = 100000.0;

        // Anonymous access
        Audience anonymous = Audience.anonymous();
        Map<String, Object> anonymousResult = anonymous
                .frame(ImmutableSet.of("title", "description", "salary"), job);

        Assert.assertNotNull(anonymousResult);
        Assert.assertTrue(anonymousResult.containsKey("title"));
        Assert.assertTrue(anonymousResult.containsKey("description"));
        Assert.assertFalse(anonymousResult.containsKey("salary")); // Salary not
                                                                   // accessible
                                                                   // to
                                                                   // anonymous

        // Authenticated candidate access - should have same limitations
        Map<String, Object> candidateResult = candidate
                .frame(ImmutableSet.of("title", "description", "salary"), job);

        Assert.assertNotNull(candidateResult);
        Assert.assertTrue(candidateResult.containsKey("title"));
        Assert.assertTrue(candidateResult.containsKey("description"));
        Assert.assertFalse(candidateResult.containsKey("salary")); // Salary
                                                                   // still not
                                                                   // accessible
    }

    @Test
    public void testRulePatternAllowlistOnly() {
        Employer company = new Employer();
        company.name = "TechCorp";
        company.description = "Tech company";
        company.website = "techcorp.com";
        company.size = 100;

        Candidate candidate = new Candidate();
        candidate.email = "candidate@email.com";
        candidate.name = "Jane Doe";

        // Candidate should only see allowlisted fields (name, description,
        // website, industry, location)
        Map<String, Object> result = candidate.frame(ImmutableSet.of("name",
                "description", "website", "size", "industry"), company);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("name"));
        Assert.assertTrue(result.containsKey("description"));
        Assert.assertTrue(result.containsKey("website"));
        Assert.assertFalse(result.containsKey("size")); // Not in allowlist for
                                                        // candidates
    }

    // ========================================================================
    // NAVIGATION ACCESS CONTROL TESTS
    // ========================================================================

    @Test
    public void testMultiHopNavigation() {
        Employer company = new Employer();
        company.name = "TechCorp";
        company.description = "A tech company";

        Job job = new Job();
        job.title = "Backend Developer";
        job.employer = company;
        job.published = true;

        Candidate candidate = new Candidate();
        candidate.name = "Bob Coder";
        candidate.email = "bob@email.com";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;
        application.coverLetter = "Cover letter content";

        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@system.com";

        // Test multi-hop navigation: application -> job -> employer
        Map<String, Object> result = admin
                .frame(ImmutableSet.of("job.employer.name",
                        "job.employer.description", "job.title"), application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("job"));

        Map<String, Object> jobData = (Map<String, Object>) result.get("job");
        Assert.assertTrue(jobData.containsKey("title"));
        Assert.assertTrue(jobData.containsKey("employer"));

        Map<String, Object> employerData = (Map<String, Object>) jobData
                .get("employer");
        Assert.assertTrue(employerData.containsKey("name"));
        Assert.assertTrue(employerData.containsKey("description"));
    }

    @Test
    public void testDiscoveryBlockingNavigation() {
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

        // candidate1 tries to navigate through application that they can't
        // discover
        Map<String, Object> result = candidate1.frame(
                ImmutableSet.of("job.title", "candidate.name"), application);

        // Should return null since candidate1 cannot discover candidate2's
        // application
        Assert.assertNull(result);
    }

    @Test
    public void testMixedNavigationRequests() {
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.email = "hr@techcorp.com";
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

        // Mix of accessible and blocked navigation paths
        Map<String, Object> result = employerUser
                .frame(ImmutableSet.of("candidate.skills", // Accessible
                        "candidate.resume", // Blocked for employer
                        "job.salary", // Accessible (own employer's job)
                        "coverLetter" // Accessible
                ), application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("candidate"));
        Assert.assertTrue(result.containsKey("job"));
        Assert.assertTrue(result.containsKey("coverLetter"));

        Map<String, Object> candidateData = (Map<String, Object>) result
                .get("candidate");
        Assert.assertTrue(candidateData.containsKey("skills"));
        Assert.assertFalse(candidateData.containsKey("resume")); // Should be
                                                                 // filtered out

        Map<String, Object> jobData = (Map<String, Object>) result.get("job");
        Assert.assertTrue(jobData.containsKey("salary"));
    }

    @Test
    public void testDeepNavigationChains() {
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
        candidate.location = "New York";

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
        admin.email = "admin@system.com";

        // Deep navigation: offer -> application -> job -> employer
        Map<String, Object> result = admin
                .frame(ImmutableSet.of("application.job.employer.name",
                        "application.job.employer.location",
                        "application.candidate.skills",
                        "application.candidate.location"), offer);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("application"));

        Map<String, Object> applicationData = (Map<String, Object>) result
                .get("application");
        Assert.assertTrue(applicationData.containsKey("job"));
        Assert.assertTrue(applicationData.containsKey("candidate"));

        Map<String, Object> jobData = (Map<String, Object>) applicationData
                .get("job");
        Assert.assertTrue(jobData.containsKey("employer"));

        Map<String, Object> employerData = (Map<String, Object>) jobData
                .get("employer");
        Assert.assertTrue(employerData.containsKey("name"));
        Assert.assertTrue(employerData.containsKey("location"));

        Map<String, Object> candidateData = (Map<String, Object>) applicationData
                .get("candidate");
        Assert.assertTrue(candidateData.containsKey("skills"));
        Assert.assertTrue(candidateData.containsKey("location"));
    }

    // ========================================================================
    // CRUD OPERATION TESTS
    // ========================================================================

    @Test
    public void testCreatePermissions() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        // Test creation permissions for different record types
        Job job = new Job();
        Assert.assertTrue("Admin should be able to create jobs",
                job.$isCreatableBy(admin));
        Assert.assertTrue("EmployerUser should be able to create jobs",
                job.$isCreatableBy(employerUser));
        Assert.assertFalse("Candidate should not be able to create jobs",
                job.$isCreatableBy(candidate));
        Assert.assertFalse("Anonymous should not be able to create jobs",
                job.$isCreatableByAnonymous());

        Application application = new Application();
        Assert.assertTrue("Admin should be able to create applications",
                application.$isCreatableBy(admin));
        Assert.assertTrue("Candidate should be able to create applications",
                application.$isCreatableBy(candidate));
        Assert.assertFalse(
                "EmployerUser should not be able to create applications",
                application.$isCreatableBy(employerUser));
        Assert.assertFalse(
                "Anonymous should not be able to create applications",
                application.$isCreatableByAnonymous());

        User user = new Candidate();
        Assert.assertTrue("Anyone should be able to create users",
                user.$isCreatableBy(admin));
        Assert.assertTrue("Anyone should be able to create users",
                user.$isCreatableBy(candidate));
        Assert.assertTrue(
                "Anonymous should be able to create users (registration)",
                user.$isCreatableByAnonymous());
    }

    @Test
    public void testWritePermissions() {
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

        // Test write permissions for EmployerUser on their own company's job
        Set<String> writableFields = job.$writableBy(employerUser);
        Assert.assertTrue("EmployerUser should be able to write job details",
                writableFields.contains("title"));
        Assert.assertTrue("EmployerUser should be able to write salary",
                writableFields.contains("salary"));
        Assert.assertTrue("EmployerUser should be able to publish job",
                writableFields.contains("published"));

        // Test that candidate cannot write to job
        Set<String> candidateWritable = job.$writableBy(candidate);
        Assert.assertEquals(
                "Candidate should not be able to write any job fields",
                AccessControl.NO_KEYS, candidateWritable);

        // Test application write permissions
        Application application = new Application();
        application.candidate = candidate;
        application.job = job;

        Set<String> candidateAppWritable = application.$writableBy(candidate);
        Assert.assertTrue("Candidate should be able to edit their cover letter",
                candidateAppWritable.contains("coverLetter"));
        Assert.assertFalse("Candidate should not be able to edit status",
                candidateAppWritable.contains("status"));

        Set<String> employerAppWritable = application.$writableBy(employerUser);
        Assert.assertTrue("Employer should be able to edit application status",
                employerAppWritable.contains("status"));
        Assert.assertTrue("Employer should be able to add notes",
                employerAppWritable.contains("notes"));
        Assert.assertFalse("Employer should not be able to edit cover letter",
                employerAppWritable.contains("coverLetter"));
    }

    @Test
    public void testDeletePermissions() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";

        Application application = new Application();
        application.candidate = candidate1;

        // Test user deletion permissions
        Assert.assertTrue("Admin should be able to delete any user",
                candidate1.$isDeletableBy(admin));
        Assert.assertTrue("User should be able to delete themselves",
                candidate1.$isDeletableBy(candidate1));
        Assert.assertFalse("User should not be able to delete other users",
                candidate1.$isDeletableBy(candidate2));

        // Test application deletion permissions
        Assert.assertTrue("Admin should be able to delete any application",
                application.$isDeletableBy(admin));
        Assert.assertTrue(
                "Candidate should be able to delete their own application",
                application.$isDeletableBy(candidate1));
        Assert.assertFalse(
                "Candidate should not be able to delete other applications",
                application.$isDeletableBy(candidate2));

        // Test employer deletion
        Employer company = new Employer();
        company.name = "TechCorp";

        EmployerUser employerUser = new EmployerUser();
        employerUser.employer = company;

        Assert.assertTrue("Admin should be able to delete employers",
                company.$isDeletableBy(admin));
        Assert.assertFalse(
                "EmployerUser should not be able to delete employers",
                company.$isDeletableBy(employerUser));
        Assert.assertFalse("Candidate should not be able to delete employers",
                company.$isDeletableBy(candidate1));
    }

    // ========================================================================
    // FRAMEWORK BEHAVIOR TESTS
    // ========================================================================

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

        Set<String> deniedFields = ImmutableSet.of("resume", "skills", "email");

        // frame() should return empty map for denied access
        Map<String, Object> frameResult = candidate1.frame(deniedFields,
                candidate2);
        for (String field : deniedFields) {
            Assert.assertFalse(frameResult.containsKey(field));
        }

        // read() should throw exception for denied access
        try {
            candidate1.read(deniedFields, candidate2);
            Assert.fail("read() should have thrown RestrictedAccessException");
        }
        catch (RestrictedAccessException e) {}

        // Test partial access scenario
        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";

        Set<String> mixedFields = ImmutableSet.of("skills", "resume", "email");

        // frame() should return only accessible fields
        Map<String, Object> partialFrameResult = employerUser.frame(mixedFields,
                candidate2);
        Assert.assertNotNull(partialFrameResult);
        Assert.assertTrue(partialFrameResult.containsKey("skills"));
        Assert.assertTrue(partialFrameResult.containsKey("email"));
        Assert.assertFalse(partialFrameResult.containsKey("resume"));

        // read() should throw exception if any field is denied
        try {
            employerUser.read(mixedFields, candidate2);
            Assert.fail(
                    "read() should have thrown RestrictedAccessException for mixed access");
        }
        catch (RestrictedAccessException e) {}
    }

    @Test
    public void testErrorHandlingForInvalidRequests() {
        Admin admin = new Admin();
        admin.name = "System Admin";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";

        // Test requests for non-existent fields
        Map<String, Object> result = admin.frame(
                ImmutableSet.of("nonExistentField", "anotherFakeField"),
                candidate);

        // Should handle gracefully - return empty map for non-existent fields
        Assert.assertNotNull(result);
        // The framework should not break, but won't contain the non-existent
        // fields

        // Test null parameters
        try {
            admin.frame(null, candidate);
            Assert.fail();
        }
        catch (NullPointerException e) {}

        // Test invalid navigation paths
        Map<String, Object> invalidNavResult = admin.frame(
                ImmutableSet.of("nonExistent.field", "fake.navigation.path"),
                candidate);
        Assert.assertNotNull(invalidNavResult);
    }

    @Test
    public void testSecurityValidationAndBypassPrevention() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";
        candidate2.resume = "Sensitive resume data";

        Application application = new Application();
        application.candidate = candidate2;

        // Attempt to bypass access control through navigation
        Map<String, Object> result = candidate1.frame(
                ImmutableSet.of("candidate.resume", "candidate.email"),
                application);

        // Should return null due to discovery restrictions
        Assert.assertNull(
                "Should not be able to bypass access control through navigation",
                result);

        // Ensure consistent behavior between read() and frame()
        try {
            candidate1.read(ImmutableSet.of("resume"), candidate2);
            Assert.fail("read() should throw exception for denied access");
        }
        catch (RestrictedAccessException e) {
            // Expected
        }

        Map<String, Object> frameResult = candidate1
                .frame(ImmutableSet.of("resume"), candidate2);
        Assert.assertFalse(frameResult.containsKey("resume"));
    }

    // ========================================================================
    // REAL-WORLD SCENARIO TESTS
    // ========================================================================

    @Test
    public void testRoleBasedAccessPatterns() {
        // Set up multi-role scenario
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@system.com";
        admin.department = "IT";

        Employer company = new Employer();
        company.name = "TechCorp";
        company.size = 100;

        EmployerUser hrManager = new EmployerUser();
        hrManager.name = "HR Manager";
        hrManager.email = "hr@techcorp.com";
        hrManager.employer = company;
        hrManager.role = "HR";

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.resume = "Sensitive resume content";
        candidate.skills = "Java, Python";

        // Test role hierarchy - Admin should override all restrictions
        Map<String, Object> adminResult = admin
                .frame(ImmutableSet.of("resume", "skills", "email"), candidate);
        Assert.assertTrue("Admin should see all fields",
                adminResult.containsKey("resume"));
        Assert.assertTrue("Admin should see all fields",
                adminResult.containsKey("skills"));
        Assert.assertTrue("Admin should see all fields",
                adminResult.containsKey("email"));

        // HR Manager should see professional info but not resume
        Map<String, Object> hrResult = hrManager
                .frame(ImmutableSet.of("resume", "skills", "email"), candidate);
        System.out.println(hrResult);
        Assert.assertNotNull(hrResult);
        Assert.assertFalse("HR should not see resume",
                hrResult.containsKey("resume"));
        Assert.assertTrue("HR should see skills",
                hrResult.containsKey("skills"));
        Assert.assertTrue("HR should see email", hrResult.containsKey("email"));

        // Candidate should see their own data
        Map<String, Object> selfResult = candidate
                .frame(ImmutableSet.of("resume", "skills", "email"), candidate);
        Assert.assertTrue("Candidate should see own resume",
                selfResult.containsKey("resume"));
        Assert.assertTrue("Candidate should see own skills",
                selfResult.containsKey("skills"));
        Assert.assertTrue("Candidate should see own email",
                selfResult.containsKey("email"));
    }

    @Test
    public void testOwnershipBasedAccess() {
        Candidate candidate1 = new Candidate();
        candidate1.name = "Alice";
        candidate1.email = "alice@email.com";

        Candidate candidate2 = new Candidate();
        candidate2.name = "Bob";
        candidate2.email = "bob@email.com";

        Job job = new Job();
        job.title = "Software Engineer";
        job.published = true;

        // Create applications for both candidates
        Application aliceApp = new Application();
        aliceApp.candidate = candidate1;
        aliceApp.job = job;
        aliceApp.coverLetter = "Alice's cover letter";

        Application bobApp = new Application();
        bobApp.candidate = candidate2;
        bobApp.job = job;
        bobApp.coverLetter = "Bob's cover letter";

        // Test ownership-based access - candidates can see their own
        // applications
        Map<String, Object> aliceOwnResult = candidate1
                .frame(ImmutableSet.of("coverLetter", "job.title"), aliceApp);
        Assert.assertNotNull("Alice should access her own application",
                aliceOwnResult);
        Assert.assertTrue(aliceOwnResult.containsKey("coverLetter"));
        Assert.assertTrue(aliceOwnResult.containsKey("job"));

        // Alice cannot see Bob's application
        Map<String, Object> aliceOtherResult = candidate1
                .frame(ImmutableSet.of("coverLetter", "job.title"), bobApp);
        Assert.assertNull("Alice should not access Bob's application",
                aliceOtherResult);

        // Test ownership through navigation
        Offer offer = new Offer();
        offer.candidate = candidate1;
        offer.application = aliceApp;
        offer.job = job;
        offer.salary = 100000.0;

        Map<String, Object> offerResult = candidate1.frame(
                ImmutableSet.of("salary", "application.coverLetter"), offer);
        Assert.assertNotNull("Alice should access her own offer", offerResult);
        Assert.assertTrue(offerResult.containsKey("salary"));
        Assert.assertTrue(offerResult.containsKey("application"));
    }

    @Test
    public void testTimeBasedAccessControl() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";

        Employer company = new Employer();
        company.name = "TechCorp";

        // Create job with deadline in the future
        Job activeJob = new Job();
        activeJob.title = "Active Position";
        activeJob.employer = company;
        activeJob.published = true;
        activeJob.deadline = Timestamp
                .fromJoda(Timestamp.now().getJoda().plusDays(30));

        // Create job with deadline in the past
        Job expiredJob = new Job();
        expiredJob.title = "Expired Position";
        expiredJob.employer = company;
        expiredJob.published = true;
        expiredJob.deadline = Timestamp
                .fromJoda(Timestamp.now().getJoda().minusDays(1));

        // Create unpublished job
        Job draftJob = new Job();
        draftJob.title = "Draft Position";
        draftJob.employer = company;
        draftJob.published = false;

        // Test access to active job
        Assert.assertTrue("Active job should be discoverable by candidate",
                activeJob.$isDiscoverableBy(candidate));
        Map<String, Object> activeResult = candidate
                .frame(ImmutableSet.of("title", "description"), activeJob);
        Assert.assertNotNull("Candidate should access active job",
                activeResult);
        Assert.assertTrue(activeResult.containsKey("title"));

        // Test access to expired job
        Assert.assertFalse(
                "Expired job should not be discoverable by candidate",
                expiredJob.$isDiscoverableBy(candidate));
        Map<String, Object> expiredResult = candidate
                .frame(ImmutableSet.of("title", "description"), expiredJob);
        Assert.assertEquals("Candidate should not access expired job", null,
                expiredResult);

        // Test access to draft job
        Assert.assertFalse("Draft job should not be discoverable by candidate",
                draftJob.$isDiscoverableBy(candidate));
        Map<String, Object> draftResult = candidate
                .frame(ImmutableSet.of("title", "description"), draftJob);
        Assert.assertEquals("Candidate should not access draft job", null,
                draftResult);

        // Employer user should see all their company's jobs regardless of state
        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.employer = company;

        Assert.assertTrue("Employer should see their expired job",
                expiredJob.$isDiscoverableBy(employerUser));
        Assert.assertTrue("Employer should see their draft job",
                draftJob.$isDiscoverableBy(employerUser));
    }

    @Test
    public void testComplexBusinessLogicMultiTenant() {
        // Set up multi-tenant scenario
        Employer company1 = new Employer();
        company1.name = "TechCorp";

        Employer company2 = new Employer();
        company2.name = "StartupInc";

        EmployerUser hr1 = new EmployerUser();
        hr1.name = "TechCorp HR";
        hr1.email = "hr@techcorp.com";
        hr1.employer = company1;

        EmployerUser hr2 = new EmployerUser();
        hr2.name = "StartupInc HR";
        hr2.email = "hr@startup.com";
        hr2.employer = company2;

        Job job1 = new Job();
        job1.title = "TechCorp Position";
        job1.employer = company1;
        job1.published = true;

        Job job2 = new Job();
        job2.title = "Startup Position";
        job2.employer = company2;
        job2.published = true;

        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";

        Application app1 = new Application();
        app1.candidate = candidate;
        app1.job = job1;
        app1.coverLetter = "Application to TechCorp";

        Application app2 = new Application();
        app2.candidate = candidate;
        app2.job = job2;
        app2.coverLetter = "Application to StartupInc";

        // HR1 should only see applications for their company's jobs
        Assert.assertTrue("HR1 should see application to their job",
                app1.$isDiscoverableBy(hr1));
        Assert.assertFalse(
                "HR1 should not see application to other company's job",
                app2.$isDiscoverableBy(hr1));

        Map<String, Object> hr1Result = hr1
                .frame(ImmutableSet.of("coverLetter", "candidate.name"), app1);
        Assert.assertNotNull("HR1 should access their company's application",
                hr1Result);

        Map<String, Object> hr1CrossResult = hr1
                .frame(ImmutableSet.of("coverLetter", "candidate.name"), app2);
        Assert.assertNull("HR1 should not access other company's application",
                hr1CrossResult);

        // HR2 should only see applications for their company's jobs
        Assert.assertTrue("HR2 should see application to their job",
                app2.$isDiscoverableBy(hr2));
        Assert.assertFalse(
                "HR2 should not see application to other company's job",
                app1.$isDiscoverableBy(hr2));
    }

    @Test
    public void testDataPrivacyScenarios() {
        Employer company = new Employer();
        company.name = "PrivacyCorp";

        EmployerUser hr = new EmployerUser();
        hr.name = "HR Manager";
        hr.email = "hr@privacycorp.com";
        hr.employer = company;

        Job job = new Job();
        job.title = "Privacy Engineer";
        job.employer = company;
        job.salary = 150000.0; // Sensitive information
        job.published = true;

        Candidate candidate = new Candidate();
        candidate.name = "Privacy Expert";
        candidate.email = "expert@email.com";
        candidate.resume = "Highly sensitive resume with personal details";
        candidate.skills = "Privacy Engineering, GDPR Compliance";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;
        application.notes = "Internal HR notes - very sensitive";

        // Test separation of public vs private information
        Audience anonymous = Audience.anonymous();
        Map<String, Object> publicJobInfo = anonymous
                .frame(ImmutableSet.of("title", "salary", "description"), job);
        Assert.assertNotNull(publicJobInfo);
        Assert.assertTrue("Job title should be public",
                publicJobInfo.containsKey("title"));
        Assert.assertFalse("Salary should not be public",
                publicJobInfo.containsKey("salary"));

        // Test HR access to candidate data through application
        Map<String, Object> hrCandidateAccess = hr.frame(ImmutableSet.of(
                "candidate.resume", "candidate.skills", "notes"), application);
        Assert.assertNotNull(hrCandidateAccess);
        Assert.assertTrue(hrCandidateAccess.containsKey("candidate"));
        Assert.assertTrue(hrCandidateAccess.containsKey("notes"));

        Map<String, Object> candidateData = (Map<String, Object>) hrCandidateAccess
                .get("candidate");
        Assert.assertTrue("HR should see candidate skills",
                candidateData.containsKey("skills"));
        Assert.assertFalse("HR should not see candidate resume",
                candidateData.containsKey("resume"));

        // Test candidate's own data access
        Map<String, Object> selfAccess = candidate
                .frame(ImmutableSet.of("resume", "skills", "email"), candidate);
        Assert.assertTrue("Candidate should see own resume",
                selfAccess.containsKey("resume"));
        Assert.assertTrue("Candidate should see own skills",
                selfAccess.containsKey("skills"));
        Assert.assertTrue("Candidate should see own email",
                selfAccess.containsKey("email"));

        // Test cross-candidate privacy
        Candidate otherCandidate = new Candidate();
        otherCandidate.name = "Other Developer";

        Map<String, Object> crossAccess = otherCandidate
                .frame(ImmutableSet.of("resume", "skills", "email"), candidate);
        Assert.assertFalse(crossAccess.containsKey("resume"));
        Assert.assertFalse(crossAccess.containsKey("skills"));
        Assert.assertFalse(crossAccess.containsKey("email"));
    }

}