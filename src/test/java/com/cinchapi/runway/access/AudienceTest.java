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

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Required;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.cinchapi.runway.Unique;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for access control using the {@link Audience} construct.
 * <p>
 * This test suite models a job application system with various user types
 * and access control rules to demonstrate the framework's capabilities.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("unchecked")
public class AudienceTest extends RunwayBaseClientServerTest {

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
                ImmutableSet.of("resume", "skills", "email", "yearsExperience"), candidate);

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
        Map<String, Object> result = admin.frame(
                ImmutableSet.of("candidate.resume", "candidate.skills", "notes"), application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("candidate"));
        Assert.assertTrue(result.containsKey("notes"));

        Map<String, Object> candidateData = (Map<String, Object>) result.get("candidate");
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

        Assert.assertEquals("Candidate should not be able to access another candidate's private fields", ImmutableMap.of(), result);
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
                ImmutableSet.of("coverLetter", "candidate.email"), otherApplication);

        // Should return null since candidate1 cannot discover other candidates' applications
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
        Map<String, Object> result = employerUser.frame(
                ImmutableSet.of("candidate.skills", "candidate.resume", "candidate.email"), application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("candidate"));

        Map<String, Object> candidateData = (Map<String, Object>) result.get("candidate");
        Assert.assertTrue(candidateData.containsKey("skills"));
        Assert.assertTrue(candidateData.containsKey("email"));
        Assert.assertFalse(candidateData.containsKey("resume")); // Resume not accessible to employers
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
                ImmutableSet.of("skills", "resume", "email", "location"), candidate);

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
                ImmutableSet.of("job.title", "job.salary", "coverLetter"), application);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("job"));
        Assert.assertTrue(result.containsKey("coverLetter"));

        // Candidate should see job title but not salary (not in job's readable fields for candidates)
        Map<String, Object> jobData = (Map<String, Object>) result.get("job");
        Assert.assertTrue(jobData.containsKey("title"));
        Assert.assertFalse(jobData.containsKey("salary"));
    }

    // ========================================================================
    // USER HIERARCHY
    // ========================================================================

    /**
     * System administrator user type.
     */
    class Admin extends User {

        public String department;

        public String permissions;

        // Admins inherit the default behavior which already gives them full access
    }

    /**
     * Job application entity.
     */
    class Application extends Record implements AccessControl {

        @Required
        public Candidate candidate;

        @Required
        public Job job;

        public String coverLetter;

        public String status = "submitted";

        public Timestamp appliedAt;

        public String notes; // Internal notes by employer

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return audience instanceof Admin || audience instanceof Candidate;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return true;
            }
            else if (audience instanceof Candidate) {
                return audience.equals(this.candidate);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return true;
            }
            else if (audience instanceof Candidate) {
                return audience.equals(this.candidate);
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null && this.job != null && empUser.employer.equals(this.job.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof Candidate) {
                if (audience.equals(this.candidate)) {
                    return ImmutableSet.of("job", "coverLetter", "status", "appliedAt");
                }
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && this.job != null && empUser.employer.equals(this.job.employer)) {
                    return ALL_KEYS; // Employer can see applications for their jobs
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof Candidate) {
                if (audience.equals(this.candidate)) {
                    return ImmutableSet.of("coverLetter");
                }
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && this.job != null && empUser.employer.equals(this.job.employer)) {
                    return ImmutableSet.of("status", "notes");
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public void deleteAs(Audience audience) {
            audience.delete(this);
        }
    }

    /**
     * Job candidate user type.
     */
    class Candidate extends User {

        public String resume;

        public String skills;

        public Integer yearsExperience;

        public String location;

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            // Candidates can be discovered by admins and employer users, but not by other candidates
            return audience instanceof Admin
                || audience instanceof EmployerUser
                || audience.equals(this);
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience.equals(this)) {
                return ALL_KEYS;
            }
            else if (audience instanceof EmployerUser) {
                // EmployerUsers can see candidate profiles
                return ImmutableSet.of("name", "email", "skills", "yearsExperience", "location");
            }
            else if (audience instanceof Candidate) {
                // Candidates can't see other candidates' private info
                return ImmutableSet.of("name");
            }
            else {
                return ImmutableSet.of("name");
            }
        }
    }

    /**
     * Employer/Company entity.
     */
    class Employer extends Record implements AccessControl {

        @Required
        @Unique
        public String name;

        public String description;

        public String website;

        public String industry;

        public String location;

        public Integer size;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return audience instanceof Admin || audience instanceof EmployerUser;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true; // Employers are publicly discoverable
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return true;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && empUser.employer.equals(this)) {
                    return ALL_KEYS; // Own employer data
                }
                else {
                    return ImmutableSet.of("name", "description", "website", "industry", "location");
                }
            }
            else {
                return ImmutableSet.of("name", "description", "website", "industry", "location");
            }
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("name", "description", "website", "industry", "location");
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && empUser.employer.equals(this)) {
                    return ImmutableSet.of("description", "website", "size");
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public void deleteAs(Audience audience) {
            audience.delete(this);
        }
    }

    // ========================================================================
    // BUSINESS ENTITIES
    // ========================================================================

    /**
     * Employer user type that represents users working for employers.
     */
    class EmployerUser extends User {

        @Required
        public Employer employer;

        public String role;

        public String department;

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience.equals(this)) {
                return ALL_KEYS;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser other = (EmployerUser) audience;
                if (other.employer != null && other.employer.equals(this.employer)) {
                    // Same employer can see more details
                    return ImmutableSet.of("name", "email", "role", "department");
                }
                else {
                    return ImmutableSet.of("name", "email");
                }
            }
            else {
                return ImmutableSet.of("name");
            }
        }
    }

    /**
     * Job posting entity.
     */
    class Job extends Record implements AccessControl {

        @Required
        public String title;

        @Required
        public Employer employer;

        public String description;

        public String requirements;

        public String location;

        public Double salary;

        public boolean published = false;

        public Timestamp deadline;

        public Timestamp createdAt;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return audience instanceof Admin || audience instanceof EmployerUser;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return true;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null && empUser.employer.equals(this.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return true;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && empUser.employer.equals(this.employer)) {
                    return true; // Own employer's jobs
                }
                else {
                    return published && (deadline == null || Timestamp.now().compareTo(deadline) <= 0);
                }
            }
            else if (audience instanceof Candidate) {
                return published && (deadline == null || Timestamp.now().compareTo(deadline) <= 0);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return published && (deadline == null || Timestamp.now().compareTo(deadline) <= 0);
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && empUser.employer.equals(this.employer)) {
                    return ALL_KEYS; // Own employer's jobs
                }
                else {
                    // Other employers can see published jobs
                    if (published && (deadline == null || Timestamp.now().compareTo(deadline) <= 0)) {
                        return ImmutableSet.of("title", "description", "requirements", "location", "employer");
                    }
                }
            }
            else if (audience instanceof Candidate) {
                // Candidates can only see published jobs that haven't expired
                if (published && (deadline == null || Timestamp.now().compareTo(deadline) <= 0)) {
                    return ImmutableSet.of("title", "description", "requirements", "location", "employer");
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            // Anonymous users can only see published jobs that haven't expired
            if (published && (deadline == null || Timestamp.now().compareTo(deadline) <= 0)) {
                return ImmutableSet.of("title", "description", "requirements", "location", "employer");
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && empUser.employer.equals(this.employer)) {
                    return ImmutableSet.of("title", "description", "requirements", "location", "salary", "published", "deadline");
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public void deleteAs(Audience audience) {
            audience.delete(this);
        }
    }

    /**
     * Job offer entity.
     */
    class Offer extends Record implements AccessControl {

        @Required
        public Application application;

        @Required
        public Job job;

        @Required
        public Candidate candidate;

        public Double salary;

        public Timestamp startDate;

        public String benefits;

        public String status = "pending";

        public Timestamp expiresAt;

        public Timestamp createdAt;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return audience instanceof Admin || audience instanceof EmployerUser;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return true;
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null && this.job != null && empUser.employer.equals(this.job.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return true;
            }
            else if (audience instanceof Candidate) {
                return audience.equals(this.candidate);
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null && this.job != null && empUser.employer.equals(this.job.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof Candidate) {
                if (audience.equals(this.candidate)) {
                    return ALL_KEYS; // Candidates can see their offers
                }
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && this.job != null && empUser.employer.equals(this.job.employer)) {
                    return ALL_KEYS; // Employer can see offers for their jobs
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience instanceof Candidate) {
                if (audience.equals(this.candidate)) {
                    return ImmutableSet.of("status"); // Candidates can accept/reject offers
                }
            }
            else if (audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if (empUser.employer != null && this.job != null && empUser.employer.equals(this.job.employer)) {
                    return ImmutableSet.of("salary", "startDate", "benefits", "status", "expiresAt");
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public void deleteAs(Audience audience) {
            audience.delete(this);
        }
    }

    /**
     * Base user class that implements both AccessControl and Audience.
     * All users can perform operations and have access controls applied to them.
     */
    abstract class User extends Record implements AccessControl, Audience {

        @Required
        @Unique
        public String email;

        @Required
        public String name;

        public String phone;

        public Timestamp lastLogin;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return true; // Allow user registration
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return audience instanceof Admin || audience.equals(this);
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return audience instanceof Admin
                || audience instanceof EmployerUser
                || audience.equals(this);
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS; // Admins can see everything
            }
            else if (audience.equals(this)) {
                return ALL_KEYS; // Users can see their own data
            }
            else if (audience instanceof EmployerUser) {
                return ImmutableSet.of("name", "email"); // Employers can see basic info
            }
            else {
                return ImmutableSet.of("name"); // Other users see only name
            }
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("name"); // Only name is public
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if (audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if (audience.equals(this)) {
                return ImmutableSet.of("name", "phone", "lastLogin");
            }
            else {
                return NO_KEYS;
            }
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public void deleteAs(Audience audience) {
            audience.delete(this);
        }
    }

}