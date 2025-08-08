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

import java.util.Set;

import javax.annotation.Nonnull;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Required;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.cinchapi.runway.Unique;
import com.google.common.collect.ImmutableSet;

/**
 * Base test class for access control tests using the {@link Audience}
 * construct.
 * <p>
 * This class defines the domain model for a job application system with
 * various user types and access control rules to demonstrate the framework's
 * capabilities.
 * </p>
 *
 * @author Jeff Nelson
 */
public abstract class AudienceAccessControlBaseTest extends RunwayBaseClientServerTest {

    // ========================================================================
    // USER HIERARCHY
    // ========================================================================

    /**
     * System administrator user type.
     */
    protected static class Admin extends User {

        public String department;

        public String permissions;

        // Admins inherit the default behavior which already gives them full
        // access
    }

    /**
     * Job candidate user type.
     */
    protected static class Candidate extends User {

        public String resume;

        public String skills;

        public Integer yearsExperience;

        public String location;

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            // Candidates can be discovered by admins and employer users, but
            // not by other candidates
            return audience instanceof Admin
                    || audience instanceof EmployerUser
                    || audience.equals(this);
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience.equals(this)) {
                return ALL_KEYS;
            }
            else if(audience instanceof EmployerUser) {
                // EmployerUsers can see candidate profiles
                return ImmutableSet.of("name", "email", "skills",
                        "yearsExperience", "location");
            }
            else if(audience instanceof Candidate) {
                // Candidates can't see other candidates' private info
                return ImmutableSet.of("name");
            }
            else {
                return ImmutableSet.of("name");
            }
        }
    }

    /**
     * Employer user type that represents users working for employers.
     */
    protected static class EmployerUser extends User {

        @Required
        public Employer employer;

        public String role;

        public String department;

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience.equals(this)) {
                return ALL_KEYS;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser other = (EmployerUser) audience;
                if(other.employer != null
                        && other.employer.equals(this.employer)) {
                    // Same employer can see more details
                    return ImmutableSet.of("name", "email", "role",
                            "department");
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
     * Base user class that implements both AccessControl and Audience.
     * All users can perform operations and have access controls applied to
     * them.
     */
    protected static abstract class User extends Record implements AccessControl, Audience {

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
            if(audience instanceof Admin) {
                return ALL_KEYS; // Admins can see everything
            }
            else if(audience.equals(this)) {
                return ALL_KEYS; // Users can see their own data
            }
            else if(audience instanceof EmployerUser) {
                return ImmutableSet.of("name", "email"); // Employers can see
                                                         // basic info
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
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience.equals(this)) {
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

    // ========================================================================
    // BUSINESS ENTITIES
    // ========================================================================

    /**
     * Job application entity.
     */
    protected static class Application extends Record implements AccessControl {

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
            if(audience instanceof Admin) {
                return true;
            }
            else if(audience instanceof Candidate) {
                return audience.equals(this.candidate);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return true;
            }
            else if(audience instanceof Candidate) {
                return audience.equals(this.candidate);
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null && this.job != null
                        && empUser.employer.equals(this.job.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof Candidate) {
                if(audience.equals(this.candidate)) {
                    return ImmutableSet.of("job", "coverLetter", "status",
                            "appliedAt");
                }
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null && this.job != null
                        && empUser.employer.equals(this.job.employer)) {
                    return ALL_KEYS; // Employer can see applications for their
                                     // jobs
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
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof Candidate) {
                if(audience.equals(this.candidate)) {
                    return ImmutableSet.of("coverLetter");
                }
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null && this.job != null
                        && empUser.employer.equals(this.job.employer)) {
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
     * Employer/Company entity.
     */
    protected static class Employer extends Record implements AccessControl {

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
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null && empUser.employer.equals(this)) {
                    return ALL_KEYS; // Own employer data
                }
                else {
                    return ImmutableSet.of("name", "description", "website",
                            "industry", "location");
                }
            }
            else {
                return ImmutableSet.of("name", "description", "website",
                        "industry", "location");
            }
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("name", "description", "website",
                    "industry", "location");
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null && empUser.employer.equals(this)) {
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

    /**
     * Job posting entity.
     */
    protected static class Job extends Record implements AccessControl {

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
            if(audience instanceof Admin) {
                return true;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null
                        && empUser.employer.equals(this.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return true;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null
                        && empUser.employer.equals(this.employer)) {
                    return true; // Own employer's jobs
                }
                else {
                    return published && (deadline == null
                            || Timestamp.now().compareTo(deadline) <= 0);
                }
            }
            else if(audience instanceof Candidate) {
                return published && (deadline == null
                        || Timestamp.now().compareTo(deadline) <= 0);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return published && (deadline == null
                    || Timestamp.now().compareTo(deadline) <= 0);
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null
                        && empUser.employer.equals(this.employer)) {
                    return ALL_KEYS; // Own employer's jobs
                }
                else {
                    // Other employers can see published jobs
                    if(published && (deadline == null
                            || Timestamp.now().compareTo(deadline) <= 0)) {
                        return ImmutableSet.of("title", "description",
                                "requirements", "location", "employer");
                    }
                }
            }
            else if(audience instanceof Candidate) {
                // Candidates can only see published jobs that haven't expired
                if(published && (deadline == null
                        || Timestamp.now().compareTo(deadline) <= 0)) {
                    return ImmutableSet.of("title", "description",
                            "requirements", "location", "employer");
                }
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            // Anonymous users can only see published jobs that haven't expired
            if(published && (deadline == null
                    || Timestamp.now().compareTo(deadline) <= 0)) {
                return ImmutableSet.of("title", "description", "requirements",
                        "location", "employer");
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null
                        && empUser.employer.equals(this.employer)) {
                    return ImmutableSet.of("title", "description",
                            "requirements", "location", "salary", "published",
                            "deadline");
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
    protected static class Offer extends Record implements AccessControl {

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
            if(audience instanceof Admin) {
                return true;
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null && this.job != null
                        && empUser.employer.equals(this.job.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return true;
            }
            else if(audience instanceof Candidate) {
                return audience.equals(this.candidate);
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                return empUser.employer != null && this.job != null
                        && empUser.employer.equals(this.job.employer);
            }
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof Candidate) {
                if(audience.equals(this.candidate)) {
                    return ALL_KEYS; // Candidates can see their offers
                }
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null && this.job != null
                        && empUser.employer.equals(this.job.employer)) {
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
            if(audience instanceof Admin) {
                return ALL_KEYS;
            }
            else if(audience instanceof Candidate) {
                if(audience.equals(this.candidate)) {
                    return ImmutableSet.of("status"); // Candidates can
                                                      // accept/reject offers
                }
            }
            else if(audience instanceof EmployerUser) {
                EmployerUser empUser = (EmployerUser) audience;
                if(empUser.employer != null && this.job != null
                        && empUser.employer.equals(this.job.employer)) {
                    return ImmutableSet.of("salary", "startDate", "benefits",
                            "status", "expiresAt");
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

}