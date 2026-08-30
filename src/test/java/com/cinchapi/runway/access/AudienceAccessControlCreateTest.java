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

import com.cinchapi.runway.Runway;
import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.access.AudienceAccessControlInternTest.Gate;
import com.cinchapi.runway.access.AudienceAccessControlInternTest.Vault;

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

    /**
     * <strong>Goal:</strong> Verify that the create-permission check of
     * {@code create} resolves within the {@link Audience Audience's}
     * transactional scope, so a creation rule that reads through a linked
     * constructor argument observes the staged state.
     * <p>
     * <strong>Start state:</strong> One saved open {@link Gate}, one saved
     * {@link Admin}, and an open {@link Transaction} that stages the
     * {@link Gate} closed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Admin} through the {@link Transaction} and stage
     * {@code open = false} on the {@link Gate} within it.</li>
     * <li>Call {@code create} on the loaded {@link Admin} for a {@link Vault}
     * whose gate argument is a copy of the {@link Gate} that was loaded outside
     * the {@link Transaction}, and catch the expected exception.</li>
     * <li>{@code commit()} the same {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown
     * because the creation rule observes the staged closed {@link Gate}, and
     * the {@link Transaction} still commits.
     */
    @Test
    public void testCreatePermissionCheckResolvesWithinAudienceTransaction() {
        Gate gate = new Gate();
        gate.open = true;
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@example.com";
        runway.save(gate, admin);
        try (Transaction transaction = runway.startTransaction()) {
            Admin audience = transaction.load(Admin.class, admin.id());
            Gate staged = transaction.load(Gate.class, gate.id());
            staged.open = false;
            transaction.save(staged);
            Gate outside = runway.load(Gate.class, gate.id());
            boolean threw = false;
            try {
                audience.create(Vault.class, "V-1", outside);
            }
            catch (RestrictedAccessException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            Assert.assertTrue(transaction.commit());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a refused {@code create} through an
     * {@link Audience} that is bound to an open {@link Transaction} leaves the
     * constructor-argument records bound as they were, so a later direct save
     * of one does not stage into the transaction.
     * <p>
     * <strong>Start state:</strong> One saved closed {@link Gate} and one saved
     * {@link Admin}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} in a try-with-resources block and load
     * the {@link Admin} through it.</li>
     * <li>Load the {@link Gate} outside the {@link Transaction}.</li>
     * <li>Call {@code create} on the loaded {@link Admin} for a {@link Vault}
     * whose gate argument is the outside {@link Gate}, and catch the expected
     * exception.</li>
     * <li>Set {@code open = true} on the outside {@link Gate} and save it
     * directly.</li>
     * <li>{@code abort()} the same {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown
     * because the {@link Gate} is closed, and the direct save survives the
     * abort: a fresh load shows the {@link Gate} open.
     */
    @Test
    public void testRefusedCreateLeavesArgumentRecordsUnbound() {
        Gate gate = new Gate();
        gate.open = false;
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@example.com";
        runway.save(gate, admin);
        try (Transaction transaction = runway.startTransaction()) {
            Admin audience = transaction.load(Admin.class, admin.id());
            Gate outside = runway.load(Gate.class, gate.id());
            boolean threw = false;
            try {
                audience.create(Vault.class, "V-1", outside);
            }
            catch (RestrictedAccessException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            outside.open = true;
            Assert.assertTrue(outside.save());
            transaction.abort();
        }
        Assert.assertTrue(runway.load(Gate.class, gate.id()).open);
    }

    /**
     * <strong>Goal:</strong> Verify that a refused {@code create} through an
     * anonymous {@link Audience} that holds an open {@link Transaction} leaves
     * the constructor-argument records bound as they were, so a later direct
     * save of one does not stage into the transaction.
     * <p>
     * <strong>Start state:</strong> One saved open {@link Gate}. A
     * {@link Vault} is never creatable by an anonymous {@link Audience}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} in a try-with-resources block and obtain
     * an {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * anonymous} {@link Audience} that holds it.</li>
     * <li>Load the {@link Gate} outside the {@link Transaction}.</li>
     * <li>Call {@code create} on the anonymous {@link Audience} for a
     * {@link Vault} whose gate argument is the outside {@link Gate}, and catch
     * the expected exception.</li>
     * <li>Set {@code open = false} on the outside {@link Gate} and save it
     * directly.</li>
     * <li>{@code abort()} the same {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown
     * and the direct save survives the abort: a fresh load shows the
     * {@link Gate} closed.
     */
    @Test
    public void testAnonymousRefusedCreateLeavesArgumentRecordsUnbound() {
        Gate gate = new Gate();
        gate.open = true;
        runway.save(gate);
        try (Transaction transaction = runway.startTransaction()) {
            Audience anonymous = Audience.anonymous(transaction);
            Gate outside = runway.load(Gate.class, gate.id());
            boolean threw = false;
            try {
                anonymous.create(Vault.class, "V-1", outside);
            }
            catch (RestrictedAccessException e) {
                threw = true;
            }
            Assert.assertTrue(threw);
            outside.open = false;
            Assert.assertTrue(outside.save());
            transaction.abort();
        }
        Assert.assertFalse(runway.load(Gate.class, gate.id()).open);
    }

    /**
     * <strong>Goal:</strong> Verify that a refused {@code create} leaves a
     * constructor-argument record that holds no binding without one, so the
     * caller can still choose the scope that record saves within.
     * <p>
     * <strong>Start state:</strong> A second open {@link Runway}, so a new
     * record names no database until the caller assigns one. A {@link Vault} is
     * never creatable by an anonymous {@link Audience}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a second {@link Runway} in a try-with-resources block.</li>
     * <li>Start a {@link Transaction} in a try-with-resources block and obtain
     * an {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * anonymous} {@link Audience} that holds it.</li>
     * <li>Construct a {@link Gate} that is never assigned or saved.</li>
     * <li>Call {@code create} on the anonymous {@link Audience} for a
     * {@link Vault} whose gate argument is the new {@link Gate}, and catch the
     * expected exception.</li>
     * <li>{@code assign} the {@link Gate} to the first {@link Runway} and save
     * it directly.</li>
     * <li>{@code abort()} the same {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown,
     * the {@code assign} is accepted, and the direct save survives the abort:
     * the first {@link Runway} holds the {@link Gate}.
     */
    @Test
    public void testRefusedCreateLeavesUnboundArgumentRecordUnbound()
            throws Exception {
        try (Runway second = runwayBuilder().build()) {
            try (Transaction transaction = runway.startTransaction()) {
                Audience anonymous = Audience.anonymous(transaction);
                Gate fresh = new Gate();
                fresh.open = true;
                boolean threw = false;
                try {
                    anonymous.create(Vault.class, "V-1", fresh);
                }
                catch (RestrictedAccessException e) {
                    threw = true;
                }
                Assert.assertTrue(threw);
                fresh.assign(runway);
                Assert.assertTrue(fresh.save());
                transaction.abort();
            }
            Assert.assertEquals(1, runway.count(Gate.class));
        }
    }

}