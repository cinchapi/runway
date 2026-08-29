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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Selection;
import com.cinchapi.runway.Selections;
import com.cinchapi.runway.SuppressedRunwayException;
import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.TransactionInterface;

/**
 * Tests for the {@link com.cinchapi.runway.Transactional Transactional}
 * operations on an {@link Audience}: {@link Audience#startTransaction()
 * startTransaction}, {@link Audience#transact(java.util.function.Consumer)
 * transact} and {@link Audience#transactAndSupply(java.util.function.Function)
 * transactAndSupply}.
 *
 * @author Jeff Nelson
 */
public class AudienceTransactionalTest extends AudienceAccessControlBaseTest {

    /**
     * Return a saved {@link Admin} that is bound to the test {@link #runway}.
     *
     * @return the {@link Admin}
     */
    private Admin createAdmin() {
        Admin admin = new Admin();
        admin.email = "admin@company.com";
        admin.name = "System Admin";
        admin.assign(runway);
        Assert.assertTrue(admin.save());
        return admin;
    }

    /**
     * Return a saved {@link Candidate} named "Jane Developer" that is bound to
     * the test {@link #runway}.
     *
     * @return the {@link Candidate}
     */
    private Candidate createCandidate() {
        Candidate candidate = new Candidate();
        candidate.email = "jane@example.com";
        candidate.name = "Jane Developer";
        candidate.assign(runway);
        Assert.assertTrue(candidate.save());
        return candidate;
    }

    /**
     * Return a {@link Criteria} that matches records whose email is
     * {@code jane@example.com}.
     *
     * @return the {@link Criteria}
     */
    private Criteria janeEmailCriteria() {
        return Criteria.where().key("email").operator(Operator.EQUALS)
                .value("jane@example.com").build();
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} created through an
     * {@link Audience} within a staged {@link Transaction} stages within it and
     * becomes durable only after the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.startTransaction()} to start a
     * {@link Transaction}.</li>
     * <li>Create a {@link Candidate} through the {@link Admin} and
     * {@code save()} it.</li>
     * <li>Search for the {@link Candidate} through the enclosing
     * {@link #runway} before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Candidate} is invisible outside the
     * transaction before the commit and visible after it.
     */
    @Test
    public void testAudienceCreateAndSaveStageWithinTransaction() {
        Admin admin = createAdmin();
        try (Transaction transaction = admin.startTransaction()) {
            Candidate candidate = admin.create(Candidate.class);
            candidate.email = "jane@example.com";
            candidate.name = "Jane Developer";
            Assert.assertTrue(candidate.save());
            Assert.assertTrue(runway.find(Candidate.class, janeEmailCriteria())
                    .isEmpty());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(1,
                runway.find(Candidate.class, janeEmailCriteria()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience Audience's}
     * database operations resolve within the {@link Transaction} it staged, so
     * they observe staged writes that readers outside the transaction do not.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.startTransaction()}.</li>
     * <li>Load the {@link Candidate} through the transaction, change its name
     * and {@code save()}.</li>
     * <li>Load the {@link Candidate} through the {@link Admin} and through the
     * enclosing {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load through the {@link Admin} observes
     * the staged name; the load through the {@link #runway} observes the
     * original name.
     */
    @Test
    public void testAudienceOperationsObserveStagedWritesWithinTransaction() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        try (Transaction transaction = admin.startTransaction()) {
            Candidate inside = transaction.load(Candidate.class,
                    candidate.id());
            inside.name = "Janet Developer";
            Assert.assertTrue(inside.save());
            Candidate viaAudience = admin.load(Candidate.class, candidate.id());
            Assert.assertEquals("Janet Developer", viaAudience.name);
            Candidate outside = runway.load(Candidate.class, candidate.id());
            Assert.assertEquals("Jane Developer", outside.name);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a close without a commit discards
     * every write an {@link Audience} staged within the {@link Transaction} it
     * started.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.startTransaction()} in a try-with-resources
     * block.</li>
     * <li>Create a {@link Candidate} through the {@link Admin} and
     * {@code save()} it.</li>
     * <li>Exit the block without a {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Candidate} does not exist outside
     * the transaction.
     */
    @Test
    public void testCloseWithoutCommitDiscardsAudienceStagedWrites() {
        Admin admin = createAdmin();
        try (Transaction transaction = admin.startTransaction()) {
            Candidate candidate = admin.create(Candidate.class);
            candidate.email = "jane@example.com";
            candidate.name = "Jane Developer";
            Assert.assertTrue(candidate.save());
        }
        Assert.assertTrue(
                runway.find(Candidate.class, janeEmailCriteria()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that an access-checked {@link Audience}
     * write stages within the {@link Transaction} the {@link Audience} staged.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.startTransaction()}.</li>
     * <li>Load the {@link Candidate} through the {@link Admin} and change its
     * name with {@code admin.write(...)}, then {@code save()}.</li>
     * <li>Load the {@link Candidate} through the enclosing {@link #runway}
     * before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The name change is invisible outside the
     * transaction before the commit and visible after it.
     */
    @Test
    public void testAudienceWriteStagesWithinTransaction() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        try (Transaction transaction = admin.startTransaction()) {
            Candidate inside = admin.load(Candidate.class, candidate.id());
            admin.write("name", "Janet Developer", inside);
            Assert.assertTrue(inside.save());
            Assert.assertEquals("Jane Developer",
                    runway.load(Candidate.class, candidate.id()).name);
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("Janet Developer",
                runway.load(Candidate.class, candidate.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} that is already
     * bound to an open {@link Transaction} refuses to stage another one.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with
     * {@code runway.startTransaction()}.</li>
     * <li>Load the {@link Admin} through the transaction.</li>
     * <li>Call {@code startTransaction()} on the loaded {@link Admin}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws an
     * {@link IllegalStateException}.
     */
    @Test
    public void testStartTransactionThrowsWhenAudienceBoundToOpenTransaction() {
        Admin admin = createAdmin();
        try (Transaction outer = runway.startTransaction()) {
            Admin inside = outer.load(Admin.class, admin.id());
            try {
                inside.startTransaction();
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {
                // expected
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code transactAndSupply} on an
     * {@link Audience} that is not bound to an open {@link Transaction} runs
     * the work in its own transaction and commits it.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.transactAndSupply(...)} with work that loads the
     * {@link Candidate} through the transaction, changes its name and
     * {@code save()}s it.</li>
     * <li>Load the {@link Candidate} through the enclosing
     * {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The work's result is returned and the name
     * change is durable.
     */
    @Test
    public void testTransactAndSupplyCommitsOwnTransactionWhenAudienceNotInTransaction() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        String name = admin.transactAndSupply(transaction -> {
            Candidate inside = transaction.load(Candidate.class,
                    candidate.id());
            inside.name = "Janet Developer";
            Assert.assertTrue(inside.save());
            return inside.name;
        });
        Assert.assertEquals("Janet Developer", name);
        Assert.assertEquals("Janet Developer",
                runway.load(Candidate.class, candidate.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code transactAndSupply} on an
     * {@link Audience} that is bound to an open {@link Transaction} joins it
     * instead of committing on its own.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@code runway.startTransaction()}
     * and load the {@link Admin} through it.</li>
     * <li>Call {@code transactAndSupply(...)} on the loaded {@link Admin} with
     * work that changes the {@link Candidate Candidate's} name and
     * {@code save()}s it.</li>
     * <li>Load the {@link Candidate} through the enclosing {@link #runway}
     * before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The name change is invisible outside the
     * transaction before the commit and visible after it.
     */
    @Test
    public void testTransactAndSupplyJoinsOpenTransactionWhenAudienceBound() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        try (Transaction outer = runway.startTransaction()) {
            Admin inside = outer.load(Admin.class, admin.id());
            inside.transactAndSupply(transaction -> {
                Candidate subject = transaction.load(Candidate.class,
                        candidate.id());
                subject.name = "Janet Developer";
                Assert.assertTrue(subject.save());
                return null;
            });
            Assert.assertEquals("Jane Developer",
                    runway.load(Candidate.class, candidate.id()).name);
            Assert.assertTrue(outer.commit());
        }
        Assert.assertEquals("Janet Developer",
                runway.load(Candidate.class, candidate.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code transact} on an
     * {@link Audience} executes work in the audience's transactional scope and
     * commits it.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.transact(...)} with work that loads the
     * {@link Candidate} through the transaction, changes its name and
     * {@code save()}s it.</li>
     * <li>Load the {@link Candidate} through the enclosing
     * {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The name change is durable.
     */
    @Test
    public void testTransactExecutesWorkInAudienceScope() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        admin.transact(transaction -> {
            Candidate inside = transaction.load(Candidate.class,
                    candidate.id());
            inside.name = "Janet Developer";
            Assert.assertTrue(inside.save());
        });
        Assert.assertEquals("Janet Developer",
                runway.load(Candidate.class, candidate.id()).name);
    }

    /**
     * Return a saved {@link Application} that belongs to the "Jane Developer"
     * {@link Candidate}. Another {@link Candidate} cannot discover, read or
     * write it, so it is invisible to the viewer audience.
     *
     * @return the {@link Application}
     */
    private Application createHiddenApplication() {
        Employer employer = new Employer();
        employer.name = "Acme Corp";
        employer.assign(runway);
        Assert.assertTrue(employer.save());
        Job job = new Job();
        job.title = "Engineer";
        job.employer = employer;
        job.assign(runway);
        Assert.assertTrue(job.save());
        Candidate jane = createCandidate();
        Application application = new Application();
        application.candidate = jane;
        application.job = job;
        application.assign(runway);
        Assert.assertTrue(application.save());
        return application;
    }

    /**
     * Return a saved {@link Candidate} named "Victor Viewer" that is bound to
     * the test {@link #runway}. A {@link Candidate} cannot see another
     * {@link Candidate Candidate's} {@link Application} and cannot create
     * {@link Job Jobs}, so this is the restricted audience for scoping tests.
     *
     * @return the {@link Candidate}
     */
    private Candidate createViewer() {
        Candidate viewer = new Candidate();
        viewer.email = "viewer@example.com";
        viewer.name = "Victor Viewer";
        viewer.assign(runway);
        Assert.assertTrue(viewer.save());
        return viewer;
    }

    /**
     * Return a {@link Criteria} that matches records whose status is
     * {@code submitted}.
     *
     * @return the {@link Criteria}
     */
    private Criteria submittedStatusCriteria() {
        return Criteria.where().key("status").operator(Operator.EQUALS)
                .value("submitted").build();
    }

    /**
     * <strong>Goal:</strong> Verify that reads against the {@link Transaction}
     * an {@link Audience} staged observe the {@link Audience Audience's}
     * visibility, not the raw database.
     * <p>
     * <strong>Start state:</strong> A saved viewer {@link Candidate} and a
     * saved {@link Application} that belongs to another {@link Candidate}, all
     * bound to the {@link #runway}. The {@link Application} is invisible to the
     * viewer.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a {@link Transaction} from the viewer {@link Candidate}.</li>
     * <li>Find the {@link Application} through the transaction and through the
     * enclosing {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The find through the transaction returns
     * nothing; the find through the {@link #runway} returns the
     * {@link Application}.
     */
    @Test
    public void testTransactionReadsFilterAudienceVisibility() {
        Candidate viewer = createViewer();
        createHiddenApplication();
        try (Transaction transaction = viewer.startTransaction()) {
            Assert.assertTrue(transaction
                    .find(Application.class, submittedStatusCriteria())
                    .isEmpty());
            Assert.assertEquals(1, runway
                    .find(Application.class, submittedStatusCriteria()).size());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a create through the
     * {@link Transaction} an {@link Audience} staged enforces the
     * {@link Audience Audience's} create permission.
     * <p>
     * <strong>Start state:</strong> A saved {@link Candidate} bound to the
     * {@link #runway}. A {@link Candidate} is not permitted to create a
     * {@link Job}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a {@link Transaction} from the {@link Candidate}.</li>
     * <li>Call {@code transaction.create(Job.class)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws a
     * {@link RestrictedAccessException}.
     */
    @Test
    public void testTransactionCreateRequiresAudiencePermission() {
        Candidate viewer = createViewer();
        try (Transaction transaction = viewer.startTransaction()) {
            try {
                transaction.create(Job.class);
                Assert.fail("Expected a RestrictedAccessException");
            }
            catch (RestrictedAccessException e) {
                // expected
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a create through the
     * {@link Transaction} an {@link Audience} staged is refused after a failed
     * save poisons the transaction, the same as an unmediated create.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a {@link Transaction} from the {@link Admin}.</li>
     * <li>Save a {@link Candidate} whose {@code Required} name is {@code null},
     * so the save throws and poisons the transaction.</li>
     * <li>Call {@code transaction.create(Candidate.class)}.</li>
     * <li>Call {@code abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link SuppressedRunwayException}, the create is refused with an
     * {@link IllegalStateException} and the abort succeeds.
     */
    @Test
    public void testTransactionCreateRefusedAfterFailedSavePoisons() {
        Admin admin = createAdmin();
        try (Transaction transaction = admin.startTransaction()) {
            Candidate invalid = admin.create(Candidate.class);
            invalid.email = "jane@example.com";
            try {
                transaction.save(invalid);
                Assert.fail("Expected the save to throw");
            }
            catch (SuppressedRunwayException e) {
                // expected
            }
            try {
                transaction.create(Candidate.class);
                Assert.fail("Expected the create to be refused");
            }
            catch (IllegalStateException e) {
                // expected
            }
            transaction.abort();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a create through the
     * {@link Transaction} an {@link Audience} staged is refused on a thread
     * that does not own the transaction, the same as an unmediated create.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a {@link Transaction} from the {@link Admin}.</li>
     * <li>Call {@code transaction.create(Candidate.class)} from a second thread
     * and {@code join()} it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call on the second thread throws an
     * {@link IllegalStateException}.
     */
    @Test
    public void testTransactionCreateRefusedFromNonOwnerThread()
            throws InterruptedException {
        Admin admin = createAdmin();
        try (Transaction transaction = admin.startTransaction()) {
            AtomicBoolean refused = new AtomicBoolean(false);
            Thread thread = new Thread(() -> {
                try {
                    transaction.create(Candidate.class);
                }
                catch (IllegalStateException e) {
                    refused.set(true);
                }
            });
            thread.start();
            thread.join(10000);
            Assert.assertTrue(refused.get());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an ended {@link Transaction} view
     * refuses database operations after its {@link Audience} joins another
     * {@link Transaction}, instead of following the new scope.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a first {@link Transaction} from the {@link Admin} and
     * {@code commit()} it.</li>
     * <li>Stage a second {@link Transaction} from the {@link Admin} and save a
     * {@link Candidate} within it, without a commit.</li>
     * <li>Call {@code find(...)} and {@code create(...)} on the ended first
     * view.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Each call on the ended view throws an
     * {@link IllegalStateException}; neither observes the second
     * {@link Transaction Transaction's} staged state.
     */
    @Test
    public void testEndedTransactionViewRefusedWhenAudienceJoinsAnotherTransaction() {
        Admin admin = createAdmin();
        Transaction first = admin.startTransaction();
        Assert.assertTrue(first.commit());
        try (Transaction second = admin.startTransaction()) {
            Candidate candidate = second.create(Candidate.class);
            candidate.email = "jane@example.com";
            candidate.name = "Jane Developer";
            Assert.assertTrue(candidate.save());
            try {
                first.find(Candidate.class, janeEmailCriteria());
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {
                // expected
            }
            try {
                first.create(Candidate.class);
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {
                // expected
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an ended {@link Transaction} view
     * falls through to the enclosing {@link com.cinchapi.runway.Runway Runway}
     * while its {@link Audience} has not joined another {@link Transaction}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a {@link Transaction} from the {@link Admin}, save a
     * {@link Candidate} within it and {@code commit()}.</li>
     * <li>Call {@code find(...)} on the ended view.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The find resolves against the enclosing
     * {@link com.cinchapi.runway.Runway Runway} and returns the committed
     * {@link Candidate}.
     */
    @Test
    public void testEndedTransactionViewFallsThroughToRunway() {
        Admin admin = createAdmin();
        Transaction transaction = admin.startTransaction();
        Candidate candidate = transaction.create(Candidate.class);
        candidate.email = "jane@example.com";
        candidate.name = "Jane Developer";
        Assert.assertTrue(candidate.save());
        Assert.assertTrue(transaction.commit());
        Assert.assertEquals(1,
                transaction.find(Candidate.class, janeEmailCriteria()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that the view {@code transactAndSupply}
     * hands to work observes the {@link Audience Audience's} visibility, not
     * the raw database.
     * <p>
     * <strong>Start state:</strong> A saved viewer {@link Candidate} and a
     * saved {@link Application} that belongs to another {@link Candidate}, all
     * bound to the {@link #runway}. The {@link Application} is invisible to the
     * viewer.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code transactAndSupply(...)} on the viewer {@link Candidate}
     * with work that finds the {@link Application} through the view it
     * receives.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The find through the view returns nothing; the
     * same find through the enclosing {@link #runway} returns the
     * {@link Application}.
     */
    @Test
    public void testTransactAndSupplyViewReadsFilterAudienceVisibility() {
        Candidate viewer = createViewer();
        createHiddenApplication();
        boolean visible = viewer.transactAndSupply(transaction -> !transaction
                .find(Application.class, submittedStatusCriteria()).isEmpty());
        Assert.assertFalse(visible);
        Assert.assertEquals(1, runway
                .find(Application.class, submittedStatusCriteria()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} operates against
     * the enclosing {@link com.cinchapi.runway.Runway Runway} again after the
     * {@link Transaction} it staged ends.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a {@link Transaction} with {@code admin.startTransaction()} and
     * {@code commit()} it.</li>
     * <li>Create a {@link Candidate} through the {@link Admin} and
     * {@code save()} it outside of any transaction.</li>
     * <li>Call {@code admin.startTransaction()} a second time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save is immediately visible through the
     * enclosing {@link #runway}, and the second {@code startTransaction()}
     * returns an open {@link Transaction} instead of throwing an
     * {@link IllegalStateException}.
     */
    @Test
    public void testAudienceOperatesAgainstRunwayAfterTransactionEnds() {
        Admin admin = createAdmin();
        try (Transaction transaction = admin.startTransaction()) {
            Assert.assertTrue(transaction.commit());
        }
        Candidate candidate = admin.create(Candidate.class);
        candidate.email = "jane@example.com";
        candidate.name = "Jane Developer";
        Assert.assertTrue(candidate.save());
        Assert.assertEquals(1,
                runway.find(Candidate.class, janeEmailCriteria()).size());
        try (Transaction transaction = admin.startTransaction()) {
            Assert.assertTrue(transaction.commit());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code scope} refuses a
     * {@link Transaction} that the {@link Audience} has not joined, instead of
     * returning a view whose reads and writes resolve in different scopes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@code runway.startTransaction()},
     * without a load of the {@link Admin} through it.</li>
     * <li>Call {@code admin.scope(transaction)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws an
     * {@link IllegalStateException}.
     */
    @Test
    public void testScopeThrowsWhenAudienceHasNotJoinedTransaction() {
        Admin admin = createAdmin();
        try (Transaction transaction = runway.startTransaction()) {
            try {
                admin.scope(transaction);
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {
                // expected
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code scope} re-scopes another
     * {@link Audience Audience's} view of the same {@link Transaction} to the
     * calling {@link Audience}, so the returned view never carries the wrong
     * audience.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin}, a saved viewer
     * {@link Candidate} and a saved {@link Application} that is invisible to
     * the viewer, all bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@code runway.startTransaction()}
     * and load the {@link Admin} and the viewer {@link Candidate} through
     * it.</li>
     * <li>Build the viewer's view with {@code viewer.scope(transaction)}.</li>
     * <li>Call {@code admin.scope(...)} with the viewer's view.</li>
     * <li>Find the {@link Application} through the returned view.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The find returns the {@link Application},
     * because the returned view observes the {@link Admin Admin's} visibility,
     * not the viewer's.
     */
    @Test
    public void testScopeRewrapsAnotherAudiencesViewForThisAudience() {
        Admin admin = createAdmin();
        Candidate viewer = createViewer();
        createHiddenApplication();
        try (Transaction transaction = runway.startTransaction()) {
            Admin adminInside = transaction.load(Admin.class, admin.id());
            Candidate viewerInside = transaction.load(Candidate.class,
                    viewer.id());
            TransactionInterface viewerView = viewerInside.scope(transaction);
            Assert.assertTrue(viewerView
                    .find(Application.class, submittedStatusCriteria())
                    .isEmpty());
            TransactionInterface adminView = adminInside.scope(viewerView);
            Assert.assertEquals(1, adminView
                    .find(Application.class, submittedStatusCriteria()).size());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code scope} refuses a
     * {@link TransactionInterface} view that is not a {@link Transaction},
     * instead of returning it with no audience mediation.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} bound to the
     * {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link TransactionInterface} stub that is not a
     * {@link Transaction}.</li>
     * <li>Call {@code admin.scope(...)} with the stub.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws an
     * {@link IllegalArgumentException}.
     */
    @Test
    public void testScopeRefusesViewThatIsNotATransaction() {
        Admin admin = createAdmin();
        TransactionInterface view = new TransactionInterface() {

            @Override
            public void afterAbort(Runnable hook) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void afterCommit(Runnable hook) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T extends Record> T create(Class<T> clazz, Object... args) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T extends Record> T intern(T record) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean save(boolean preventStaleWrites, Record... records) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Transaction startTransaction() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T transactAndSupply(
                    Function<TransactionInterface, T> work) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Selections select(Selection<?>... selections) {
                throw new UnsupportedOperationException();
            }

        };
        try {
            admin.scope(view);
            Assert.fail("Expected an IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Return a {@link Criteria} that matches records whose email is
     * {@code restricted@example.com}.
     *
     * @return the {@link Criteria}
     */
    private Criteria restrictedEmailCriteria() {
        return Criteria.where().key("email").operator(Operator.EQUALS)
                .value("restricted@example.com").build();
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern()} on an
     * {@link Audience} record is an identity operation that does not apply the
     * audience's own create permission.
     * <p>
     * <strong>Start state:</strong> An unsaved {@link RestrictedUser} bound to
     * the {@link #runway}. Only an {@link Admin} may create a
     * {@link RestrictedUser}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link RestrictedUser}, {@code assign(...)} it to the
     * {@link #runway} and call {@code intern()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns the record itself, saved and
     * durable, instead of throwing a {@link RestrictedAccessException}.
     */
    @Test
    public void testInternDoesNotRequireAudienceCreatePermission() {
        RestrictedUser user = new RestrictedUser();
        user.email = "restricted@example.com";
        user.name = "Restricted User";
        user.assign(runway);
        RestrictedUser interned = user.intern();
        Assert.assertSame(user, interned);
        Assert.assertEquals(1, runway
                .find(RestrictedUser.class, restrictedEmailCriteria()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern()} on an
     * {@link Audience} record that is bound to an open {@link Transaction}
     * resolves within it as an identity operation, without the audience's own
     * create permission.
     * <p>
     * <strong>Start state:</strong> A saved {@link RestrictedUser} bound to the
     * {@link #runway}. Only an {@link Admin} may create a
     * {@link RestrictedUser}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with
     * {@code runway.startTransaction()}.</li>
     * <li>Load the {@link RestrictedUser} through the transaction and call
     * {@code intern()} on it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns the record that claims the
     * identity instead of throwing a {@link RestrictedAccessException}.
     */
    @Test
    public void testInternWithinTransactionDoesNotRequireAudienceCreatePermission() {
        RestrictedUser user = new RestrictedUser();
        user.email = "restricted@example.com";
        user.name = "Restricted User";
        user.assign(runway);
        Assert.assertTrue(user.save());
        try (Transaction transaction = runway.startTransaction()) {
            RestrictedUser inside = transaction.load(RestrictedUser.class,
                    user.id());
            RestrictedUser interned = inside.intern();
            Assert.assertEquals(user.id(), interned.id());
        }
    }

}
