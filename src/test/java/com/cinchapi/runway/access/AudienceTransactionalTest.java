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
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Transaction;

/**
 * Tests for the {@link com.cinchapi.runway.Transactional Transactional}
 * operations on an {@link Audience}: {@link Audience#stage() stage},
 * {@link Audience#startTransaction() startTransaction},
 * {@link Audience#run(java.util.function.Consumer) run} and
 * {@link Audience#supply(java.util.function.Function) supply}.
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
     * <li>Call {@code admin.stage()} to start a {@link Transaction}.</li>
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
    public void testAudienceCreateAndSaveStageWithinStagedTransaction() {
        Admin admin = createAdmin();
        try (Transaction transaction = admin.stage()) {
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
     * <li>Call {@code admin.startTransaction()} (the {@code stage()}
     * alias).</li>
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
     * <li>Call {@code admin.stage()} in a try-with-resources block.</li>
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
        try (Transaction transaction = admin.stage()) {
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
     * <li>Call {@code admin.stage()}.</li>
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
    public void testAudienceWriteStagesWithinStagedTransaction() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        try (Transaction transaction = admin.stage()) {
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
     * <li>Start a {@link Transaction} with {@code runway.stage()}.</li>
     * <li>Load the {@link Admin} through the transaction.</li>
     * <li>Call {@code stage()} on the loaded {@link Admin}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws an
     * {@link IllegalStateException}.
     */
    @Test
    public void testStageThrowsWhenAudienceBoundToOpenTransaction() {
        Admin admin = createAdmin();
        try (Transaction outer = runway.stage()) {
            Admin inside = outer.load(Admin.class, admin.id());
            try {
                inside.stage();
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {
                // expected
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code supply} on an {@link Audience}
     * that is not bound to an open {@link Transaction} runs the work in its own
     * transaction and commits it.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.supply(...)} with work that loads the
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
    public void testSupplyCommitsOwnTransactionWhenAudienceNotInTransaction() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        String name = admin.supply(transaction -> {
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
     * <strong>Goal:</strong> Verify that {@code supply} on an {@link Audience}
     * that is bound to an open {@link Transaction} joins it instead of
     * committing on its own.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@code runway.stage()} and load the
     * {@link Admin} through it.</li>
     * <li>Call {@code supply(...)} on the loaded {@link Admin} with work that
     * changes the {@link Candidate Candidate's} name and {@code save()}s
     * it.</li>
     * <li>Load the {@link Candidate} through the enclosing {@link #runway}
     * before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The name change is invisible outside the
     * transaction before the commit and visible after it.
     */
    @Test
    public void testSupplyJoinsOpenTransactionWhenAudienceBound() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        try (Transaction outer = runway.stage()) {
            Admin inside = outer.load(Admin.class, admin.id());
            inside.supply(transaction -> {
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
     * <strong>Goal:</strong> Verify that {@code run} on an {@link Audience}
     * executes work in the audience's transactional scope and commits it.
     * <p>
     * <strong>Start state:</strong> A saved {@link Admin} and a saved
     * {@link Candidate}, both bound to the {@link #runway}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.run(...)} with work that loads the
     * {@link Candidate} through the transaction, changes its name and
     * {@code save()}s it.</li>
     * <li>Load the {@link Candidate} through the enclosing
     * {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The name change is durable.
     */
    @Test
    public void testRunExecutesWorkInAudienceScope() {
        Admin admin = createAdmin();
        Candidate candidate = createCandidate();
        admin.run(transaction -> {
            Candidate inside = transaction.load(Candidate.class,
                    candidate.id());
            inside.name = "Janet Developer";
            Assert.assertTrue(inside.save());
        });
        Assert.assertEquals("Janet Developer",
                runway.load(Candidate.class, candidate.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that the anonymous {@link Audience} refuses
     * every transactional operation.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get {@link Audience#anonymous()}.</li>
     * <li>Call {@code stage()}, {@code startTransaction()}, {@code supply(...)}
     * and {@code run(...)} on it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Every call throws an
     * {@link UnsupportedOperationException}.
     */
    @Test
    public void testAnonymousAudienceRefusesTransactionalOperations() {
        Audience anonymous = Audience.anonymous();
        try {
            anonymous.stage();
            Assert.fail("Expected an UnsupportedOperationException");
        }
        catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            anonymous.startTransaction();
            Assert.fail("Expected an UnsupportedOperationException");
        }
        catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            anonymous.supply(transaction -> null);
            Assert.fail("Expected an UnsupportedOperationException");
        }
        catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            anonymous.run(transaction -> {});
            Assert.fail("Expected an UnsupportedOperationException");
        }
        catch (UnsupportedOperationException e) {
            // expected
        }
    }

}
