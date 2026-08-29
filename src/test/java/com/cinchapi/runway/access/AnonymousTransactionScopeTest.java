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

import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Runway;
import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.TransactionInterface;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for the transactional operations of an anonymous {@link Audience},
 * which holds the database it operates against.
 *
 * @author Jeff Nelson
 */
public class AnonymousTransactionScopeTest
        extends AudienceAccessControlBaseTest {

    /**
     * Return a saved {@link Employer} that is bound to the test
     * {@link #runway}.
     *
     * @return the {@link Employer}
     */
    private Employer createEmployer() {
        Employer employer = new Employer();
        employer.name = "Acme";
        employer.assign(runway);
        Assert.assertTrue(employer.save());
        return employer;
    }

    /**
     * Return a saved {@link GuestNote} that is bound to the test
     * {@link #runway}.
     *
     * @param label the display label
     * @param votes the initial vote tally
     * @return the {@link GuestNote}
     */
    private GuestNote createGuestNote(String label, int votes) {
        GuestNote note = new GuestNote();
        note.label = label;
        note.votes = votes;
        note.assign(runway);
        Assert.assertTrue(note.save());
        return note;
    }

    /**
     * Return a saved {@link Job} that is bound to the test {@link #runway}.
     *
     * @param employer the {@link Employer} that posts the {@link Job}
     * @param published whether the {@link Job} is published, which is what
     *            makes it discoverable by an anonymous {@link Audience}
     * @return the {@link Job}
     */
    private Job createJob(Employer employer, boolean published) {
        Job job = new Job();
        job.title = published ? "Published Role" : "Draft Role";
        job.employer = employer;
        job.published = published;
        job.assign(runway);
        Assert.assertTrue(job.save());
        return job;
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
     * Return a {@link Criteria} that matches records whose label is
     * {@code label}.
     *
     * @param label the label to match
     * @return the {@link Criteria}
     */
    private Criteria labelCriteria(String label) {
        return Criteria.where().key("label").operator(Operator.EQUALS)
                .value(label).build();
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} created through the
     * scoped view of an anonymous {@link Audience} stages within that view's
     * {@link Transaction}, so a direct {@code save()} does not become durable
     * before the commit.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Create a {@link Candidate} through the anonymous
     * {@link Audience#scope(TransactionInterface) scoped} view and
     * {@code save()} it directly.</li>
     * <li>Search for the {@link Candidate} through the enclosing
     * {@link #runway} before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Candidate} is invisible outside the
     * transaction before the commit and visible after it.
     */
    @Test
    public void testAnonymousScopedViewStagesCreatedRecord() {
        try (Transaction transaction = runway.startTransaction()) {
            TransactionInterface view = Audience.anonymous().scope(transaction);
            Candidate candidate = view.create(Candidate.class);
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
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} scoped
     * to a {@link Transaction} reads through that transaction, so it observes a
     * write the transaction stages but has not committed.
     * <p>
     * <strong>Start state:</strong> A saved unpublished {@link Job}, which an
     * anonymous {@link Audience} cannot discover.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Load the {@link Job} through the transaction, publish it and
     * {@code save()} it, so the write stages without committing.</li>
     * <li>Load the {@link Job} through the anonymous
     * {@link Audience#scope(TransactionInterface) scoped} view.</li>
     * <li>Load the {@link Job} through the enclosing {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The scoped view finds the {@link Job}, because
     * the staged write makes it discoverable there, while the enclosing
     * {@link #runway} still reads it as unpublished.
     */
    @Test
    public void testAnonymousScopedViewReadsStagedWrite() {
        Employer employer = createEmployer();
        Job draft = createJob(employer, false);
        try (Transaction transaction = runway.startTransaction()) {
            Job staged = transaction.load(Job.class, draft.id());
            staged.published = true;
            Assert.assertTrue(staged.save());
            TransactionInterface view = Audience.anonymous().scope(transaction);
            Assert.assertNotNull(view.load(Job.class, draft.id()));
            Assert.assertFalse(runway.load(Job.class, draft.id()).published);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} scoped
     * to a {@link Transaction} still applies its visibility constraints, so the
     * transaction does not widen what the audience may read.
     * <p>
     * <strong>Start state:</strong> One saved published {@link Job} and one
     * saved unpublished {@link Job}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Load each {@link Job} through the anonymous
     * {@link Audience#scope(TransactionInterface) scoped} view.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The published {@link Job} is found and the
     * unpublished one is not.
     */
    @Test
    public void testAnonymousScopedViewAppliesVisibilityConstraints() {
        Employer employer = createEmployer();
        Job published = createJob(employer, true);
        Job draft = createJob(employer, false);
        try (Transaction transaction = runway.startTransaction()) {
            TransactionInterface view = Audience.anonymous().scope(transaction);
            Assert.assertNotNull(view.load(Job.class, published.id()));
            Assert.assertNull(view.load(Job.class, draft.id()));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code transactAndSupply} on an
     * anonymous {@link Audience} that holds an open {@link Transaction} joins
     * it, so the work observes the transaction's staged writes.
     * <p>
     * <strong>Start state:</strong> A saved unpublished {@link Job}, which an
     * anonymous {@link Audience} cannot discover.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Load the {@link Job} through the transaction, publish it and
     * {@code save()} it, so the write stages without committing.</li>
     * <li>Get {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * Audience.anonymous(transaction)} and load the {@link Job} within
     * {@code transactAndSupply}.</li>
     * <li>Load the {@link Job} through the enclosing {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The work finds the {@link Job}, because the
     * staged write makes it discoverable there, while the enclosing
     * {@link #runway} still reads it as unpublished.
     */
    @Test
    public void testAnonymousTransactAndSupplyJoinsHeldTransaction() {
        Employer employer = createEmployer();
        Job draft = createJob(employer, false);
        try (Transaction transaction = runway.startTransaction()) {
            Job staged = transaction.load(Job.class, draft.id());
            staged.published = true;
            Assert.assertTrue(staged.save());
            Audience anonymous = Audience.anonymous(transaction);
            Job found = anonymous.transactAndSupply(
                    view -> view.load(Job.class, draft.id()));
            Assert.assertNotNull(found);
            Assert.assertFalse(runway.load(Job.class, draft.id()).published);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} can
     * start a {@link Transaction}, and that a {@link Record} created through
     * the returned view stages within it until the commit.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code Audience.anonymous().startTransaction()}.</li>
     * <li>Create a {@link Candidate} through the returned view and
     * {@code save()} it.</li>
     * <li>Search for the {@link Candidate} through the enclosing
     * {@link #runway} before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Candidate} is invisible outside the
     * transaction before the commit and visible after it.
     */
    @Test
    public void testAnonymousStartTransactionStagesAndCommits() {
        try (Transaction transaction = Audience.anonymous()
                .startTransaction()) {
            Candidate candidate = transaction.create(Candidate.class);
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
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} that
     * holds an open {@link Transaction} refuses to start another one, because
     * transactions do not nest.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Get {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * Audience.anonymous(transaction)} and call
     * {@code startTransaction()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws an
     * {@link IllegalStateException}.
     */
    @Test
    public void testAnonymousStartTransactionRefusedWithinHeldTransaction() {
        try (Transaction transaction = runway.startTransaction()) {
            Audience anonymous = Audience.anonymous(transaction);
            try {
                anonymous.startTransaction();
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {
                // expected
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that anonymous {@link Audience Audiences}
     * are equal to one another regardless of the database each holds, and that
     * {@link Audience#isAnonymous()} distinguishes them from a {@link Record}
     * audience.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Get {@link Audience#anonymous()} and
     * {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * Audience.anonymous(runway)}.</li>
     * <li>Compare the two with {@code equals} and {@code hashCode}.</li>
     * <li>Call {@code isAnonymous()} on both, and on a {@link Candidate}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The instances are distinct but equal in both
     * directions with equal hash codes; both report {@code isAnonymous()} and
     * the {@link Candidate} does not.
     */
    @Test
    public void testAnonymousAudiencesAreEqual() {
        Audience a = Audience.anonymous();
        Audience b = Audience.anonymous(runway);
        Assert.assertNotSame(a, b);
        Assert.assertEquals(a, b);
        Assert.assertEquals(b, a);
        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertTrue(a.isAnonymous());
        Assert.assertTrue(b.isAnonymous());
        Assert.assertFalse(new Candidate().isAnonymous());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Audience#anonymous()} returns
     * one stable instance for the single open {@link Runway}, and that
     * {@link Runway#anonymous()} returns the same one.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Audience#anonymous()} twice.</li>
     * <li>Call {@link Runway#anonymous()} on the {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Every call returns the same object reference.
     */
    @Test
    public void testAnonymousIsStablePerRunwayInstance() {
        Assert.assertSame(Audience.anonymous(), Audience.anonymous());
        Assert.assertSame(runway.anonymous(), Audience.anonymous());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Audience#anonymous()} names no
     * database when no single {@link Runway} instance is open, so it still
     * answers access policy questions but refuses a database operation, and
     * that the explicit
     * {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * anonymous(db)} form still works in that state.
     * <p>
     * <strong>Start state:</strong> One saved published {@link Job} and one
     * saved unpublished {@link Job}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a second {@link Runway} against the test server.</li>
     * <li>Apply the {@code $checkIfVisible()} predicate of
     * {@link Audience#anonymous()} to each {@link Job}.</li>
     * <li>Call {@code startTransaction()} on it and catch the expected
     * exception.</li>
     * <li>Call {@code Audience.anonymous(runway)}.</li>
     * <li>Close the second {@link Runway} and call {@link Audience#anonymous()}
     * again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The predicate accepts the published
     * {@link Job} and rejects the unpublished one; {@code startTransaction()}
     * throws an {@link IllegalStateException} whose message names
     * {@code anonymous(db)}; the explicit form works; and after the second
     * instance closes, the no-arg call names a database again.
     */
    @Test
    public void testAnonymousWithoutSingleOpenRunwayInstanceIsPolicyOnly() {
        Employer employer = createEmployer();
        Job published = createJob(employer, true);
        Job draft = createJob(employer, false);
        Runway other = runwayBuilder().build();
        try {
            Audience anonymous = Audience.anonymous();
            Assert.assertTrue(anonymous.$checkIfVisible().test(published));
            Assert.assertFalse(anonymous.$checkIfVisible().test(draft));
            try {
                anonymous.startTransaction();
                Assert.fail("Expected an IllegalStateException");
            }
            catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("anonymous(db)"));
            }
            Assert.assertTrue(Audience.anonymous(runway).isAnonymous());
        }
        finally {
            try {
                other.close();
            }
            catch (Exception ignored) {/* close failure not under test */}
        }
        Assert.assertSame(runway.anonymous(), Audience.anonymous());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} through an anonymous
     * {@link Audience} that holds an open {@link Transaction} stages within it,
     * so nothing becomes durable before the commit.
     * <p>
     * <strong>Start state:</strong> No saved {@link Candidate Candidates}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Get {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * Audience.anonymous(transaction)} and {@code intern} a new
     * {@link Candidate}.</li>
     * <li>Search for the {@link Candidate} through the enclosing
     * {@link #runway} before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The intern returns the {@link Candidate}
     * itself, which is invisible outside the transaction before the commit and
     * visible after it.
     */
    @Test
    public void testAnonymousInternStagesWithinHeldTransaction() {
        try (Transaction transaction = runway.startTransaction()) {
            Audience anonymous = Audience.anonymous(transaction);
            Candidate candidate = new Candidate();
            candidate.email = "jane@example.com";
            candidate.name = "Jane Developer";
            Candidate interned = anonymous.intern(candidate);
            Assert.assertSame(candidate, interned);
            Assert.assertTrue(runway.find(Candidate.class, janeEmailCriteria())
                    .isEmpty());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(1,
                runway.find(Candidate.class, janeEmailCriteria()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that an atomic find-and-update through an
     * anonymous {@link Audience} that holds an open {@link Transaction} stages
     * the lookup and the write within it, so the update is invisible outside
     * the transaction before the commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link GuestNote} with one vote.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} on the {@link #runway}.</li>
     * <li>Get {@link Audience#anonymous(com.cinchapi.runway.DatabaseInterface)
     * Audience.anonymous(transaction)} and call {@code findUniqueAndUpdate} to
     * increment the votes.</li>
     * <li>Load the {@link GuestNote} through the enclosing {@link #runway}
     * before and after {@code commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The update returns the {@link GuestNote} with
     * two votes; the enclosing {@link #runway} reads one vote before the commit
     * and two votes after it.
     */
    @Test
    public void testAnonymousAtomicUpdateStagesWithinHeldTransaction() {
        GuestNote note = createGuestNote("welcome", 1);
        try (Transaction transaction = runway.startTransaction()) {
            Audience anonymous = Audience.anonymous(transaction);
            GuestNote updated = anonymous.findUniqueAndUpdate(GuestNote.class,
                    labelCriteria("welcome"), "votes",
                    (Integer votes) -> votes + 1);
            Assert.assertNotNull(updated);
            Assert.assertEquals(2, updated.votes.intValue());
            Assert.assertEquals(1,
                    runway.load(GuestNote.class, note.id()).votes.intValue());
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals(2,
                runway.load(GuestNote.class, note.id()).votes.intValue());
    }

    /**
     * A note that an anonymous {@link Audience} may discover, read and vote on.
     *
     * @author Jeff Nelson
     */
    protected static class GuestNote extends Record implements AccessControl {

        /**
         * The display label.
         */
        public String label;

        /**
         * The vote tally.
         */
        public Integer votes;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return true;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return true;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return ImmutableSet.of("votes");
        }

    }

}
