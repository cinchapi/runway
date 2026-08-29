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

import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.TransactionInterface;

/**
 * Tests for the view that an anonymous {@link Audience} operates through when
 * it is scoped to a {@link Transaction}.
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

}
