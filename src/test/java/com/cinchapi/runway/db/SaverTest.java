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
package com.cinchapi.runway.db;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableSet;

/**
 * Behavioral contract tests for {@link Saver} implementations.
 * <p>
 * Concrete subclasses supply the implementation under test by overriding
 * {@link #newSaver()} and inherit the full suite of behavioral tests.
 *
 * @author Jeff Nelson
 */
public abstract class SaverTest extends ClientServerTest {

    /**
     * A long-lived {@link Concourse} connection used for arrange/assert
     * operations that should not flow through the {@link Saver} under test.
     */
    protected Concourse concourse;

    @Override
    public void afterStartedTest() {
        concourse.close();
    }

    @Override
    public void beforeEachTest() {
        concourse = Concourse.at().port(server.getClientPort()).connect();
    }

    /**
     * Construct the {@link Saver} under test, wrapping a fresh connection so it
     * has its own staging context independent of {@link #concourse}.
     *
     * @return a new {@link Saver}
     */
    protected abstract Saver newSaver();

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#concourse()} returns the
     * underlying {@link Concourse} that the {@link Saver} was constructed with
     * so cascade-read fallthrough has a connection to use.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Saver}.</li>
     * <li>Call {@link Saver#concourse()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned reference is non-{@code null}.
     */
    @Test
    public void testConcourseReturnsTheUnderlyingConnection() {
        Saver saver = newSaver();
        Assert.assertNotNull(saver.concourse());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#commit()} returns
     * {@code true} when the staged transaction commits cleanly with no recorded
     * operations beyond stage.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Commit immediately without recording any reads or writes.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@link Saver#commit()} returns {@code true}.
     */
    @Test
    public void testEmptyCommitSucceeds() {
        Saver saver = newSaver();
        saver.stage();
        Assert.assertTrue(saver.commit());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#set} is applied to the
     * database after the staged transaction commits.
     * <p>
     * <strong>Start state:</strong> An empty target record.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code set} of {@code "bar"} into {@code "foo"} on the
     * target.</li>
     * <li>Commit the {@link Saver}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The target record's {@code "foo"} field holds
     * exactly {@code "bar"}.
     */
    @Test
    public void testSetIsAppliedAfterCommit() {
        long id = concourse.add("placeholder", 1L);

        Saver saver = newSaver();
        saver.stage();
        saver.set("foo", "bar", id);
        Assert.assertTrue(saver.commit());

        Set<Object> values = concourse.select("foo", id);
        Assert.assertEquals(ImmutableSet.of("bar"), values);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#clear(String, long)}
     * removes the values previously associated with the key.
     * <p>
     * <strong>Start state:</strong> A record with a value under {@code "drop"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code clear(key, record)}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code "drop"} field is empty on the
     * record.
     */
    @Test
    public void testClearKeyOnRecordIsAppliedAfterCommit() {
        long id = concourse.add("drop", "value");

        Saver saver = newSaver();
        saver.stage();
        saver.clear("drop", id);
        Assert.assertTrue(saver.commit());

        Assert.assertTrue(concourse.select("drop", id).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#clear(long)} wipes the
     * entire record.
     * <p>
     * <strong>Start state:</strong> A record with values under two keys.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code clear(record)} on the record.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The record holds no values under any key.
     */
    @Test
    public void testClearRecordIsAppliedAfterCommit() {
        long id = concourse.add("a", 1);
        concourse.add("b", 2, id);

        Saver saver = newSaver();
        saver.stage();
        saver.clear(id);
        Assert.assertTrue(saver.commit());

        Assert.assertTrue(concourse.select(id).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#verifyOrSet} updates the
     * value on a record so that exactly one value remains.
     * <p>
     * <strong>Start state:</strong> A record with two values under {@code "k"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code verifyOrSet} with a new value.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The record holds exactly the new value under
     * {@code "k"}.
     */
    @Test
    public void testVerifyOrSetIsAppliedAfterCommit() {
        long id = concourse.add("k", "old1");
        concourse.add("k", "old2", id);

        Saver saver = newSaver();
        saver.stage();
        saver.verifyOrSet("k", "new", id);
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of("new"), concourse.select("k", id));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#reconcile} replaces the
     * existing values for a key with exactly the supplied set.
     * <p>
     * <strong>Start state:</strong> A record with two existing values under
     * {@code "tags"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code reconcile} with a different set of values.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The record holds exactly the reconciled
     * values, with the prior values removed.
     */
    @Test
    public void testReconcileIsAppliedAfterCommit() {
        long id = concourse.add("tags", "a");
        concourse.add("tags", "b", id);

        Saver saver = newSaver();
        saver.stage();
        saver.reconcile("tags", id, new Object[] { "b", "c" });
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of("b", "c"),
                concourse.select("tags", id));
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code validator} passed to
     * {@link Saver#audit(long, java.util.function.Consumer) audit} receives the
     * audit history for the recorded record.
     * <p>
     * <strong>Start state:</strong> A record that has been modified once.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record an {@code audit} with a {@code validator} that captures the
     * result.</li>
     * <li>Commit so the {@code validator} is guaranteed to have run.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The captured audit map is non-empty.
     */
    @Test
    public void testAuditValidatorReceivesResult() {
        long id = concourse.add("a", 1);

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Map<Timestamp, List<String>>> captured = new AtomicReference<>();
        saver.audit(id, captured::set);
        Assert.assertTrue(saver.commit());

        Assert.assertNotNull(captured.get());
        Assert.assertFalse(captured.get().isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that the {@code validator} passed to
     * {@link Saver#find(Criteria, java.util.function.Consumer) find} receives
     * the matching record ids.
     * <p>
     * <strong>Start state:</strong> Two records with {@code flag = true} and
     * one with {@code flag = false}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code find} for {@code flag = true} with a
     * {@code validator} that captures the matching ids.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The captured set contains exactly the two
     * matching ids.
     */
    @Test
    public void testFindValidatorReceivesMatchingIds() {
        long match1 = concourse.add("flag", true);
        long match2 = concourse.add("flag", true);
        concourse.add("flag", false);

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Set<Long>> captured = new AtomicReference<>();
        saver.find(Criteria.where().key("flag").operator(Operator.EQUALS)
                .value(true), captured::set);
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of(match1, match2), captured.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code validator} that throws
     * propagates the exception out to the caller and that calling
     * {@link Saver#abort()} afterward leaves no recorded writes persisted.
     * <p>
     * <strong>Start state:</strong> A record with a single value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code set} of a new value into a new key.</li>
     * <li>Record a {@code find} with a {@code validator} that throws
     * {@link IllegalStateException}.</li>
     * <li>Call {@link Saver#commit()}.</li>
     * <li>In the catch, call {@link Saver#abort()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is observed
     * (either from the recording call for synchronous implementations or from
     * {@link Saver#commit()} for bulk implementations) and the new write is not
     * visible on the record after abort.
     */
    @Test
    public void testValidatorThrowAbortsStagedTransaction() {
        long id = concourse.add("flag", true);

        Saver saver = newSaver();
        saver.stage();
        boolean caught = false;
        try {
            saver.set("foo", "bar", id);
            saver.find(Criteria.where().key("flag").operator(Operator.EQUALS)
                    .value(true), records -> {
                        throw new IllegalStateException("rejected");
                    });
            saver.commit();
        }
        catch (IllegalStateException expected) {
            caught = true;
            saver.abort();
        }

        Assert.assertTrue("expected validator to reject the save", caught);
        Assert.assertTrue(concourse.select("foo", id).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#abort()} leaves no
     * recorded writes persisted on the database.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code set}.</li>
     * <li>Call {@link Saver#abort()} instead of {@link Saver#commit()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The recorded write is not visible on the
     * target record.
     */
    @Test
    public void testAbortDiscardsPendingWrites() {
        long id = concourse.add("placeholder", 1L);

        Saver saver = newSaver();
        saver.stage();
        saver.set("foo", "bar", id);
        saver.abort();

        Assert.assertTrue(concourse.select("foo", id).isEmpty());
    }

}
