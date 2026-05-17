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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.google.common.collect.ImmutableSet;

/**
 * Behavioral contract tests for {@link Saver} implementations.
 * <p>
 * Concrete subclasses supply the implementation under test by overriding
 * {@link #instantiateSaver(Concourse)} and inherit the full suite of behavioral
 * tests.
 *
 * @author Jeff Nelson
 */
public abstract class SaverTest extends RunwayBaseClientServerTest {

    /**
     * Tracks the {@link Concourse} connections wrapped by every {@link Saver}
     * returned from {@link #newSaver()} so they can be released in
     * {@link #afterStartedTest()} instead of leaking against a server reused
     * across tests.
     */
    private final List<Concourse> saverConnections = new ArrayList<>();

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#abort()} on a fresh
     * {@link Saver} (no {@link Saver#stage() stage}, no recordings) is a no-op
     * and leaves the underlying connection usable for a subsequent save.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Saver}.</li>
     * <li>Call {@link Saver#abort()} immediately.</li>
     * <li>Stage, record a {@code set}, and commit on the same
     * {@link Saver}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The initial {@link Saver#abort() abort} does
     * not throw, the follow-on commit returns {@code true}, and the recorded
     * write is visible on the target record.
     */
    @Test
    public void testAbortBeforeStageIsNoOp() {
        long id = client.add("placeholder", 1L);

        Saver saver = newSaver();
        saver.abort();

        saver.stage();
        saver.set("foo", "bar", id);
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of("bar"), client.select("foo", id));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Saver#abort()} after
     * {@link Saver#stage() stage} but with no recordings does not corrupt the
     * underlying connection, so a subsequent save against the same
     * {@link Saver} still commits.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Saver}.</li>
     * <li>Call {@link Saver#stage()} and then {@link Saver#abort()} immediately
     * with no operations in between.</li>
     * <li>Stage, record a {@code set}, and commit on the same
     * {@link Saver}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The follow-on commit returns {@code true} and
     * the recorded write is visible.
     */
    @Test
    public void testAbortAfterStageWithNoRecordingsIsNoOp() {
        long id = client.add("placeholder", 1L);

        Saver saver = newSaver();
        saver.stage();
        saver.abort();

        saver.stage();
        saver.set("foo", "bar", id);
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of("bar"), client.select("foo", id));
    }

    /**
     * <strong>Goal:</strong> Verify that an empty {@link Saver#reconcile
     * reconcile} is equivalent to {@link Saver#clear(String, long) clear(key,
     * record)} &mdash; both impls must route empty values through clear so
     * callers see uniform behavior regardless of transport.
     * <p>
     * <strong>Start state:</strong> A record with two existing values under
     * {@code "tags"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code reconcile} with an empty {@link java.util.Collection
     * Collection}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code "tags"} field is empty on the
     * record.
     */
    @Test
    public void testReconcileWithEmptyCollectionClearsKey() {
        long id = client.add("tags", "a");
        client.add("tags", "b", id);

        Saver saver = newSaver();
        saver.stage();
        saver.reconcile("tags", id, ImmutableSet.of());
        Assert.assertTrue(saver.commit());

        Assert.assertTrue(client.select("tags", id).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that an empty
     * {@link Saver#reconcile(String, long, Object[]) reconcile(key, record,
     * Object[])} is equivalent to {@link Saver#clear(String, long) clear(key,
     * record)}.
     * <p>
     * <strong>Start state:</strong> A record with two existing values under
     * {@code "tags"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link Saver}.</li>
     * <li>Record a {@code reconcile} with an empty {@code Object[]}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code "tags"} field is empty on the
     * record.
     */
    @Test
    public void testReconcileWithEmptyArrayClearsKey() {
        long id = client.add("tags", "a");
        client.add("tags", "b", id);

        Saver saver = newSaver();
        saver.stage();
        saver.reconcile("tags", id, new Object[0]);
        Assert.assertTrue(saver.commit());

        Assert.assertTrue(client.select("tags", id).isEmpty());
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
        long id = client.add("placeholder", 1L);

        Saver saver = newSaver();
        saver.stage();
        saver.set("foo", "bar", id);
        saver.abort();

        Assert.assertTrue(client.select("foo", id).isEmpty());
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
        long id = client.add("a", 1);

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Map<Timestamp, List<String>>> captured = new AtomicReference<>();
        saver.audit(id, captured::set);
        Assert.assertTrue(saver.commit());

        Assert.assertNotNull(captured.get());
        Assert.assertFalse(captured.get().isEmpty());
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
        long id = client.add("drop", "value");

        Saver saver = newSaver();
        saver.stage();
        saver.clear("drop", id);
        Assert.assertTrue(saver.commit());

        Assert.assertTrue(client.select("drop", id).isEmpty());
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
        long id = client.add("a", 1);
        client.add("b", 2, id);

        Saver saver = newSaver();
        saver.stage();
        saver.clear(id);
        Assert.assertTrue(saver.commit());

        Assert.assertTrue(client.select(id).isEmpty());
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
        long match1 = client.add("flag", true);
        long match2 = client.add("flag", true);
        client.add("flag", false);

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Set<Long>> captured = new AtomicReference<>();
        saver.find(Criteria.where().key("flag").operator(Operator.EQUALS)
                .value(true), captured::set);
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of(match1, match2), captured.get());
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
        long id = client.add("tags", "a");
        client.add("tags", "b", id);

        Saver saver = newSaver();
        saver.stage();
        saver.reconcile("tags", id, new Object[] { "b", "c" });
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of("b", "c"),
                client.select("tags", id));
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
        long id = client.add("placeholder", 1L);

        Saver saver = newSaver();
        saver.stage();
        saver.set("foo", "bar", id);
        Assert.assertTrue(saver.commit());

        Set<Object> values = client.select("foo", id);
        Assert.assertEquals(ImmutableSet.of("bar"), values);
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
        long id = client.add("flag", true);

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
        Assert.assertTrue(client.select("foo", id).isEmpty());
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
        long id = client.add("k", "old1");
        client.add("k", "old2", id);

        Saver saver = newSaver();
        saver.stage();
        saver.verifyOrSet("k", "new", id);
        Assert.assertTrue(saver.commit());

        Assert.assertEquals(ImmutableSet.of("new"), client.select("k", id));
    }

    /**
     * Open a fresh {@link Concourse} connection on the same environment as
     * {@link #client}, hand it to the subclass to wrap into a {@link Saver},
     * and track the connection so {@link #afterStartedTest()} can release it.
     *
     * @return the {@link Saver} under test
     */
    protected final Saver newSaver() {
        Concourse connection = Concourse.at().port(server.getClientPort())
                .environment(environment).connect();
        saverConnections.add(connection);
        return instantiateSaver(connection);
    }

    /**
     * Wrap {@code connection} in the {@link Saver} implementation under test.
     *
     * @param connection the {@link Concourse} connection the {@link Saver}
     *            should target
     * @return the {@link Saver} under test
     */
    protected abstract Saver instantiateSaver(Concourse connection);

    @Override
    public void afterStartedTest() {
        try {
            super.afterStartedTest();
        }
        finally {
            for (Concourse connection : saverConnections) {
                try {
                    connection.close();
                }
                catch (Exception ignored) {/*
                                            * second-close on a connection a
                                            * test already closed is benign
                                            */}
            }
            saverConnections.clear();
        }
    }

    @Override
    protected boolean reuseServerAcrossTests() {
        return true;
    }

}
