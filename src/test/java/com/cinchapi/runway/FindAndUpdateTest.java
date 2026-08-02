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
package com.cinchapi.runway;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests for
 * {@link Runway#findAndUpdate(Class, Criteria, java.util.function.Consumer)
 * findAndUpdate}. Each test runs under both Command-API modes (bulk enabled and
 * disabled), so the matrix drives the atomic update through both of its
 * transaction paths: batched submissions when the server supports bulk commands
 * and the incremental path otherwise.
 *
 * @author Javier Lores
 */
@RunWith(Parameterized.class)
public class FindAndUpdateTest extends RunwayBaseClientServerTest {

    /**
     * Return the parameter matrix that drives each test once per Command-API
     * capability.
     *
     * @return one row with bulk commands enabled and one with it disabled
     */
    @Parameters(name = "bulkCommands={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    /**
     * Whether the test run exercises the bulk Command-API read path.
     */
    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to exercise the bulk Command-API read
     *            path; {@code false} for the incremental path
     */
    public FindAndUpdateTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAndUpdate} updates every
     * matching record and durably persists all of the updates.
     * <p>
     * <strong>Start state:</strong> Three {@link Doc Docs} that all match the
     * criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findAndUpdate} with {@code rank > 0} and a consumer that
     * sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load each returned {@link Doc} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Three {@link Doc Docs} are returned, each with
     * {@code owner == "worker"}, and every re-loaded {@link Doc} shows the
     * persisted {@code owner}.
     */
    @Test
    public void testFindAndUpdateUpdatesAllMatchesAndPersists() {
        runway.save(new Doc(1), new Doc(2), new Doc(3));
        Set<Doc> updated = runway.findAndUpdate(Doc.class, rankPositive(),
                doc -> doc.owner = "worker");
        Assert.assertEquals(3, updated.size());
        for (Doc doc : updated) {
            Assert.assertEquals("worker", doc.owner);
            Assert.assertEquals("worker",
                    runway.load(Doc.class, doc.id()).owner);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAndUpdate} returns an empty
     * {@link Set} and never invokes the consumer when no record matches.
     * <p>
     * <strong>Start state:</strong> Three {@link Doc Docs} none of which
     * matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findAndUpdate} with {@code rank > 100} and a consumer
     * that flips an {@link AtomicBoolean}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Set} is empty and the
     * consumer never ran.
     */
    @Test
    public void testFindAndUpdateReturnsEmptyAndSkipsConsumerWhenNoMatch() {
        runway.save(new Doc(1), new Doc(2), new Doc(3));
        AtomicBoolean consumerRan = new AtomicBoolean(false);
        Set<Doc> updated = runway.findAndUpdate(
                Doc.class, Criteria.where().key("rank")
                        .operator(Operator.GREATER_THAN).value(100).build(),
                doc -> consumerRan.set(true));
        Assert.assertTrue(updated.isEmpty());
        Assert.assertFalse(consumerRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify the all-or-nothing guarantee: when the
     * consumer throws partway through the match set, no record's update is
     * persisted.
     * <p>
     * <strong>Start state:</strong> Three {@link Doc Docs} that all match the
     * criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findAndUpdate} with a consumer that sets {@code owner}
     * but throws when it reaches the rank-2 {@link Doc}.</li>
     * <li>Catch the thrown exception, then re-load all three {@link Doc
     * Docs}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exception propagates and none of the
     * re-loaded {@link Doc Docs} has an {@code owner} set.
     */
    @Test
    public void testFindAndUpdateIsAllOrNothingWhenConsumerThrows() {
        Doc one = new Doc(1);
        Doc two = new Doc(2);
        Doc three = new Doc(3);
        runway.save(one, two, three);
        boolean threw = false;
        try {
            runway.findAndUpdate(Doc.class, rankPositive(), doc -> {
                if(doc.rank == 2) {
                    throw new IllegalStateException("boom");
                }
                doc.owner = "worker";
            });
        }
        catch (IllegalStateException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertNull(runway.load(Doc.class, one.id()).owner);
        Assert.assertNull(runway.load(Doc.class, two.id()).owner);
        Assert.assertNull(runway.load(Doc.class, three.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} the consumer attaches
     * remains saveable after a terminal failure, so its unsaved changes are not
     * silently dropped by a later save.
     * <p>
     * <strong>Start state:</strong> Two {@link Doc Docs} that match the
     * criteria and one persisted {@link Memo} whose {@code owner} was then
     * modified in memory but not saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1 and 2 and a {@link Memo}.</li>
     * <li>Set the {@link Memo Memo's} in-memory {@code owner} to
     * {@code "author"} without a save.</li>
     * <li>Call {@code findAndUpdate} with a consumer that attaches the
     * {@link Memo} to each {@link Doc} and throws on its second
     * invocation.</li>
     * <li>Catch the thrown exception, save the {@link Memo}, then re-load it by
     * id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exception propagates, the subsequent save
     * succeeds, and the re-loaded {@link Memo Memo's} {@code owner} is
     * {@code "author"}, proving the aborted attempt did not leave the
     * {@link Memo} looking saved.
     */
    @Test
    public void testFindAndUpdateAttachedRecordStaysSaveableAfterFailure() {
        Doc one = new Doc(1);
        Doc two = new Doc(2);
        Memo memo = new Memo(9);
        runway.save(one, two, memo);
        memo.owner = "author";
        AtomicInteger invocations = new AtomicInteger(0);
        boolean threw = false;
        try {
            runway.findAndUpdate(Doc.class, rankPositive(), doc -> {
                doc.attachment = memo;
                if(invocations.incrementAndGet() == 2) {
                    throw new IllegalStateException("boom");
                }
            });
        }
        catch (IllegalStateException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.save(memo));
        Assert.assertEquals("author", runway.load(Memo.class, memo.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAndUpdate} only matches
     * records of the target class, even when records of an unrelated class
     * share the queried key.
     * <p>
     * <strong>Start state:</strong> Two {@link Doc Docs} and one {@link Memo}
     * whose {@code rank} values all satisfy the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1 and 2 and a {@link Memo} with rank
     * 3.</li>
     * <li>Call {@code findAndUpdate} for {@link Doc} with {@code rank > 0} and
     * a consumer that sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load the {@link Memo} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Exactly the two {@link Doc Docs} are returned
     * and updated, and the re-loaded {@link Memo Memo's} {@code owner} is still
     * unset.
     */
    @Test
    public void testFindAndUpdateOnlyMatchesRecordsOfTargetClass() {
        Memo memo = new Memo(3);
        runway.save(new Doc(1), new Doc(2), memo);
        Set<Doc> updated = runway.findAndUpdate(Doc.class, rankPositive(),
                doc -> doc.owner = "worker");
        Assert.assertEquals(2, updated.size());
        for (Doc doc : updated) {
            Assert.assertEquals("worker",
                    runway.load(Doc.class, doc.id()).owner);
        }
        Assert.assertNull(runway.load(Memo.class, memo.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAndUpdate} excludes
     * subclass records, consistent with how {@code find} resolves a class
     * exactly rather than across its hierarchy.
     * <p>
     * <strong>Start state:</strong> One {@link Doc} and one {@link SpecialDoc}
     * whose {@code rank} values both satisfy the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Doc} with rank 1 and a {@link SpecialDoc} with rank
     * 2.</li>
     * <li>Call {@code findAndUpdate} for {@link Doc} with {@code rank > 0} and
     * a consumer that sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load the {@link SpecialDoc} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only the {@link Doc} is returned and updated,
     * and the re-loaded {@link SpecialDoc SpecialDoc's} {@code owner} is still
     * unset.
     */
    @Test
    public void testFindAndUpdateExcludesSubclassRecords() {
        Doc doc = new Doc(1);
        SpecialDoc special = new SpecialDoc(2);
        runway.save(doc, special);
        Set<Doc> updated = runway.findAndUpdate(Doc.class, rankPositive(),
                d -> d.owner = "worker");
        Assert.assertEquals(1, updated.size());
        Assert.assertEquals(doc.id(), updated.iterator().next().id());
        Assert.assertEquals("worker", runway.load(Doc.class, doc.id()).owner);
        Assert.assertNull(runway.load(SpecialDoc.class, special.id()).owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAndUpdate} supports a
     * {@link Criteria} over derived data that the database cannot resolve,
     * updating exactly the matching records.
     * <p>
     * <strong>Start state:</strong> Three {@link Doc Docs} with ranks 1, 2, and
     * 3, of which the odd-ranked two match the derived {@code parity} criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findAndUpdate} with {@code parity == "odd"} and a
     * consumer that sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load each returned {@link Doc} and the non-matching {@link Doc} by
     * id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Two odd-ranked {@link Doc Docs} are returned
     * with the persisted {@code owner}, and the even-ranked {@link Doc} has no
     * {@code owner}.
     */
    @Test
    public void testFindAndUpdateUpdatesOnlyDerivedCriteriaMatches() {
        Doc one = new Doc(1);
        Doc two = new Doc(2);
        Doc three = new Doc(3);
        runway.save(one, two, three);
        Set<Doc> updated = runway.findAndUpdate(Doc.class, parity("odd"),
                doc -> doc.owner = "worker");
        Assert.assertEquals(2, updated.size());
        for (Doc doc : updated) {
            Assert.assertEquals(1, doc.rank % 2);
            Assert.assertEquals("worker",
                    runway.load(Doc.class, doc.id()).owner);
        }
        Assert.assertNull(runway.load(Doc.class, two.id()).owner);
    }

    /**
     * Return a {@link Criteria} matching every {@link Doc} whose derived
     * {@code parity} equals the given {@code value}.
     *
     * @param value the parity to match; {@code "odd"} or {@code "even"}
     * @return the {@code parity == value} {@link Criteria}
     */
    private static Criteria parity(String value) {
        return Criteria.where().key("parity").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * Return a {@link Criteria} matching every {@link Doc} whose {@code rank}
     * is positive.
     *
     * @return the {@code rank > 0} {@link Criteria}
     */
    private static Criteria rankPositive() {
        return Criteria.where().key("rank").operator(Operator.GREATER_THAN)
                .value(0).build();
    }

    /**
     * A {@link Record} with a queryable {@code rank} and a mutable
     * {@code owner}.
     *
     * @author Javier Lores
     */
    public static class Doc extends Record {

        /**
         * The queryable rank.
         */
        int rank;

        /**
         * The mutable owner, or {@code null} when unset.
         */
        String owner;

        /**
         * An attached {@link Memo}, or {@code null} when none is attached.
         */
        Memo attachment;

        /**
         * Construct a new instance.
         *
         * @param rank the queryable rank
         */
        public Doc(int rank) {
            this.rank = rank;
        }

        @Override
        protected Map<String, Object> derived() {
            Map<String, Object> derived = new HashMap<>();
            derived.put("parity", rank % 2 == 0 ? "even" : "odd");
            return derived;
        }
    }

    /**
     * A {@link Doc} subclass used to verify that {@code findAndUpdate} resolves
     * the target class exactly rather than across its hierarchy.
     *
     * @author Jeff Nelson
     */
    public static class SpecialDoc extends Doc {

        /**
         * Construct a new instance.
         *
         * @param rank the queryable rank
         */
        public SpecialDoc(int rank) {
            super(rank);
        }
    }

    /**
     * A {@link Record} of an unrelated class that shares the queryable
     * {@code rank} key with {@link Doc}.
     *
     * @author Jeff Nelson
     */
    public static class Memo extends Record {

        /**
         * The queryable rank.
         */
        int rank;

        /**
         * The mutable owner, or {@code null} when unset.
         */
        String owner;

        /**
         * Construct a new instance.
         *
         * @param rank the queryable rank
         */
        public Memo(int rank) {
            this.rank = rank;
        }
    }

}
