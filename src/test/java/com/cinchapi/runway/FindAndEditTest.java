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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * {@link Runway#findAndEdit(Class, Criteria, java.util.function.Consumer)
 * findAndEdit}. Each test runs under both Command-API modes (bulk enabled and
 * disabled); the atomic edit itself always uses the incremental,
 * synchronously-staged transaction path, so the matrix additionally guards the
 * surrounding save and load operations under both modes.
 *
 * @author Javier Lores
 */
@RunWith(Parameterized.class)
public class FindAndEditTest extends RunwayBaseClientServerTest {

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

    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to exercise the bulk Command-API read
     *            path; {@code false} for the incremental path
     */
    public FindAndEditTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAndEdit} edits every
     * matching record and durably persists all of the edits.
     * <p>
     * <strong>Start state:</strong> Three {@link Doc Docs} that all match the
     * criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findAndEdit} with {@code rank > 0} and a consumer that
     * sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load each returned {@link Doc} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Three {@link Doc Docs} are returned, each with
     * {@code owner == "worker"}, and every re-loaded {@link Doc} shows the
     * persisted {@code owner}.
     */
    @Test
    public void testFindAndEditEditsAllMatchesAndPersists() {
        runway.save(new Doc(1), new Doc(2), new Doc(3));
        Set<Doc> edited = runway.findAndEdit(Doc.class, rankPositive(),
                doc -> doc.owner = "worker");
        Assert.assertEquals(3, edited.size());
        for (Doc doc : edited) {
            Assert.assertEquals("worker", doc.owner);
            Assert.assertEquals("worker",
                    runway.load(Doc.class, doc.id()).owner);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAndEdit} returns an empty
     * {@link Set} and never invokes the consumer when no record matches.
     * <p>
     * <strong>Start state:</strong> Three {@link Doc Docs} none of which
     * matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findAndEdit} with {@code rank > 100} and a consumer that
     * flips an {@link AtomicBoolean}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Set} is empty and the
     * consumer never ran.
     */
    @Test
    public void testFindAndEditReturnsEmptyAndSkipsConsumerWhenNoMatch() {
        runway.save(new Doc(1), new Doc(2), new Doc(3));
        AtomicBoolean consumerRan = new AtomicBoolean(false);
        Set<Doc> edited = runway.findAndEdit(
                Doc.class, Criteria.where().key("rank")
                        .operator(Operator.GREATER_THAN).value(100).build(),
                doc -> consumerRan.set(true));
        Assert.assertTrue(edited.isEmpty());
        Assert.assertFalse(consumerRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify the all-or-nothing guarantee: when the
     * consumer throws partway through the match set, no record's edit is
     * persisted.
     * <p>
     * <strong>Start state:</strong> Three {@link Doc Docs} that all match the
     * criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Doc Docs} with ranks 1, 2, and 3.</li>
     * <li>Call {@code findAndEdit} with a consumer that sets {@code owner} but
     * throws when it reaches the rank-2 {@link Doc}.</li>
     * <li>Catch the thrown exception, then re-load all three {@link Doc
     * Docs}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exception propagates and none of the
     * re-loaded {@link Doc Docs} has an {@code owner} set.
     */
    @Test
    public void testFindAndEditIsAllOrNothingWhenConsumerThrows() {
        Doc one = new Doc(1);
        Doc two = new Doc(2);
        Doc three = new Doc(3);
        runway.save(one, two, three);
        boolean threw = false;
        try {
            runway.findAndEdit(Doc.class, rankPositive(), doc -> {
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
     * A {@link Record} with a queryable {@code rank} and an editable
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
         * The editable owner, or {@code null} when unset.
         */
        String owner;

        /**
         * Construct a new instance.
         *
         * @param rank the queryable rank
         */
        public Doc(int rank) {
            this.rank = rank;
        }
    }

}
