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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link BatchSaver} that combine the shared {@link Saver}
 * contract with the implementation's batched semantics.
 *
 * @author Jeff Nelson
 */
public class BatchSaverTest extends SaverTest {

    @Override
    protected Saver instantiateSaver(Concourse connection) {
        return new BatchSaver(connection);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link BatchSaver} validator does
     * <em>not</em> run at recording time &mdash; the record-then-batch shape
     * requires that validators only fire after the reads submission resolves
     * inside {@link Saver#commit()}.
     * <p>
     * <strong>Start state:</strong> A record matches {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record a {@code find} whose validator sets a flag.</li>
     * <li>Read the flag immediately after the {@code find} call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The validator has <em>not</em> run yet.
     */
    @Test
    public void testFindValidatorIsDeferredUntilCommit() {
        client.add("flag", true);

        Saver saver = newSaver();
        saver.stage();
        AtomicBoolean validatorRan = new AtomicBoolean(false);
        saver.find(Criteria.where().key("flag").operator(Operator.EQUALS)
                .value(true), ids -> validatorRan.set(true));

        Assert.assertFalse("validator should defer until commit",
                validatorRan.get());

        Assert.assertNotNull(saver.commit());
        Assert.assertTrue("validator should have run inside commit",
                validatorRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify that when a validator throws inside
     * {@link Saver#commit()}, the writes recorded against the
     * {@link BatchSaver} are <em>not</em> persisted &mdash; the second
     * submission must be skipped.
     * <p>
     * <strong>Start state:</strong> A record exists with {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage.</li>
     * <li>Record a {@code set} of a new field's value.</li>
     * <li>Record a {@code find} whose validator throws.</li>
     * <li>Call {@code commit}; catch the exception.</li>
     * <li>Call {@code abort} to release the server-side staged
     * transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The set's key is not present on the record.
     */
    @Test
    public void testValidatorThrowSkipsWritesSubmission() {
        long id = client.add("flag", true);

        Saver saver = newSaver();
        saver.stage();
        saver.set("scratch", "value", id);
        saver.find(Criteria.where().key("flag").operator(Operator.EQUALS)
                .value(true), ids -> {
                    throw new IllegalStateException("rejected");
                });

        boolean caught = false;
        try {
            saver.commit();
        }
        catch (IllegalStateException e) {
            caught = true;
            saver.abort();
        }

        Assert.assertTrue(caught);
        Assert.assertTrue(client.select("scratch", id).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code consumer} passed to
     * {@link Saver#select(String, Criteria, java.util.function.Consumer)
     * select} can record further reads on the {@link BatchSaver} without
     * mutating the validator list currently being iterated &mdash; the nested
     * recordings must start a fresh batch and resolve cleanly inside the next
     * flush.
     * <p>
     * <strong>Start state:</strong> A record with {@code name = "alpha"} and
     * {@code flag = true}, plus a second record with {@code flag = true} that
     * does not match the select.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record a {@code select} on {@code name} for the {@code "alpha"}
     * record whose consumer in turn records a {@code find} for
     * {@code flag = true}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The select consumer runs without throwing
     * {@link java.util.ConcurrentModificationException
     * ConcurrentModificationException}, the nested {@code find} resolves inside
     * the next flush, and the captured ids include both records.
     */
    @Test
    public void testSelectConsumerCanRecordNestedReads() {
        long alpha = client.add("name", "alpha");
        client.add("flag", true, alpha);
        long bravo = client.add("name", "bravo");
        client.add("flag", true, bravo);

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Set<Long>> nestedResult = new AtomicReference<>();
        saver.select("name",
                Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("alpha"),
                result -> saver.find(Criteria.where().key("flag")
                        .operator(Operator.EQUALS).value(true),
                        nestedResult::set));
        Assert.assertNotNull(saver.commit());

        Assert.assertNotNull(nestedResult.get());
        Assert.assertTrue(nestedResult.get().contains(alpha));
        Assert.assertTrue(nestedResult.get().contains(bravo));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code consumer} passed to
     * {@link Saver#select(String, Criteria, java.util.function.Consumer)
     * select} can record another nested
     * {@link Saver#select(String, Criteria, java.util.function.Consumer)
     * select} on the same {@link BatchSaver} &mdash; the deeper case that
     * exercises recursive {@code flushReads} re-entry against the
     * snapshot-and-clear pattern.
     * <p>
     * <strong>Start state:</strong> One record matches the outer select on
     * {@code name = "outer"}; one record matches the inner select on
     * {@code color = "red"}; one record matches the deepest find on
     * {@code category = "X"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record a {@code select} on {@code name} whose consumer records a
     * second {@code select} on {@code color} whose consumer records a
     * {@code find} on {@code category}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> All three reads resolve cleanly without
     * throwing {@link java.util.ConcurrentModificationException
     * ConcurrentModificationException}, and the captured find result contains
     * the matching record.
     */
    @Test
    public void testSelectConsumerCanRecursivelyRecordSelect() {
        long outer = client.add("name", "outer");
        client.add("color", "red", outer);
        long match = client.add("category", "X");

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Set<Long>> findResult = new AtomicReference<>();
        saver.select("name",
                Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("outer"),
                outerResult -> saver.select("color",
                        Criteria.where().key("color").operator(Operator.EQUALS)
                                .value("red"),
                        innerResult -> saver.find(
                                Criteria.where().key("category")
                                        .operator(Operator.EQUALS).value("X"),
                                findResult::set)));
        Assert.assertNotNull(saver.commit());

        Assert.assertNotNull(findResult.get());
        Assert.assertTrue(findResult.get().contains(match));
    }

}
