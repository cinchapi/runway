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
 * Unit tests for {@link EventualSaver} that combine the shared {@link Saver}
 * contract with the implementation's batched semantics.
 *
 * @author Jeff Nelson
 */
public class EventualSaverTest extends SaverTest {

    @Override
    protected Saver newSaver() {
        Concourse connection = Concourse.at().port(server.getClientPort())
                .connect();
        return new EventualSaver(connection);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link EventualSaver} validator
     * does <em>not</em> run at recording time &mdash; the record-then-batch
     * shape requires that validators only fire after the reads submission
     * resolves inside {@link Saver#commit()}.
     * <p>
     * <strong>Start state:</strong> A record matches {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link EventualSaver}.</li>
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

        Assert.assertTrue(saver.commit());
        Assert.assertTrue("validator should have run inside commit",
                validatorRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify that when a validator throws inside
     * {@link Saver#commit()}, the writes recorded against the
     * {@link EventualSaver} are <em>not</em> persisted &mdash; the second
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
     * select} can record further reads on the {@link EventualSaver} without
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
     * <li>Stage the {@link EventualSaver}.</li>
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
        Assert.assertTrue(saver.commit());

        Assert.assertNotNull(nestedResult.get());
        Assert.assertTrue(nestedResult.get().contains(alpha));
        Assert.assertTrue(nestedResult.get().contains(bravo));
    }

}
