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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.db.Saver.Timing;
import com.google.common.collect.ImmutableSet;

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
     * <strong>Goal:</strong> Verify that when a {@code consumer} throws inside
     * {@link Saver#commit()}, the writes recorded against the
     * {@link BatchSaver} are not persisted, because the writes submission is
     * skipped.
     * <p>
     * <strong>Start state:</strong> A record exists with {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record a {@code set} of a new field's value.</li>
     * <li>Record a {@link Timing#DEFERRED deferred} {@code select} whose
     * {@code consumer} throws.</li>
     * <li>Call {@code commit} and catch the exception.</li>
     * <li>Call {@code abort} to release the server-side staged
     * transaction.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exception arrives from {@code commit} and
     * the set's key is not present on the record.
     */
    @Test
    public void testConsumerThrowSkipsWritesSubmission() {
        long id = client.add("flag", true);

        Saver saver = newSaver();
        saver.stage();
        saver.set("scratch", "value", id);
        saver.select("flag", Criteria.where().key("flag")
                .operator(Operator.EQUALS).value(true), result -> {
                    throw new IllegalStateException("rejected");
                }, Timing.DEFERRED);

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
     * <strong>Goal:</strong> Verify that an
     * {@link Saver#observe(java.util.Collection, long, java.util.function.Consumer)
     * observe} consumer that throws does not turn a committed transaction into
     * a reported failure.
     * <p>
     * <strong>Start state:</strong> A record exists with {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record a {@code set} of a new field's value.</li>
     * <li>Record an {@code observe} whose consumer throws.</li>
     * <li>Call {@code commit}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code commit} returns {@code true} without
     * throwing, and the recorded write is durable.
     */
    @Test
    public void testObserverThrowDoesNotFailCommit() {
        long id = client.add("flag", true);

        Saver saver = newSaver();
        saver.stage();
        saver.set("scratch", "value", id);
        saver.observe(ImmutableSet.of("flag"), id, data -> {
            throw new IllegalStateException("rejected");
        });
        Assert.assertTrue(saver.commit());

        Assert.assertEquals("value", client.get("scratch", id));
    }

    /**
     * <strong>Goal:</strong> Verify that an
     * {@link Saver#observe(java.util.Collection, long, java.util.function.Consumer)
     * observe} consumer that throws does not stop the consumer of a later
     * {@code observe} from running.
     * <p>
     * <strong>Start state:</strong> Two records exist, each with
     * {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record an {@code observe} on the first record whose consumer
     * throws.</li>
     * <li>Record an {@code observe} on the second record whose consumer
     * captures the result.</li>
     * <li>Call {@code commit}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code commit} returns {@code true} and the
     * second consumer receives the second record's stored values.
     */
    @Test
    public void testObserverThrowDoesNotBlockLaterObserver() {
        long first = client.add("flag", true);
        long second = client.add("flag", true);

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Map<String, Set<Object>>> observed = new AtomicReference<>();
        saver.observe(ImmutableSet.of("flag"), first, data -> {
            throw new IllegalStateException("rejected");
        });
        saver.observe(ImmutableSet.of("flag"), second, observed::set);
        Assert.assertTrue(saver.commit());

        Assert.assertNotNull(observed.get());
        Assert.assertTrue(observed.get().get("flag").contains(true));
    }

    /**
     * <strong>Goal:</strong> Verify that an
     * {@link Saver#observe(java.util.Collection, long, java.util.function.Consumer)
     * observe} that a {@code consumer} records receives the values its own read
     * returned, because the read it belongs to rides a later submission than
     * the one the {@code consumer} was dispatched against.
     * <p>
     * <strong>Start state:</strong> A record exists with
     * {@code name = "alpha"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record an {@link Timing#INLINE inline} {@code select} on {@code name}
     * whose {@code consumer} records an {@code observe} of {@code name} in the
     * matching record.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code observe} consumer receives the
     * stored {@code name}.
     */
    @Test
    public void testObserveRecordedByConsumerReceivesItsOwnReadResult() {
        long id = client.add("name", "alpha");

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Map<String, Set<Object>>> observed = new AtomicReference<>();
        saver.select("name",
                Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("alpha"),
                result -> saver.observe(ImmutableSet.of("name"), id,
                        observed::set));
        Assert.assertTrue(saver.commit());

        Assert.assertNotNull(observed.get());
        Assert.assertEquals(ImmutableSet.of("alpha"),
                observed.get().get("name"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code consumer} passed to
     * {@link Saver#select(String, Criteria, java.util.function.Consumer)
     * select} can record a further read on the {@link BatchSaver} without
     * mutating the consumer list currently being iterated &mdash; the nested
     * recording must start a fresh batch and resolve inside the next flush.
     * <p>
     * <strong>Start state:</strong> A record with {@code name = "alpha"} and
     * {@code flag = true}, plus a second record with {@code flag = true} that
     * does not match the outer select.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record an {@link Timing#INLINE inline} {@code select} on {@code name}
     * for the {@code "alpha"} record whose {@code consumer} in turn records a
     * {@link Timing#DEFERRED deferred} {@code select} on {@code flag}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The outer {@code consumer} runs without
     * throwing {@link java.util.ConcurrentModificationException
     * ConcurrentModificationException}, the nested {@code select} resolves
     * inside the next flush, and its result covers both records.
     */
    @Test
    public void testSelectConsumerCanRecordNestedReads() {
        long alpha = client.add("name", "alpha");
        client.add("flag", true, alpha);
        long bravo = client.add("name", "bravo");
        client.add("flag", true, bravo);

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Map<Long, Set<Object>>> nested = new AtomicReference<>();
        saver.select("name",
                Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("alpha"),
                result -> saver.select(
                        "flag", Criteria.where().key("flag")
                                .operator(Operator.EQUALS).value(true),
                        nested::set, Timing.DEFERRED));
        Assert.assertTrue(saver.commit());

        Assert.assertNotNull(nested.get());
        Assert.assertTrue(nested.get().containsKey(alpha));
        Assert.assertTrue(nested.get().containsKey(bravo));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code consumer} passed to
     * {@link Saver#select(String, Criteria, java.util.function.Consumer)
     * select} can record another {@link Timing#INLINE inline} {@code select} on
     * the same {@link BatchSaver} &mdash; the deeper case that exercises
     * recursive read-flush re-entry against the snapshot-and-clear pattern.
     * <p>
     * <strong>Start state:</strong> One record matches the outer select on
     * {@code name = "outer"}; one record matches the inner select on
     * {@code color = "red"}; one record matches the deepest select on
     * {@code category = "X"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link BatchSaver}.</li>
     * <li>Record an {@link Timing#INLINE inline} {@code select} on {@code name}
     * whose {@code consumer} records a second inline {@code select} on
     * {@code color} whose {@code consumer} records a {@link Timing#DEFERRED
     * deferred} {@code select} on {@code category}.</li>
     * <li>Commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> All three reads resolve without throwing
     * {@link java.util.ConcurrentModificationException
     * ConcurrentModificationException}, and the deepest result covers the
     * matching record.
     */
    @Test
    public void testSelectConsumerCanRecursivelyRecordSelect() {
        long outer = client.add("name", "outer");
        client.add("color", "red", outer);
        long match = client.add("category", "X");

        Saver saver = newSaver();
        saver.stage();
        AtomicReference<Map<Long, Set<Object>>> deepest = new AtomicReference<>();
        saver.select("name",
                Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("outer"),
                outerResult -> saver.select("color",
                        Criteria.where().key("color").operator(Operator.EQUALS)
                                .value("red"),
                        innerResult -> saver.select("category",
                                Criteria.where().key("category")
                                        .operator(Operator.EQUALS).value("X"),
                                deepest::set, Timing.DEFERRED)));
        Assert.assertTrue(saver.commit());

        Assert.assertNotNull(deepest.get());
        Assert.assertTrue(deepest.get().containsKey(match));
    }

}
