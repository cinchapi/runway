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

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.runway.validation.Validator;

/**
 * Tests for the single-key atomic operations on {@link Record}:
 * {@link Record#exchange(String, Object) exchange},
 * {@link Record#getAndUpdate(String, java.util.function.UnaryOperator)
 * getAndUpdate} and
 * {@link Record#updateAndGet(String, java.util.function.UnaryOperator)
 * updateAndGet}.
 *
 * @author Jeff Nelson
 */
public class RecordAtomicOperationTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a successful {@code exchange} durably
     * persists the replacement and syncs the in-memory field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code value = 0}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("value", 10L)}.</li>
     * <li>Re-load the {@link Meter} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true}, the
     * re-loaded {@link Meter} has {@code value == 10} and the original
     * instance's in-memory {@code value} is also {@code 10}.
     */
    @Test
    public void testExchangePersistsReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        Assert.assertTrue(meter.exchange("value", 10L));
        Assert.assertEquals(10, meter.value);
        Assert.assertEquals(10, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that a stale {@code exchange} fails without
     * writing and without disturbing either the stored or the in-memory state.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose stored value
     * was changed through a second loaded copy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} with {@code value = 0}.</li>
     * <li>Load a fresh copy, set {@code value = 5}, and save it.</li>
     * <li>Call {@code exchange("value", 10L)} on the original (now stale)
     * instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code false}, the
     * database still holds {@code 5} and the stale instance still holds
     * {@code 0} in memory.
     */
    @Test
    public void testExchangeFailsWhenStale() {
        Meter meter = new Meter();
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.value = 5;
        runway.save(fresh);
        Assert.assertFalse(meter.exchange("value", 10L));
        Assert.assertEquals(0, meter.value);
        Assert.assertEquals(5, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} supports
     * {@link Record}-typed fields by exchanging one link for another.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} linked to one
     * {@link Owner}, with a second saved {@link Owner} available.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} whose {@code owner} is the first
     * {@link Owner}.</li>
     * <li>Call {@code exchange("owner", second)}.</li>
     * <li>Re-load the {@link Meter} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true}, the
     * in-memory {@code owner} is the second {@link Owner} and the re-loaded
     * {@link Meter} links to the second {@link Owner Owner's} id.
     */
    @Test
    public void testExchangeSupportsLinkFields() {
        Owner first = new Owner();
        Owner second = new Owner();
        Meter meter = new Meter();
        meter.owner = first;
        runway.save(meter, second);
        Assert.assertTrue(meter.exchange("owner", second));
        Assert.assertSame(second, meter.owner);
        Assert.assertEquals(second.id(),
                runway.load(Meter.class, meter.id()).owner.id());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} accepts a replacement
     * that satisfies the field's {@link ValidatedBy} validator.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code score}
     * field requires an even value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} with {@code score = 2}.</li>
     * <li>Call {@code exchange("score", 4L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true} and the
     * re-loaded {@link Meter} has {@code score == 4}.
     */
    @Test
    public void testExchangeAcceptsValidReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        Assert.assertTrue(meter.exchange("score", 4L));
        Assert.assertEquals(4, runway.load(Meter.class, meter.id()).score);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a replacement
     * that fails the field's {@link ValidatedBy} validator before anything is
     * written.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code score}
     * field requires an even value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} with {@code score = 2}.</li>
     * <li>Call {@code exchange("score", 3L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown and
     * the stored {@code score} remains {@code 2}.
     */
    @Test
    public void testExchangeRejectsInvalidReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        try {
            meter.exchange("score", 3L);
            Assert.fail("Expected an IllegalStateException");
        }
        catch (IllegalStateException e) {
            Assert.assertEquals(2, runway.load(Meter.class, meter.id()).score);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects an empty
     * replacement for a {@link Required} field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code label}
     * field is {@link Required}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("label", "")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown.
     */
    @Test(expected = IllegalStateException.class)
    public void testExchangeRejectsEmptyReplacementForRequiredField() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("label", "");
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a
     * {@link Unique} field because a single-key atomic operation cannot enforce
     * the uniqueness constraint.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("code", "abc")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExchangeRejectsUniqueField() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("code", "abc");
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a collection
     * field because it does not store a single value.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("tags", "abc")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExchangeRejectsCollectionField() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("tags", "abc");
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a key that
     * does not name an intrinsic field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("wat", 1L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExchangeRejectsUnknownKey() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("wat", 1L);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a
     * {@code null} replacement because null is represented as key absence and
     * cannot be exchanged in atomically.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("value", null)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExchangeRejectsNullReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("value", null);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects the operation
     * when the field has no in-memory value to use as the expected operand.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code note}
     * field is {@code null}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("note", "hello")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown.
     */
    @Test(expected = IllegalStateException.class)
    public void testExchangeRejectsNullCurrentValue() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("note", "hello");
    }

    /**
     * <strong>Goal:</strong> Verify that a successful {@code exchange} on a
     * record with no unsaved changes leaves the record with no unsaved changes.
     * <p>
     * <strong>Start state:</strong> A freshly saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} and confirm it has no unsaved changes.</li>
     * <li>Call {@code exchange("value", 10L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code hasUnsavedChanges()} returns
     * {@code false} after the exchange.
     */
    @Test
    public void testExchangeKeepsCleanRecordClean() {
        Meter meter = new Meter();
        runway.save(meter);
        Assert.assertFalse(meter.hasUnsavedChanges());
        Assert.assertTrue(meter.exchange("value", 10L));
        Assert.assertFalse(meter.hasUnsavedChanges());
    }

    /**
     * <strong>Goal:</strong> Verify that a successful {@code exchange} on a
     * record with pending changes preserves those changes so a later
     * {@code save} still persists them.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with an unsaved
     * modification to a different field.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}, then set {@code label = "changed"} without
     * saving.</li>
     * <li>Call {@code exchange("value", 10L)}.</li>
     * <li>Save the {@link Meter} and re-load it from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The record reports unsaved changes after the
     * exchange, and the re-loaded {@link Meter} has both {@code label ==
     * "changed"} and {@code value == 10}.
     */
    @Test
    public void testExchangePreservesPendingChanges() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.label = "changed";
        Assert.assertTrue(meter.hasUnsavedChanges());
        Assert.assertTrue(meter.exchange("value", 10L));
        Assert.assertTrue(meter.hasUnsavedChanges());
        Assert.assertTrue(meter.save());
        Meter loaded = runway.load(Meter.class, meter.id());
        Assert.assertEquals("changed", loaded.label);
        Assert.assertEquals(10, loaded.value);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} returns the prior
     * value and durably persists the updated one.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code value = 0}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 5)}.</li>
     * <li>Re-load the {@link Meter} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code 0}, the in-memory
     * {@code value} is {@code 5} and the re-loaded {@link Meter} has
     * {@code value == 5}.
     */
    @Test
    public void testGetAndUpdateReturnsPriorValueAndPersists() {
        Meter meter = new Meter();
        runway.save(meter);
        long before = meter.getAndUpdate("value", (Long v) -> v + 5);
        Assert.assertEquals(0, before);
        Assert.assertEquals(5, meter.value);
        Assert.assertEquals(5, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} recovers from a
     * stale read by re-applying the update against refreshed state.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose stored value
     * was changed through a second loaded copy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} with {@code value = 0}.</li>
     * <li>Load a fresh copy, set {@code value = 5}, and save it.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)} on the original (now
     * stale) instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code 5} (the refreshed
     * prior value) and the re-loaded {@link Meter} has {@code value == 6}.
     */
    @Test
    public void testGetAndUpdateRecoversFromStaleRead() {
        Meter meter = new Meter();
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.value = 5;
        runway.save(fresh);
        long before = meter.getAndUpdate("value", (Long v) -> v + 1);
        Assert.assertEquals(5, before);
        Assert.assertEquals(6, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code getAndUpdate} whose update
     * function returns the value unchanged verifies freshness without recording
     * a write.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with a known revision
     * history for {@code value}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} and count the revisions for {@code value}.</li>
     * <li>Call {@code getAndUpdate("value", v -> v)}.</li>
     * <li>Count the revisions for {@code value} again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns the current value and the
     * revision count does not change.
     */
    @Test
    public void testGetAndUpdateWithoutChangeVerifiesWithoutWrite() {
        Meter meter = new Meter();
        runway.save(meter);
        int revisions = meter.audit("value").size();
        long result = meter.getAndUpdate("value", (Long v) -> v);
        Assert.assertEquals(0, result);
        Assert.assertEquals(revisions, meter.audit("value").size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code updateAndGet} returns the value
     * that the update produced.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code value = 0}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code updateAndGet("value", v -> v + 5)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code 5} and the re-loaded
     * {@link Meter} has {@code value == 5}.
     */
    @Test
    public void testUpdateAndGetReturnsUpdatedValue() {
        Meter meter = new Meter();
        runway.save(meter);
        long after = meter.updateAndGet("value", (Long v) -> v + 5);
        Assert.assertEquals(5, after);
        Assert.assertEquals(5, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that concurrent {@code getAndUpdate}
     * increments from independently loaded copies never lose an update.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code value = 0} and a {@link Runway} configured with a generous
     * pause-free {@link AtomicRetryPolicy}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Rebuild the {@link Runway} with
     * {@code AtomicRetryPolicy.create(1000, 0)}.</li>
     * <li>Save a {@link Meter}.</li>
     * <li>Start 4 threads; each loads its own copy and performs 25
     * {@code getAndUpdate("value", v -> v + 1)} calls.</li>
     * <li>Join all threads and re-load the {@link Meter}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> No thread throws and the final stored value is
     * exactly {@code 100}.
     */
    @Test
    public void testConcurrentGetAndUpdateNeverLosesIncrements()
            throws InterruptedException {
        rebuildRunway(AtomicRetryPolicy.create(1000, 0));
        Meter meter = new Meter();
        runway.save(meter);
        int threads = 4;
        int increments = 25;
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; ++i) {
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                    Meter copy = runway.load(Meter.class, meter.id());
                    for (int j = 0; j < increments; ++j) {
                        copy.getAndUpdate("value", (Long v) -> v + 1);
                    }
                }
                catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            workers[i].start();
        }
        start.countDown();
        for (Thread worker : workers) {
            worker.join(60000);
        }
        Assert.assertNull(failure.get());
        Assert.assertEquals(threads * increments,
                runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} throws
     * {@link RetryExhaustedException} when contention persists beyond the
     * configured {@link AtomicRetryPolicy} limit.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} configured with a
     * zero-retry {@link AtomicRetryPolicy} and a {@link Meter} whose stored
     * value was changed through a second loaded copy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Rebuild the {@link Runway} with
     * {@code AtomicRetryPolicy.create(0, 0)}.</li>
     * <li>Save a {@link Meter} with {@code value = 0}.</li>
     * <li>Load a fresh copy, set {@code value = 5}, and save it.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)} on the original (now
     * stale) instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RetryExhaustedException} is thrown
     * because the first attempt fails and no retry is permitted.
     */
    @Test(expected = RetryExhaustedException.class)
    public void testGetAndUpdateThrowsWhenRetriesAreExhausted() {
        rebuildRunway(AtomicRetryPolicy.create(0, 0));
        Meter meter = new Meter();
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.value = 5;
        runway.save(fresh);
        meter.getAndUpdate("value", (Long v) -> v + 1);
    }

    /**
     * Replace the test's {@link Runway} with one configured to use the
     * specified {@code policy}.
     *
     * @param policy the {@link AtomicRetryPolicy} for the rebuilt
     *            {@link Runway}
     */
    private void rebuildRunway(AtomicRetryPolicy policy) {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
        runway = runwayBuilder().atomicRetryPolicy(policy).build();
    }

    /**
     * A {@link Validator} that only accepts even numbers.
     *
     * @author Jeff Nelson
     */
    public static class EvenValidator implements Validator {

        @Override
        public boolean validate(Object object) {
            return ((Long) object) % 2 == 0;
        }

        @Override
        public String getErrorMessage() {
            return "The value must be even";
        }

    }

    /**
     * A {@link Record} with a numeric gauge and a representative field for each
     * atomic-operation eligibility rule.
     *
     * @author Jeff Nelson
     */
    public static class Meter extends Record {

        /**
         * A required label; atomic operations must reject an empty replacement.
         */
        @Required
        public String label = "meter";

        /**
         * The gauge that atomic operations target.
         */
        public long value = 0;

        /**
         * A unique key; atomic operations must reject it.
         */
        @Unique
        public String code = null;

        /**
         * A multi-value field; atomic operations must reject it.
         */
        public Set<String> tags = null;

        /**
         * A validated field; atomic operations must reject a value that fails
         * validation.
         */
        @ValidatedBy(EvenValidator.class)
        public long score = 2;

        /**
         * A link field; atomic operations must support exchanging links.
         */
        public Owner owner = null;

        /**
         * A nullable field; atomic operations must reject an absent expected
         * value.
         */
        public String note = null;

    }

    /**
     * A minimal {@link Record} that {@link Meter} links to.
     *
     * @author Jeff Nelson
     */
    public static class Owner extends Record {

        /**
         * A placeholder attribute.
         */
        public String name = "owner";

    }

}
