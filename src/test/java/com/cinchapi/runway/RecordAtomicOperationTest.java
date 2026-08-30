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
import java.util.function.UnaryOperator;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
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
     * <strong>Goal:</strong> Verify that a successful {@code exchange} on a
     * {@link String} field persists the replacement, locking the symmetry
     * between the expected operand's serialized form and the form a
     * {@code save} stores.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code label = "meter"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("label", "updated")}.</li>
     * <li>Re-load the {@link Meter} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true}, the
     * re-loaded {@link Meter} has {@code label == "updated"} and the original
     * instance's in-memory {@code label} is also {@code "updated"}.
     */
    @Test
    public void testExchangePersistsStringReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        Assert.assertTrue(meter.exchange("label", "updated"));
        Assert.assertEquals("updated", meter.label);
        Assert.assertEquals("updated",
                runway.load(Meter.class, meter.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a successful {@code exchange} on a
     * {@link Timestamp} field persists the replacement, locking the symmetry
     * between the expected operand's serialized form and the form a
     * {@code save} stores.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code updated}
     * field holds the construction-time {@link Timestamp}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("updated", ...)} with a new
     * {@link Timestamp}.</li>
     * <li>Re-load the {@link Meter} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true} and both the
     * in-memory and the re-loaded {@code updated} equal the replacement.
     */
    @Test
    public void testExchangePersistsTimestampReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        Timestamp replacement = Timestamp.now();
        Assert.assertTrue(meter.exchange("updated", replacement));
        Assert.assertEquals(replacement, meter.updated);
        Assert.assertEquals(replacement,
                runway.load(Meter.class, meter.id()).updated);
    }

    /**
     * <strong>Goal:</strong> Verify that a successful {@code exchange} on an
     * enum field persists the replacement, locking the symmetry between the
     * expected operand's serialized form and the form a {@code save} stores.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code color = RED}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("color", GREEN)}.</li>
     * <li>Re-load the {@link Meter} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true}, the
     * re-loaded {@link Meter} has {@code color == GREEN} and the original
     * instance's in-memory {@code color} is also {@code GREEN}.
     */
    @Test
    public void testExchangePersistsEnumReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        Assert.assertTrue(meter.exchange("color", Color.GREEN));
        Assert.assertSame(Color.GREEN, meter.color);
        Assert.assertSame(Color.GREEN,
                runway.load(Meter.class, meter.id()).color);
    }

    /**
     * <strong>Goal:</strong> Verify that a successful {@code exchange} on a
     * {@link Tag} field persists the replacement, locking the symmetry between
     * the expected operand's serialized form and the form a {@code save}
     * stores.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code token = "alpha"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("token", ...)} with a new {@link Tag}.</li>
     * <li>Re-load the {@link Meter} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true} and both the
     * in-memory and the re-loaded {@code token} equal the replacement.
     */
    @Test
    public void testExchangePersistsTagReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        Tag replacement = Tag.create("beta");
        Assert.assertTrue(meter.exchange("token", replacement));
        Assert.assertEquals(replacement, meter.token);
        Assert.assertEquals(replacement,
                runway.load(Meter.class, meter.id()).token);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a
     * serialized-object field, because its type is not a primitive, an enum, or
     * a {@link Tag}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("spec", ...)} with a new {@link Spec}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExchangeRejectsSerializedObjectField() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("spec", new Spec());
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
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a
     * {@link Record}-typed field, because the linked record's state cannot be
     * covered by a single-key operation.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} linked to one
     * {@link Owner}, with a second saved {@link Owner} available.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} whose {@code owner} is the first
     * {@link Owner}.</li>
     * <li>Call {@code exchange("owner", second)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the stored link still targets the first {@link Owner}.
     */
    @Test
    public void testExchangeRejectsLinkTypedField() {
        Owner first = new Owner();
        Owner second = new Owner();
        Meter meter = new Meter();
        meter.owner = first;
        runway.save(meter, second);
        try {
            meter.exchange("owner", second);
            Assert.fail("Expected an IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            Assert.assertEquals(first.id(),
                    runway.load(Meter.class, meter.id()).owner.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a
     * {@link DeferredReference}-typed field, matching the refusal of every
     * link-typed field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code ref}
     * wraps one saved {@link Owner}, with a second saved {@link Owner}
     * available.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Owner Owners}.</li>
     * <li>Save a {@link Meter} whose {@code ref} wraps the first
     * {@link Owner}.</li>
     * <li>Call {@code exchange("ref", ...)} with a {@link DeferredReference}
     * that wraps the second {@link Owner}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the stored reference still resolves to the first {@link Owner}.
     */
    @Test
    public void testExchangeRejectsDeferredReferenceTypedField() {
        Owner first = new Owner();
        Owner second = new Owner();
        runway.save(first, second);
        Meter meter = new Meter();
        meter.ref = new DeferredReference<>(first);
        runway.save(meter);
        try {
            meter.exchange("ref", new DeferredReference<>(second));
            Assert.fail("Expected an IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            Assert.assertEquals(first.id(),
                    runway.load(Meter.class, meter.id()).ref.get().id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} rejects a
     * {@link Record}-typed field, matching the {@code exchange} contract.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} linked to a saved
     * {@link Owner}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} whose {@code owner} is an {@link Owner}.</li>
     * <li>Call {@code getAndUpdate("owner", o -> o)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetAndUpdateRejectsLinkTypedField() {
        Owner owner = new Owner();
        Meter meter = new Meter();
        meter.owner = owner;
        runway.save(meter);
        meter.getAndUpdate("owner", (Owner o) -> o);
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
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a transient
     * field because a transient field is never stored.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("scratch", "abc")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExchangeRejectsTransientField() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("scratch", "abc");
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
     * <strong>Goal:</strong> Verify that {@code exchange} treats a {@code null}
     * in-memory value as an expectation of absence and stores the replacement
     * when the field has no stored value.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code note}
     * field is {@code null} and has no stored value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("note", "hello")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code true}, the
     * in-memory {@code note} is {@code "hello"}, and the re-loaded
     * {@link Meter} stores the same value.
     */
    @Test
    public void testExchangeSetsValueWhenFieldHasNoValue() {
        Meter meter = new Meter();
        runway.save(meter);
        Assert.assertTrue(meter.exchange("note", "hello"));
        Assert.assertEquals("hello", meter.note);
        Assert.assertEquals("hello", runway.load(Meter.class, meter.id()).note);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@code exchange} that expects
     * absence fails when a concurrent writer stored a value first, so a stale
     * observation of absence never overwrites real data.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code note}
     * was set through a second loaded copy while the original instance still
     * observes {@code null}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Load a fresh copy, set {@code note = "other"}, and save it.</li>
     * <li>Call {@code exchange("note", "hello")} on the original (now stale)
     * instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code false}, the
     * original in-memory {@code note} is still {@code null}, and the stored
     * {@code note} remains {@code "other"}.
     */
    @Test
    public void testExchangeExpectingAbsenceFailsWhenValueConcurrentlyAppears() {
        Meter meter = new Meter();
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.note = "other";
        runway.save(fresh);
        Assert.assertFalse(meter.exchange("note", "hello"));
        Assert.assertNull(meter.note);
        Assert.assertEquals("other", runway.load(Meter.class, meter.id()).note);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@code exchange} that expects
     * absence fails on a {@link Record} that was never saved, so the exchange
     * never writes a lone field into a record that does not exist in the
     * database.
     * <p>
     * <strong>Start state:</strong> A {@link Meter} that is pinned to a
     * {@link Runway} instance but never saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Meter} and {@code assign} it to {@code runway}
     * without a save.</li>
     * <li>Call {@code exchange("note", "hello")}.</li>
     * <li>Describe the {@link Meter Meter's} id through a raw {@link Concourse}
     * connection.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code false}, the
     * in-memory {@code note} is still {@code null}, and the database stores no
     * data at all for the id.
     */
    @Test
    public void testExchangeExpectingAbsenceFailsWhenRecordNeverSaved() {
        Meter meter = new Meter();
        meter.assign(runway);
        Assert.assertFalse(meter.exchange("note", "hello"));
        Assert.assertNull(meter.note);
        Concourse concourse = runway.connections.request();
        try {
            Assert.assertTrue(concourse.describe(meter.id()).isEmpty());
        }
        finally {
            runway.connections.release(concourse);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an {@code exchange} that expects
     * absence fails when the {@link Record Record's} data was concurrently
     * erased, so the exchange never resurrects the record as a lone field
     * without its metadata.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose data was erased
     * through a raw {@link Concourse} connection while the instance still
     * observes {@code note == null}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Erase every stored value for its id through a raw {@link Concourse}
     * connection.</li>
     * <li>Call {@code exchange("note", "hello")}.</li>
     * <li>Describe the {@link Meter Meter's} id through a raw {@link Concourse}
     * connection.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code false}, the
     * in-memory {@code note} is still {@code null}, and the database stores no
     * data at all for the id.
     */
    @Test
    public void testExchangeExpectingAbsenceFailsWhenRecordConcurrentlyErased() {
        Meter meter = new Meter();
        runway.save(meter);
        Concourse concourse = runway.connections.request();
        try {
            concourse.clear(meter.id());
            Assert.assertFalse(meter.exchange("note", "hello"));
            Assert.assertNull(meter.note);
            Assert.assertTrue(concourse.describe(meter.id()).isEmpty());
        }
        finally {
            runway.connections.release(concourse);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a replacement
     * that is not an instance of the field's type before anything is written.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code value = 0}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("value", "oops")} with a {@link String}
     * replacement on the {@code long} field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the stored {@code value} remains {@code 0}.
     */
    @Test
    public void testExchangeRejectsWrongTypedReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        try {
            meter.exchange("value", "oops");
            Assert.fail("Expected an IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            Assert.assertEquals(0, runway.load(Meter.class, meter.id()).value);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} rejects a replacement
     * whose boxed type does not match a primitive field's type, so the stored
     * type never diverges from the type a {@code save} would write.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with
     * {@code value = 0}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("value", 10)} with an {@link Integer}
     * replacement on the {@code long} field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExchangeRejectsMismatchedBoxedReplacement() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.exchange("value", 10);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} refuses a field that
     * the governing {@link DynamicWritePolicy} does not permit writing, so the
     * atomic operations cannot bypass the policy that governs {@code set}.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} configured with
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} and a saved
     * {@link Meter} whose {@code hidden} field is private.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Rebuild the {@link Runway} with the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.</li>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code exchange("hidden", 2L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link NonWritableFieldException} is thrown
     * and the stored {@code hidden} value remains {@code 1}.
     */
    @Test
    public void testExchangeRefusesFieldTheDynamicWritePolicyForbids() {
        rebuildRunway(builder -> builder
                .dynamicWritePolicy(DynamicWritePolicy.javaDefaults()));
        Meter meter = new Meter();
        runway.save(meter);
        try {
            meter.exchange("hidden", 2L);
            Assert.fail("Expected a NonWritableFieldException");
        }
        catch (NonWritableFieldException e) {
            Assert.assertEquals(1, runway.load(Meter.class, meter.id()).hidden);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} refuses a field
     * that the governing {@link DynamicWritePolicy} does not permit writing,
     * matching the {@code exchange} contract.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} configured with
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} and a saved
     * {@link Meter} whose {@code hidden} field is private.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Rebuild the {@link Runway} with the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.</li>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code getAndUpdate("hidden", v -> v + 1)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link NonWritableFieldException} is thrown.
     */
    @Test(expected = NonWritableFieldException.class)
    public void testGetAndUpdateRefusesFieldTheDynamicWritePolicyForbids() {
        rebuildRunway(builder -> builder
                .dynamicWritePolicy(DynamicWritePolicy.javaDefaults()));
        Meter meter = new Meter();
        runway.save(meter);
        meter.getAndUpdate("hidden", (Long v) -> v + 1);
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
     * <strong>Goal:</strong> Verify that {@code exchange} uses the staged
     * in-memory value for the target key as the expected operand, so a staged
     * change that the database never stored makes the exchange fail.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with an unsaved
     * modification to {@code value}, the target key.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}, then set {@code value = 7} without saving.</li>
     * <li>Call {@code exchange("value", 10L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exchange returns {@code false}, the staged
     * {@code 7} survives in memory, the record still reports unsaved changes
     * and the stored {@code value} remains {@code 0}.
     */
    @Test
    public void testExchangeFailsWhenTargetKeyHasStagedChange() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.value = 7;
        Assert.assertTrue(meter.hasUnsavedChanges());
        Assert.assertFalse(meter.exchange("value", 10L));
        Assert.assertEquals(7, meter.value);
        Assert.assertTrue(meter.hasUnsavedChanges());
        Assert.assertEquals(0, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code exchange} refuses a record that
     * is staged for deletion, so an exchange never persists a value that the
     * pending save immediately deletes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} that is staged for
     * deletion.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}, then call {@code deleteOnSave()}.</li>
     * <li>Call {@code exchange("value", 10L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown and
     * the stored {@code value} remains {@code 0}.
     */
    @Test
    public void testExchangeRefusesRecordStagedForDeletion() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.deleteOnSave();
        try {
            meter.exchange("value", 10L);
            Assert.fail("Expected an IllegalStateException");
        }
        catch (IllegalStateException e) {
            Assert.assertEquals(0, runway.load(Meter.class, meter.id()).value);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a successful {@code exchange}
     * invalidates the cached audit trail, so a later {@code audit} call on the
     * same instance sees the revision the exchange wrote.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose audit trail was
     * already read once on the same instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} and read {@code audit("value")} to populate the
     * instance's cached trail.</li>
     * <li>Call {@code exchange("value", 10L)}.</li>
     * <li>Read {@code audit("value")} again on the same instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second audit read contains more revisions
     * than the first.
     */
    @Test
    public void testExchangeInvalidatesCachedAudit() {
        Meter meter = new Meter();
        runway.save(meter);
        int revisions = meter.audit("value").size();
        Assert.assertTrue(meter.exchange("value", 10L));
        Assert.assertTrue(meter.audit("value").size() > revisions);
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
     * <li>Save a {@link Meter} and count the revisions for {@code value}
     * through a freshly loaded copy, so the count reads the database instead of
     * an instance-level cache.</li>
     * <li>Call {@code getAndUpdate("value", v -> v)}.</li>
     * <li>Count the revisions for {@code value} through another freshly loaded
     * copy.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns the current value and the
     * revision count does not change.
     */
    @Test
    public void testGetAndUpdateWithoutChangeVerifiesWithoutWrite() {
        Meter meter = new Meter();
        runway.save(meter);
        int revisions = runway.load(Meter.class, meter.id()).audit("value")
                .size();
        long result = meter.getAndUpdate("value", (Long v) -> v);
        Assert.assertEquals(0, result);
        Assert.assertEquals(revisions,
                runway.load(Meter.class, meter.id()).audit("value").size());
    }

    /**
     * <strong>Goal:</strong> Verify that a no-op {@code getAndUpdate} that
     * retries after a stale read invalidates the cached audit trail, so a later
     * audit read includes the concurrent revision the retry observed.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose audit trail was
     * already read once on the same instance, and whose stored value was
     * changed through a second loaded copy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} and read {@code audit("value")} to populate the
     * instance's cached trail.</li>
     * <li>Load a fresh copy, set {@code value = 5}, and save it.</li>
     * <li>Call {@code getAndUpdate("value", v -> v)} on the original (now
     * stale) instance.</li>
     * <li>Read {@code audit("value")} again on the same instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code 5} (the re-read value)
     * without a write, and the second audit read contains more revisions than
     * the first.
     */
    @Test
    public void testGetAndUpdateNoOpRetryInvalidatesCachedAudit() {
        Meter meter = new Meter();
        runway.save(meter);
        int revisions = meter.audit("value").size();
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.value = 5;
        runway.save(fresh);
        long result = meter.getAndUpdate("value", (Long v) -> v);
        Assert.assertEquals(5, result);
        Assert.assertTrue(meter.audit("value").size() > revisions);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} refuses a record
     * that has unsaved changes, so a lost race can never silently discard the
     * staged state.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with an unsaved
     * modification to a different field.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}, then set {@code label = "changed"} without
     * saving.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown,
     * the staged {@code label} survives in memory, the record still reports
     * unsaved changes and the stored {@code value} remains {@code 0}.
     */
    @Test
    public void testGetAndUpdateRefusesRecordWithUnsavedChanges() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.label = "changed";
        try {
            meter.getAndUpdate("value", (Long v) -> v + 1);
            Assert.fail("Expected an IllegalStateException");
        }
        catch (IllegalStateException e) {
            Assert.assertEquals("changed", meter.label);
            Assert.assertTrue(meter.hasUnsavedChanges());
            Assert.assertEquals(0, runway.load(Meter.class, meter.id()).value);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} refuses a record
     * that was never saved instead of burning retries and blaming contention.
     * <p>
     * <strong>Start state:</strong> A new, never-saved {@link Meter}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Meter} without saving it.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetAndUpdateRefusesUnsavedRecord() {
        Meter meter = new Meter();
        meter.getAndUpdate("value", (Long v) -> v + 1);
    }

    /**
     * <strong>Goal:</strong> Verify that unsaved realm changes survive a
     * contended {@code getAndUpdate}, because a retry re-reads only the target
     * key and touches nothing else in the record.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose stored value
     * was changed through a second loaded copy, with an unsaved realm addition.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Load a fresh copy, set {@code value = 5}, and save it.</li>
     * <li>Call {@code addRealm("test")} on the original instance without a
     * save.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)} on the original (now
     * stale) instance, then {@code save()} it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code 5} (the re-read prior
     * value), the staged realm membership survives the retry and persists on
     * the save, and the stored {@code value} is {@code 6}.
     */
    @Test
    public void testGetAndUpdatePreservesUnsavedRealmChangesAcrossRetry() {
        Meter meter = new Meter();
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.value = 5;
        runway.save(fresh);
        meter.addRealm("test");
        long before = meter.getAndUpdate("value", (Long v) -> v + 1);
        Assert.assertEquals(5, before);
        Assert.assertTrue(meter.realms().contains("test"));
        Assert.assertTrue(meter.save());
        Meter loaded = runway.load(Meter.class, meter.id());
        Assert.assertTrue(loaded.realms().contains("test"));
        Assert.assertEquals(6, loaded.value);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} refuses a record
     * that is staged for deletion, so the operation never writes a value that
     * the pending save immediately deletes.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} that is staged for
     * deletion.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}, then call {@code deleteOnSave()}.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown and
     * the stored {@code value} remains {@code 0}.
     */
    @Test
    public void testGetAndUpdateRefusesRecordStagedForDeletion() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.deleteOnSave();
        try {
            meter.getAndUpdate("value", (Long v) -> v + 1);
            Assert.fail("Expected an IllegalStateException");
        }
        catch (IllegalStateException e) {
            Assert.assertEquals(0, runway.load(Meter.class, meter.id()).value);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an uncontended {@code getAndUpdate} on
     * a scalar field succeeds without touching a dirty linked record held by a
     * different field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose linked
     * {@link Owner} was modified after its save, with no concurrent writer.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} whose {@code owner} is an {@link Owner}.</li>
     * <li>Change the {@link Owner Owner's} {@code name} without a save.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code 0}, the stored
     * {@code value} is {@code 1}, the in-memory {@code owner} is the same
     * instance and its staged {@code name} survives.
     */
    @Test
    public void testGetAndUpdatePreservesDirtyLinkedRecordWhenUncontended() {
        Owner owner = new Owner();
        Meter meter = new Meter();
        meter.owner = owner;
        runway.save(meter);
        owner.name = "changed";
        long before = meter.getAndUpdate("value", (Long v) -> v + 1);
        Assert.assertEquals(0, before);
        Assert.assertSame(owner, meter.owner);
        Assert.assertEquals("changed", owner.name);
        Assert.assertEquals(1, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that a dirty linked record elsewhere in the
     * record survives a contended {@code getAndUpdate}, because a retry
     * re-reads only the target key instead of the whole record.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose stored value
     * was changed through a second loaded copy and whose linked {@link Owner}
     * was modified in memory without a save.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} whose {@code owner} is an {@link Owner}.</li>
     * <li>Load a fresh copy, set {@code value = 5}, and save it.</li>
     * <li>Change the {@link Owner Owner's} {@code name} without a save.</li>
     * <li>Call {@code getAndUpdate("value", v -> v + 1)} on the original (now
     * stale) instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code 5} (the re-read prior
     * value), the in-memory {@code owner} is the same instance with its staged
     * {@code name} intact and the stored {@code value} is {@code 6}.
     */
    @Test
    public void testGetAndUpdateRetryPreservesDirtyLinkedRecord() {
        Owner owner = new Owner();
        Meter meter = new Meter();
        meter.owner = owner;
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.value = 5;
        runway.save(fresh);
        owner.name = "changed";
        long before = meter.getAndUpdate("value", (Long v) -> v + 1);
        Assert.assertEquals(5, before);
        Assert.assertSame(owner, meter.owner);
        Assert.assertEquals("changed", owner.name);
        Assert.assertEquals(6, runway.load(Meter.class, meter.id()).value);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} retries against
     * {@code null} when the stored value is concurrently removed, so the
     * function decides how to fill the now-empty field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose stored
     * {@code note} was cleared through a second loaded copy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter} with {@code note = "hello"}.</li>
     * <li>Load a fresh copy, set {@code note = null}, and save it.</li>
     * <li>Call {@code getAndUpdate("note", s -> s == null ? "reset" : s + "!")}
     * on the original (now stale) instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code null} (the value the
     * settled attempt replaced) and the stored {@code note} is {@code "reset"}.
     */
    @Test
    public void testGetAndUpdateRetriesAgainstNullWhenValueConcurrentlyCleared() {
        Meter meter = new Meter();
        meter.note = "hello";
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.note = null;
        runway.save(fresh);
        String before = meter.getAndUpdate("note",
                (String s) -> s == null ? "reset" : s + "!");
        Assert.assertNull(before);
        Assert.assertEquals("reset", runway.load(Meter.class, meter.id()).note);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} applies the
     * function to {@code null} when the field has no value and fills the field
     * with the produced value.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code note}
     * field has no value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code getAndUpdate("note", s -> s == null ? "first" : s)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns {@code null} and the
     * re-loaded {@link Meter} stores {@code note == "first"}.
     */
    @Test
    public void testGetAndUpdateAppliesFunctionToNullWhenFieldHasNoValue() {
        Meter meter = new Meter();
        runway.save(meter);
        String before = meter.getAndUpdate("note",
                (String s) -> s == null ? "first" : s);
        Assert.assertNull(before);
        Assert.assertEquals("first", runway.load(Meter.class, meter.id()).note);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code getAndUpdate} rejects a
     * function that returns {@code null}, even when the current value is also
     * {@code null}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} whose {@code note}
     * field has no value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}.</li>
     * <li>Call {@code getAndUpdate("note", s -> null)}.</li>
     * <li>Catch the expected exception, then re-load the {@link Meter}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and the re-loaded {@link Meter} still has no {@code note}.
     */
    @Test
    public void testGetAndUpdateRejectsNullFunctionResult() {
        Meter meter = new Meter();
        runway.save(meter);
        boolean threw = false;
        try {
            meter.getAndUpdate("note", (String s) -> null);
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertNull(runway.load(Meter.class, meter.id()).note);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code updateAndGet} refuses a record
     * that has unsaved changes, matching the {@code getAndUpdate} contract.
     * <p>
     * <strong>Start state:</strong> A saved {@link Meter} with an unsaved
     * modification to a different field.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Meter}, then set {@code label = "changed"} without
     * saving.</li>
     * <li>Call {@code updateAndGet("value", v -> v + 1)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown.
     */
    @Test(expected = IllegalStateException.class)
    public void testUpdateAndGetRefusesRecordWithUnsavedChanges() {
        Meter meter = new Meter();
        runway.save(meter);
        meter.label = "changed";
        meter.updateAndGet("value", (Long v) -> v + 1);
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
        rebuildRunway(builder -> builder
                .atomicRetryPolicy(AtomicRetryPolicy.create(1000, 0)));
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
        rebuildRunway(builder -> builder
                .atomicRetryPolicy(AtomicRetryPolicy.create(0, 0)));
        Meter meter = new Meter();
        runway.save(meter);
        Meter fresh = runway.load(Meter.class, meter.id());
        fresh.value = 5;
        runway.save(fresh);
        meter.getAndUpdate("value", (Long v) -> v + 1);
    }

    /**
     * Replace the test's {@link Runway} with one built from a builder that
     * {@code config} customizes.
     *
     * @param config the customization to apply to the {@link Runway.Builder}
     */
    private void rebuildRunway(UnaryOperator<Runway.Builder> config) {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
        runway = config.apply(runwayBuilder()).build();
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
         * An enum field; a successful atomic operation must serialize the
         * expected operand to the same form a save stores.
         */
        public Color color = Color.RED;

        /**
         * A tag field; a successful atomic operation must serialize the
         * expected operand to the same form a save stores.
         */
        public Tag token = Tag.create("alpha");

        /**
         * A timestamp gauge; a successful atomic operation must serialize the
         * expected operand to the same form a save stores.
         */
        public Timestamp updated = Timestamp.now();

        /**
         * A serialized-object field; atomic operations must reject a
         * non-primitive scalar.
         */
        public Spec spec = new Spec();

        /**
         * A link field; atomic operations must reject it.
         */
        public Owner owner = null;

        /**
         * A deferred link field; atomic operations must reject it.
         */
        public DeferredReference<Owner> ref = null;

        /**
         * A nullable field; atomic operations treat its absent value as the
         * expected operand.
         */
        public String note = null;

        /**
         * A transient field that is never stored; atomic operations must reject
         * it.
         */
        public transient String scratch = null;

        /**
         * A private field; a restrictive {@link DynamicWritePolicy} must refuse
         * atomic operations on it.
         */
        private long hidden = 1;

    }

    /**
     * A plain object stored by the {@link Meter Meter's} {@code spec} field in
     * serialized form.
     *
     * @author Jeff Nelson
     */
    public static class Spec {

        /**
         * A placeholder attribute.
         */
        public long threshold = 1;

    }

    /**
     * The enum stored by the {@link Meter Meter's} {@code color} field.
     *
     * @author Jeff Nelson
     */
    public enum Color {

        /**
         * The value every {@link Meter} starts with.
         */
        RED,

        /**
         * The value an exchange swaps in.
         */
        GREEN;

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
