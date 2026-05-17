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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.google.common.collect.ImmutableSet;

/**
 * Integration tests for {@link Runway#save(Record...)} and
 * {@link Runway#save(boolean, Record...)} that verify field persistence,
 * stale-data detection, uniqueness enforcement, cascading delete, record-graph
 * saves, and {@link Record#overrideSave() override-driven abort}. Each test
 * runs once against the {@link com.cinchapi.runway.db.BatchSaver BatchSaver}
 * and once against the {@link com.cinchapi.runway.db.IncrementalSaver
 * IncrementalSaver} so both save paths are exercised regardless of the
 * connected server's Command-API capability.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class RunwayBulkSaveIntegrationTest extends RunwayBaseClientServerTest {

    /**
     * Return the parameter matrix that drives each test once per save path.
     *
     * @return one row per {@link com.cinchapi.runway.db.Saver Saver}
     *         implementation
     */
    @Parameters(name = "bulkCommands={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to drive saves through the
     *            {@link com.cinchapi.runway.db.BatchSaver BatchSaver};
     *            {@code false} for the
     *            {@link com.cinchapi.runway.db.IncrementalSaver
     *            IncrementalSaver}
     */
    public RunwayBulkSaveIntegrationTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    public void beforeEachTest() {
        super.beforeEachTest();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that a single-record save persists every
     * field on the record when the bulk save path is active.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Person} with a name and an age.</li>
     * <li>Save it via {@link Record#save()}.</li>
     * <li>Reload it from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Person} has the same name
     * and age as the original.
     */
    @Test
    public void testBulkSavePersistsAllFields() {
        Person p = new Person("Alice", 30);
        Assert.assertTrue(p.save());

        Person loaded = runway.load(Person.class, p.id());
        Assert.assertEquals("Alice", loaded.name);
        Assert.assertEquals(30, loaded.age);
    }

    /**
     * <strong>Goal:</strong> Verify that a save involving a linked
     * {@link Record} graph persists both the root and the linked record on the
     * bulk path.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Person} and a {@link Pet} that links to it.</li>
     * <li>Save the {@link Pet}.</li>
     * <li>Reload both from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both records are persisted with their fields
     * intact and the {@link Pet Pet's} owner link resolves to the saved
     * {@link Person}.
     */
    @Test
    public void testBulkSavePersistsLinkedRecordGraph() {
        Person owner = new Person("Bob", 40);
        Pet pet = new Pet("Rex", owner);
        Assert.assertTrue(pet.save());

        Pet loadedPet = runway.load(Pet.class, pet.id());
        Assert.assertEquals("Rex", loadedPet.name);
        Assert.assertNotNull(loadedPet.owner);
        Assert.assertEquals(owner.id(), loadedPet.owner.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a save of multiple unrelated records
     * in a single call persists every one when the bulk path is active.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct three {@link Person Persons}.</li>
     * <li>Save them all in one {@link Runway#save(Record...)} call.</li>
     * <li>Count {@link Person Persons} in the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> All three are persisted.
     */
    @Test
    public void testBulkSavePersistsMultipleRecordsInOneCall() {
        Person a = new Person("A", 1);
        Person b = new Person("B", 2);
        Person c = new Person("C", 3);

        Assert.assertTrue(runway.save(a, b, c));

        Assert.assertEquals(3, runway.load(Person.class).size());
    }

    /**
     * <strong>Goal:</strong> Verify that a save which violates a {@link Unique}
     * constraint throws {@link IllegalStateException} and leaves the database
     * unchanged.
     * <p>
     * <strong>Start state:</strong> A {@link UniqueNamed} record with name
     * {@code "Alpha"} is already saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a first {@link UniqueNamed} with name {@code "Alpha"}.</li>
     * <li>Construct a second {@link UniqueNamed} with the same name.</li>
     * <li>Save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second save returns {@code false} and the
     * {@link UniqueNamed Record's} {@code errors} list contains an
     * {@link IllegalStateException} mentioning uniqueness. Only one record
     * exists in the database.
     */
    @Test
    public void testBulkSaveRejectsUniquenessViolation() {
        UniqueNamed first = new UniqueNamed("Alpha");
        Assert.assertTrue(first.save());

        UniqueNamed duplicate = new UniqueNamed("Alpha");
        Assert.assertFalse(duplicate.save());

        Assert.assertFalse(duplicate.errors.isEmpty());
        Throwable err = duplicate.errors.iterator().next();
        Assert.assertTrue(err instanceof IllegalStateException);
        Assert.assertTrue(err.getMessage().toLowerCase().contains("unique"));

        Assert.assertEquals(1, runway.load(UniqueNamed.class).size());
    }

    /**
     * <strong>Goal:</strong> Verify that a save with
     * {@code preventStaleWrites=true} detects external modification of the
     * target record and throws {@link StaleDataException}.
     * <p>
     * <strong>Start state:</strong> A {@link Person} has been saved and loaded,
     * then modified externally via a fresh {@link Runway} instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save and reload a {@link Person}.</li>
     * <li>Open a second {@link Runway}, load the same {@link Person}, modify
     * and save it.</li>
     * <li>Modify the original {@link Person} instance.</li>
     * <li>Call {@link Runway#save(boolean, Record...)} with
     * {@code preventStaleWrites=true}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws {@link StaleDataException}.
     */
    @Test
    public void testBulkSaveThrowsStaleDataWhenPreventStaleWritesIsTrue() {
        Person p = new Person("Carol", 25);
        Assert.assertTrue(p.save());
        Person loaded = runway.load(Person.class, p.id());

        Runway other = Runway.builder().port(server.getClientPort()).build();
        try {
            Person externallyModified = other.load(Person.class, p.id());
            externallyModified.age = 26;
            Assert.assertTrue(other.save(externallyModified));
        }
        finally {
            try {
                other.close();
            }
            catch (Exception ignored) {/* close failure not under test */}
        }

        loaded.age = 99;
        try {
            runway.save(true, loaded);
            Assert.fail("expected StaleDataException");
        }
        catch (StaleDataException expected) {
            // good
        }
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Record#overrideSave()
     * override} that returns {@code false} aborts the staged transaction
     * without persisting any writes on the bulk save path.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vetoing} with {@code veto=true}.</li>
     * <li>Save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code false} and no
     * {@link Vetoing} records exist in the database.
     */
    @Test
    public void testBulkSaveAbortsWhenOverrideReturnsFalse() {
        Vetoing v = new Vetoing("won't save", true);
        Assert.assertFalse(v.save());

        Assert.assertEquals(0, runway.load(Vetoing.class).size());
    }

    /**
     * <strong>Goal:</strong> Verify that two records saved in a single
     * {@link Runway#save(Record...)} call that would both write the same value
     * to a {@link Unique} field are rejected even though no such record exists
     * in the database when the save begins.
     * <p>
     * The per-call save path relies on the staged transaction to make earlier
     * records' writes visible to later uniqueness reads. The bulk path submits
     * every queued uniqueness {@code find} against a single pre-write snapshot,
     * so detecting duplicates between records in the same save call needs
     * explicit intra-batch detection.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct two {@link UniqueNamed} records with the same name.</li>
     * <li>Save them together in one {@link Runway#save(Record...)} call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code false}, at least one
     * of the records carries an {@link IllegalStateException} mentioning
     * uniqueness in its {@code errors} list, and zero {@link UniqueNamed}
     * records exist in the database.
     */
    @Test
    public void testBulkSaveRejectsIntraBatchUniquenessViolation() {
        UniqueNamed first = new UniqueNamed("Alpha");
        UniqueNamed second = new UniqueNamed("Alpha");

        Assert.assertFalse(runway.save(first, second));

        boolean rejected = first.errors.stream()
                .anyMatch(t -> t instanceof IllegalStateException
                        && t.getMessage().toLowerCase().contains("unique"))
                || second.errors.stream()
                        .anyMatch(t -> t instanceof IllegalStateException && t
                                .getMessage().toLowerCase().contains("unique"));
        Assert.assertTrue("expected an intra-batch uniqueness violation",
                rejected);
        Assert.assertEquals(0, runway.load(UniqueNamed.class).size());
    }

    /**
     * <strong>Goal:</strong> Verify that a named compound {@link Unique}
     * constraint whose fields include a sequence rejects two records saved in
     * one call when their sequences overlap on a single item and the scalar
     * fields all match &mdash; the case the canonical must enumerate via
     * cartesian product to catch overlap rather than only exact-set match.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct two {@link CompoundUnique} records with the same
     * {@code category} and overlapping {@code tags} &mdash; {@code [X, Y]} and
     * {@code [Y, Z]}.</li>
     * <li>Save them together in one {@link Runway#save(Record...)} call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code false}, at least one
     * of the records carries an {@link IllegalStateException} mentioning
     * uniqueness in its {@code errors} list, and zero {@link CompoundUnique}
     * records exist in the database.
     */
    @Test
    public void testBulkSaveRejectsIntraBatchOverlapInCompoundUniqueSequence() {
        CompoundUnique a = new CompoundUnique(ImmutableSet.of("X", "Y"),
                "cat1");
        CompoundUnique b = new CompoundUnique(ImmutableSet.of("Y", "Z"),
                "cat1");

        Assert.assertFalse(runway.save(a, b));

        boolean rejected = a.errors.stream()
                .anyMatch(t -> t instanceof IllegalStateException
                        && t.getMessage().toLowerCase().contains("unique"))
                || b.errors.stream()
                        .anyMatch(t -> t instanceof IllegalStateException && t
                                .getMessage().toLowerCase().contains("unique"));
        Assert.assertTrue(
                "expected an intra-batch overlap conflict on the compound "
                        + "unique constraint",
                rejected);
        Assert.assertEquals(0, runway.load(CompoundUnique.class).size());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Runway#save save} that fails
     * a {@link Unique @Unique} validation on the bulk path leaves the rejected
     * {@link Record} in an unsaved state, so a subsequent {@link Runway#save
     * save} of the same in-memory instance &mdash; with no further field
     * mutations &mdash; still writes the record's fields to the database when
     * the conflict is removed.
     * <p>
     * <strong>Start state:</strong> A {@link UniqueNamed} with name
     * {@code "Alpha"} is already saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a second in-memory {@link UniqueNamed} with the same name
     * and call {@link Record#save save}; the call returns {@code false}.</li>
     * <li>Delete the conflicting first {@link UniqueNamed} so the same name is
     * now free.</li>
     * <li>Call {@link Record#save save} on the original duplicate
     * {@link UniqueNamed} instance again, with no in-memory changes between
     * calls.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Exactly one {@link UniqueNamed} exists in the
     * database after the second save, with name {@code "Alpha"} &mdash; proving
     * the failed save did not silently mark the in-memory record clean and that
     * the second save actually wrote the record's fields.
     */
    @Test
    public void testFailedBulkSaveLeavesRecordReSavable() {
        UniqueNamed first = new UniqueNamed("Alpha");
        Assert.assertTrue(first.save());

        UniqueNamed dup = new UniqueNamed("Alpha");
        Assert.assertFalse(dup.save());

        first.deleteOnSave();
        Assert.assertTrue(first.save());

        Assert.assertTrue(dup.save());

        Set<UniqueNamed> all = runway.load(UniqueNamed.class);
        Assert.assertEquals(1, all.size());
        Assert.assertEquals("Alpha", all.iterator().next().name);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Runway#save save} that fails
     * with a {@link StaleDataException} on the bulk path under
     * {@code preventStaleWrites=true} leaves the in-memory {@link Record} in
     * its pre-save state, so a subsequent {@link Runway#save save} of the same
     * instance &mdash; even with the staleness check disabled and no
     * intervening reload &mdash; still observes the in-memory mutation and
     * writes it to the database.
     * <p>
     * <strong>Start state:</strong> A {@link Person} has been saved and loaded;
     * a second {@link Runway} writes an external modification to the same
     * database row so the in-memory copy is now stale.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save and reload a {@link Person} so the in-memory copy has a
     * checkpoint timestamp.</li>
     * <li>Open a second {@link Runway}, externally modify the same
     * {@link Person} row, and close the second {@link Runway}.</li>
     * <li>Modify the original in-memory {@link Person}.</li>
     * <li>Call {@link Runway#save(boolean, Record...) save} with
     * {@code preventStaleWrites=true}; catch the {@link StaleDataException}.
     * </li>
     * <li>Force the write by calling {@link Runway#save(Record...) save}
     * (staleness check off) on the same in-memory instance with no further
     * field mutations.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The forced save returns {@code true} and the
     * in-memory mutation is persisted &mdash; proving that the
     * {@link StaleDataException} did not silently update the record's
     * {@code __checksum} to match its current field state, which would
     * otherwise cause the forced save to skip the write loop entirely.
     */
    @Test
    public void testStaleDataFailureLeavesRecordReSavable() {
        Person p = new Person("Carol", 25);
        Assert.assertTrue(p.save());
        Person loaded = runway.load(Person.class, p.id());

        Runway other = Runway.builder().port(server.getClientPort()).build();
        try {
            Person externallyModified = other.load(Person.class, p.id());
            externallyModified.age = 26;
            Assert.assertTrue(other.save(externallyModified));
        }
        finally {
            try {
                other.close();
            }
            catch (Exception ignored) {/* close failure not under test */}
        }

        loaded.age = 99;
        try {
            runway.save(true, loaded);
            Assert.fail("expected StaleDataException");
        }
        catch (StaleDataException expected) {
            // good
        }

        Assert.assertTrue(runway.save(loaded));

        Person finalLoad = runway.load(Person.class, p.id());
        Assert.assertEquals(99, finalLoad.age);
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link CaptureDelete @CaptureDelete} cascade lookup on the bulk save path
     * observes link mutations queued earlier in the same {@link Runway#save
     * save} call, so that a record whose link was <em>moved</em> away from the
     * deletion target in this save is not falsely identified as still pointing
     * at the target and therefore is not nulled out by the cascade cleanup.
     * <p>
     * <strong>Start state:</strong> A {@link Custodian} record is saved with
     * {@code captured} pointing at an existing {@link Holding}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Reload the {@link Custodian} and the original {@link Holding}.</li>
     * <li>Construct a brand-new {@link Holding} and assign it to the
     * {@link Custodian Custodian's} {@code captured} field, replacing the
     * original.</li>
     * <li>Mark the original {@link Holding} for deletion via
     * {@link Record#deleteOnSave deleteOnSave}.</li>
     * <li>Call {@link Runway#save(Record...) save} with both records in one
     * call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Custodian Custodian's}
     * {@code captured} field still points at the new {@link Holding}; it has
     * not been nulled out by a stale cascade-delete lookup that read the
     * pre-save link state.
     */
    @Test
    public void testCascadeDeleteSeesInFlightLinkUpdate() {
        Holding original = new Holding("original");
        Custodian custodian = new Custodian(original);
        Assert.assertTrue(runway.save(custodian));

        Custodian loadedCustodian = runway.load(Custodian.class,
                custodian.id());
        Holding loadedOriginal = runway.load(Holding.class, original.id());

        Holding replacement = new Holding("replacement");
        loadedCustodian.captured = replacement;
        loadedOriginal.deleteOnSave();

        Assert.assertTrue(runway.save(loadedCustodian, loadedOriginal));

        Custodian reloaded = runway.load(Custodian.class, custodian.id());
        Assert.assertNotNull(
                "Custodian.captured must point at the replacement Holding, "
                        + "not be nulled by a stale cascade-delete lookup",
                reloaded.captured);
        Assert.assertEquals("replacement", reloaded.captured.name);
    }

    /**
     * A simple {@link Record} type with a name and age, used as the baseline
     * for save tests.
     */
    public static class Person extends Record {

        String name;

        int age;

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    /**
     * A {@link Record} type with a link to a {@link Person} owner, used to
     * exercise object-graph saves.
     */
    public static class Pet extends Record {

        String name;

        Person owner;

        Pet(String name, Person owner) {
            this.name = name;
            this.owner = owner;
        }
    }

    /**
     * A {@link Record} type with a {@link Unique}-annotated name field, used to
     * exercise uniqueness validation.
     */
    public static class UniqueNamed extends Record {

        @Unique
        String name;

        UniqueNamed(String name) {
            this.name = name;
        }
    }

    /**
     * A {@link Record} type with a named compound {@link Unique} constraint
     * across a sequence-valued field and a scalar field, used to exercise
     * intra-batch overlap detection in the bulk save path.
     */
    public static class CompoundUnique extends Record {

        @Unique(name = "bk")
        Set<String> tags;

        @Unique(name = "bk")
        String category;

        CompoundUnique(Set<String> tags, String category) {
            this.tags = tags;
            this.category = category;
        }
    }

    /**
     * A {@link Record} type whose {@link Record#overrideSave()} returns
     * {@code false} when {@code veto} is set, used to exercise the
     * override-driven abort path.
     */
    public static class Vetoing extends Record {

        String name;

        transient boolean veto;

        Vetoing(String name, boolean veto) {
            this.name = name;
            this.veto = veto;
        }

        @Override
        protected java.util.function.Supplier<Boolean> overrideSave() {
            return veto ? () -> false : null;
        }
    }

    /**
     * A {@link Record} type whose {@code captured} field is annotated with
     * {@link CaptureDelete @CaptureDelete}, used to exercise the cascade
     * cleanup that runs when the linked record is deleted.
     */
    public static class Custodian extends Record {

        @CaptureDelete
        Holding captured;

        Custodian(Holding captured) {
            this.captured = captured;
        }
    }

    /**
     * A simple {@link Record} type linked from {@link Custodian} via a
     * {@link CaptureDelete @CaptureDelete}-annotated field, used to exercise
     * the cascade-delete lookup path on the bulk save path.
     */
    public static class Holding extends Record {

        String name;

        Holding(String name) {
            this.name = name;
        }
    }

}
