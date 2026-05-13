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

import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link Runway#save(Record...)} and
 * {@link Runway#save(boolean, Record...)} on the bulk-command save path enabled
 * when the connected server supports the Concourse Command API (1.0.0+).
 * <p>
 * The bulk path drives every save through an
 * {@link com.cinchapi.runway.db.EventualSaver EventualSaver} that batches stage
 * + audit + uniqueness {@code find} into one round trip, validates the results
 * client-side, and submits the writes plus {@code commit()} in a second round
 * trip. These tests verify that the save semantics &mdash; field persistence,
 * stale-data detection, uniqueness enforcement, cascading delete, record-graph
 * saves, and {@link Record#overrideSave() override-driven abort} &mdash; match
 * the legacy path the {@link com.cinchapi.runway.db.ImmediateSaver
 * ImmediateSaver} preserves.
 *
 * @author Jeff Nelson
 */
public class RunwayBulkSaveIntegrationTest extends RunwayBaseClientServerTest {

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

        Assert.assertFalse(duplicate.errors().isEmpty());
        Throwable err = duplicate.errors().iterator().next();
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
     * A simple {@link Record} type with a name and age, used as the baseline
     * for save tests.
     */
    class Person extends Record {

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
    class Pet extends Record {

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
    class UniqueNamed extends Record {

        @Unique
        String name;

        UniqueNamed(String name) {
            this.name = name;
        }
    }

    /**
     * A {@link Record} type whose {@link Record#overrideSave()} returns
     * {@code false} when {@code veto} is set, used to exercise the
     * override-driven abort path.
     */
    class Vetoing extends Record {

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

}
