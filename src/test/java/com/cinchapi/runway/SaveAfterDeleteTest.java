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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;

/**
 * Tests that a save never restores a {@link Record} whose data another writer
 * erased. Each test runs once against the
 * {@link com.cinchapi.runway.db.BatchSaver BatchSaver} and once against the
 * {@link com.cinchapi.runway.db.IncrementalSaver IncrementalSaver} so the
 * contract holds on both save paths.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class SaveAfterDeleteTest extends RunwayBaseClientServerTest {

    /**
     * A test user record.
     *
     * @author Jeff Nelson
     */
    public static class TUser extends Record {

        /**
         * The user's name.
         */
        public String name;

        /**
         * The user's manager.
         */
        public TUser manager;

        /**
         * Construct a new instance.
         *
         * @param name the user's name
         */
        public TUser(String name) {
            this.name = name;
        }
    }

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

    /**
     * Whether the save path under test uses bulk commands.
     */
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
    public SaveAfterDeleteTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * Return {@code true} if {@code id} holds any data in the database.
     *
     * @param id the record id to describe
     * @return {@code true} if the record holds data
     */
    private boolean exists(long id) {
        Concourse concourse = runway.connections.request();
        try {
            return !concourse.describe(id).isEmpty();
        }
        finally {
            runway.connections.release(concourse);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a save of a {@link Record} whose data
     * another writer erased fails instead of restoring the record.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Change the name on the first instance and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link DeletedRecordException} that names the record, and the record
     * holds no data afterward.
     */
    @Test
    public void testSaveThrowsWhenRecordWasDeleted() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        TUser other = runway.load(TUser.class, user.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        user.name = "resurrected";
        try {
            runway.save(user);
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {
            Assert.assertEquals(user.id(), e.id());
        }
        Assert.assertFalse(exists(user.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a failed save leaves no trace of the
     * deleted {@link Record} for a query over its class.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Change the name on the first instance and save it, expecting the save
     * to fail.</li>
     * <li>Load every {@link TUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> No loaded {@link TUser} carries the deleted
     * record's id.
     */
    @Test
    public void testFailedSaveLeavesNoRecordInClassQuery() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        long id = user.id();
        TUser other = runway.load(TUser.class, id);
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        user.name = "resurrected";
        try {
            runway.save(user);
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {/* expected */}
        Assert.assertTrue(runway.load(TUser.class).stream()
                .noneMatch(record -> record.id() == id));
    }

    /**
     * <strong>Goal:</strong> Verify that the existence check does not reject a
     * save of a {@link Record} that the database still holds.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Change its name and save it again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the new name persists.
     */
    @Test
    public void testSaveSucceedsWhenRecordStillExists() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        user.name = "jeffery";
        Assert.assertTrue(runway.save(user));
        Assert.assertEquals("jeffery",
                runway.load(TUser.class, user.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that the existence check does not reject
     * the first save of a new {@link Record}, which has no stored data to
     * check.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link TUser} and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the record holds data.
     */
    @Test
    public void testSaveSucceedsForNewRecord() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        Assert.assertTrue(exists(user.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that deleting a {@link Record} that another
     * writer already deleted stays a no-op instead of failing.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Delete the first instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second deletion succeeds and the record
     * holds no data.
     */
    @Test
    public void testDeleteOfAlreadyDeletedRecordSucceeds() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        TUser other = runway.load(TUser.class, user.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        user.deleteOnSave();
        Assert.assertTrue(runway.save(user));
        Assert.assertFalse(exists(user.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save of an unchanged {@link Record}
     * whose data another writer erased writes nothing and does not fail, since
     * a save with no changes restores nothing.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Save the first instance without changing it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the record holds no
     * data.
     */
    @Test
    public void testSaveWithoutChangesAfterDeleteWritesNothing() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        TUser other = runway.load(TUser.class, user.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        Assert.assertTrue(runway.save(user));
        Assert.assertFalse(exists(user.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that the existence check inside a
     * {@link Transaction} observes the {@link Transaction Transaction's} own
     * staged writes, so a re-save of a {@link Record} that the same
     * {@link Transaction} created is accepted.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a {@link Transaction}.</li>
     * <li>Save a new {@link TUser} through it.</li>
     * <li>Change the name on the same instance and save it again.</li>
     * <li>Commit the {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves and the commit succeed, and the
     * second name persists.
     */
    @Test
    public void testResaveWithinTransactionAcceptsRecordItCreated() {
        TUser user = new TUser("jeff");
        try (Transaction transaction = runway.startTransaction()) {
            Assert.assertTrue(transaction.save(user));
            user.name = "jeffery";
            Assert.assertTrue(transaction.save(user));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("jeffery",
                runway.load(TUser.class, user.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that a save inside a {@link Transaction} of
     * a {@link Record} whose data another writer erased fails and poisons the
     * {@link Transaction}, so none of its staged writes can commit.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes before the {@link Transaction} opens.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Change the name on the first instance.</li>
     * <li>Open a {@link Transaction} and save the first instance through
     * it.</li>
     * <li>Attempt to commit the {@link Transaction}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link DeletedRecordException} that names the record, the commit is
     * refused with an {@link IllegalStateException}, and the record holds no
     * data afterward.
     */
    @Test
    public void testSaveWithinTransactionThrowsWhenRecordWasDeleted() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        TUser other = runway.load(TUser.class, user.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        user.name = "resurrected";
        try (Transaction transaction = runway.startTransaction()) {
            try {
                transaction.save(user);
                Assert.fail("Expected DeletedRecordException");
            }
            catch (DeletedRecordException e) {
                Assert.assertEquals(user.id(), e.id());
            }
            try {
                transaction.commit();
                Assert.fail("Expected the commit to be refused");
            }
            catch (IllegalStateException e) {/* expected */}
        }
        Assert.assertFalse(exists(user.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save whose only change is realm
     * membership does not write into a {@link Record} whose data another writer
     * erased, since a realm change is not an unsaved change but is still a
     * write.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Add the first instance to a realm and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link DeletedRecordException} and the record holds no data afterward.
     */
    @Test
    public void testRealmOnlySaveThrowsWhenRecordWasDeleted() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        TUser other = runway.load(TUser.class, user.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        user.addRealm("tenant-a");
        try {
            runway.save(user);
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {
            Assert.assertEquals(user.id(), e.id());
        }
        Assert.assertFalse(exists(user.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save refused because a linked
     * {@link Record} holds no data names the linked {@link Record} rather than
     * the {@link Record} that the caller passed to the save.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that links to a
     * second {@link TUser} whose data another instance erases.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} that links to a manager {@link TUser}.</li>
     * <li>Load a second instance of the manager and delete that instance.</li>
     * <li>Change the manager's name and save the {@link TUser} that links to
     * it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link DeletedRecordException} that names the manager, and the manager
     * holds no data afterward.
     */
    @Test
    public void testSaveThrowsWhenLinkedRecordWasDeleted() {
        TUser manager = new TUser("boss");
        TUser user = new TUser("jeff");
        user.manager = manager;
        Assert.assertTrue(runway.save(user));
        TUser other = runway.load(TUser.class, manager.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        manager.name = "resurrected";
        try {
            runway.save(user);
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {
            Assert.assertEquals(manager.id(), e.id());
        }
        Assert.assertFalse(exists(manager.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save whose only write is the author
     * attribution does not write into a {@link Record} whose data another
     * writer erased, since an attribution is not an unsaved change but is still
     * a write.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes, attributed to a saved author {@link TUser}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an author {@link TUser} and a subject {@link TUser}.</li>
     * <li>Load a second instance of the subject and delete that instance.</li>
     * <li>Attribute the subject to the author without changing any field or
     * realm, then save the subject.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link DeletedRecordException} that names the subject, and the subject
     * holds no data afterward.
     */
    @Test
    public void testAuthorOnlySaveThrowsWhenRecordWasDeleted() {
        TUser author = new TUser("uma");
        Assert.assertTrue(runway.save(author));
        TUser subject = new TUser("jeff");
        Assert.assertTrue(runway.save(subject));
        TUser other = runway.load(TUser.class, subject.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        Reflection.set("_author", author, subject); // (authorized)
        try {
            runway.save(subject);
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {
            Assert.assertEquals(subject.id(), e.id());
        }
        Assert.assertFalse(exists(subject.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that the deleted-record refusal reaches the
     * caller ahead of a stale-write failure when both apply.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that a second
     * instance deletes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Change the name on the first instance and save it with stale-write
     * prevention.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link DeletedRecordException}, not a {@link StaleDataException}, and the
     * record holds no data afterward.
     */
    @Test
    public void testDeletedRecordRefusalPrecedesStaleWriteFailure() {
        TUser user = new TUser("jeff");
        Assert.assertTrue(runway.save(user));
        TUser other = runway.load(TUser.class, user.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        user.name = "resurrected";
        try {
            runway.save(true, user);
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {
            Assert.assertEquals(user.id(), e.id());
        }
        Assert.assertFalse(exists(user.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save refused because one
     * {@link Record} holds no data writes nothing for the other {@link Record
     * Records} of the same save.
     * <p>
     * <strong>Start state:</strong> Two saved {@link TUser TUsers}, one of
     * which a second instance deletes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link TUser TUsers} together.</li>
     * <li>Load a second instance of one of them and delete that instance.</li>
     * <li>Change the name on both and save them together.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save throws a
     * {@link DeletedRecordException} that names the deleted {@link TUser}, and
     * the surviving {@link TUser} still holds the name it was saved with.
     */
    @Test
    public void testRefusedSaveWritesNothingForOtherRecords() {
        TUser doomed = new TUser("doomed");
        TUser healthy = new TUser("healthy");
        Assert.assertTrue(runway.save(doomed, healthy));
        TUser other = runway.load(TUser.class, doomed.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        doomed.name = "resurrected";
        healthy.name = "changed";
        try {
            runway.save(doomed, healthy);
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {
            Assert.assertEquals(doomed.id(), e.id());
        }
        Assert.assertEquals("healthy",
                runway.load(TUser.class, healthy.id()).name);
    }

}
