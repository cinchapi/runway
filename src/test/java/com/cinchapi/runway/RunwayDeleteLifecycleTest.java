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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for delete notifications and their interplay with save
 * notifications in {@link Runway}.
 *
 * @author Jeff Nelson
 */
public class RunwayDeleteLifecycleTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a save which deletes a {@link Record}
     * fires the delete listener and removes the record from the database.
     * <p>
     * <strong>Start state:</strong> A saved {@link TrackedRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener via
     * {@link Runway.Builder#onDelete(java.util.function.Consumer)}.</li>
     * <li>Save a {@link TrackedRecord}.</li>
     * <li>Call {@link Record#deleteOnSave()} and save the record again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires with the deleted
     * {@link Record} and the record no longer loads from the database.
     */
    @Test
    public void testDeleteListenerCalledWhenSaveDeletesRecord()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            latch.countDown();
        }).build();

        TrackedRecord record = new TrackedRecord();
        record.name = "To Be Deleted";
        Assert.assertTrue(record.save());

        record.deleteOnSave();
        Assert.assertTrue(record.save());

        Assert.assertTrue("Delete listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(record));
        Assert.assertNull("Record should have been deleted",
                runway.load(TrackedRecord.class, record.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save which deletes a {@link Record}
     * does not fire the save listener.
     * <p>
     * <strong>Start state:</strong> A saved {@link TrackedRecord} and a
     * {@link Runway} with only a save listener registered.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener with a call counter.</li>
     * <li>Save a {@link TrackedRecord} and await the notification.</li>
     * <li>Call {@link Record#deleteOnSave()} and save the record again.</li>
     * <li>Wait to confirm no additional notification arrives.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save listener fires exactly once, for the
     * initial save only.
     */
    @Test
    public void testSaveListenerNotCalledWhenSaveDeletesRecord()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger saveCount = new AtomicInteger(0);

        runway.close();
        runway = runwayBuilder().onSave(record -> {
            saveCount.incrementAndGet();
            latch.countDown();
        }).build();

        TrackedRecord record = new TrackedRecord();
        record.name = "Saved Then Deleted";
        Assert.assertTrue(record.save());
        Assert.assertTrue("Save listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        record.deleteOnSave();
        Assert.assertTrue(record.save());

        // Give some time for any potential async processing
        Thread.sleep(1000);

        Assert.assertEquals(
                "Save listener should not fire when the save deletes the record",
                1, saveCount.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a typed delete listener only fires for
     * {@link Record Records} of the registered type.
     * <p>
     * <strong>Start state:</strong> A saved {@link TrackedRecord} and a saved
     * {@link OtherRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener for {@link TrackedRecord} only.</li>
     * <li>Delete the {@link OtherRecord} via a save.</li>
     * <li>Delete the {@link TrackedRecord} via a save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The listener fires only for the
     * {@link TrackedRecord}.
     */
    @Test
    public void testTypedDeleteListenerOnlyFiresForMatchingType()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(TrackedRecord.class, record -> {
            deletedRecords.add(record);
            latch.countDown();
        }).build();

        OtherRecord other = new OtherRecord();
        other.label = "Other";
        Assert.assertTrue(other.save());

        TrackedRecord tracked = new TrackedRecord();
        tracked.name = "Tracked";
        Assert.assertTrue(tracked.save());

        other.deleteOnSave();
        Assert.assertTrue(other.save());

        tracked.deleteOnSave();
        Assert.assertTrue(tracked.save());

        Assert.assertTrue("Typed delete listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(tracked));
    }

    /**
     * <strong>Goal:</strong> Verify that a standard save does not fire the
     * delete listener.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener with a call counter.</li>
     * <li>Save a {@link TrackedRecord} without marking it for deletion.</li>
     * <li>Wait to confirm no notification arrives.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener is never called.
     */
    @Test
    public void testDeleteListenerNotCalledOnStandardSave() throws Exception {
        AtomicInteger deleteCount = new AtomicInteger(0);

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deleteCount.incrementAndGet();
        }).build();

        TrackedRecord record = new TrackedRecord();
        record.name = "Standard Save";
        Assert.assertTrue(record.save());

        // Give some time for any potential async processing
        Thread.sleep(1000);

        Assert.assertEquals(
                "Delete listener should not fire for a standard save", 0,
                deleteCount.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a delete listener registered after
     * build via {@link Runway.Properties#onDelete(java.util.function.Consumer)}
     * fires when a save deletes a {@link Record}.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} built with no listeners.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Runway} without any listeners.</li>
     * <li>Register a delete listener via {@code properties().onDelete}.</li>
     * <li>Save a {@link TrackedRecord}, then delete it via a save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The post-build delete listener fires with the
     * deleted {@link Record}.
     */
    @Test
    public void testOnDeleteAfterBuild() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().build();

        runway.properties().onDelete(record -> {
            deletedRecords.add(record);
            latch.countDown();
        });

        TrackedRecord record = new TrackedRecord();
        record.name = "Post-Build Delete";
        Assert.assertTrue(record.save());

        record.deleteOnSave();
        Assert.assertTrue(record.save());

        Assert.assertTrue(
                "Post-build delete listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(record));
    }

    /**
     * <strong>Goal:</strong> Verify that a bulk save containing a modified
     * {@link Record} and a {@link Record} marked for deletion routes each to
     * the matching listener.
     * <p>
     * <strong>Start state:</strong> Two saved {@link TrackedRecord
     * TrackedRecords}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener and a delete listener.</li>
     * <li>Save two {@link TrackedRecord TrackedRecords} in bulk.</li>
     * <li>Modify one record and mark the other with
     * {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save listener fires for the two initial
     * saves and the update; the delete listener fires only for the deleted
     * {@link Record}.
     */
    @Test
    public void testMixedBulkSaveRoutesNotificationsByKind() throws Exception {
        CountDownLatch saveLatch = new CountDownLatch(3);
        CountDownLatch deleteLatch = new CountDownLatch(1);
        AtomicInteger saveCount = new AtomicInteger(0);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onSave(record -> {
            saveCount.incrementAndGet();
            saveLatch.countDown();
        }).onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        TrackedRecord kept = new TrackedRecord();
        kept.name = "Kept";
        TrackedRecord removed = new TrackedRecord();
        removed.name = "Removed";
        Assert.assertTrue(runway.save(kept, removed));

        kept.name = "Kept (Updated)";
        removed.deleteOnSave();
        Assert.assertTrue(runway.save(kept, removed));

        Assert.assertTrue("Save listener calls did not arrive within timeout",
                saveLatch.await(5, TimeUnit.SECONDS));
        Assert.assertTrue("Delete listener was not called within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));

        // Give some time to catch any extra, misrouted notifications
        Thread.sleep(1000);

        Assert.assertEquals(3, saveCount.get());
        Assert.assertEquals(1, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(removed));
        Assert.assertNull(runway.load(TrackedRecord.class, removed.id()));
        Assert.assertNotNull(runway.load(TrackedRecord.class, kept.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that delete listeners compose and that an
     * exception in one listener does not prevent the next from firing.
     * <p>
     * <strong>Start state:</strong> A saved {@link TrackedRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register two delete listeners; the first always throws.</li>
     * <li>Delete the {@link TrackedRecord} via a save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second listener still fires.
     */
    @Test
    public void testDeleteListenerCompositionAndErrorIsolation()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger secondCount = new AtomicInteger(0);

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            throw new RuntimeException(
                    "Intentional exception from first listener");
        }).onDelete(record -> {
            secondCount.incrementAndGet();
            latch.countDown();
        }).build();

        TrackedRecord record = new TrackedRecord();
        record.name = "Error Isolation";
        Assert.assertTrue(record.save());

        record.deleteOnSave();
        Assert.assertTrue(record.save());

        Assert.assertTrue(
                "Second delete listener was not called despite first throwing",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, secondCount.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a companion {@link Record} deleted
     * through {@link CascadeDelete} fires the delete listener alongside the
     * explicitly deleted {@link Record}.
     * <p>
     * <strong>Start state:</strong> A saved {@link CascadeParent} linked to a
     * saved {@link CascadeChild}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener.</li>
     * <li>Save a {@link CascadeParent} and its linked
     * {@link CascadeChild}.</li>
     * <li>Mark the parent with {@link Record#deleteOnSave()} and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both the parent
     * and the cascaded child, and neither record loads afterwards.
     */
    @Test
    public void testDeleteListenerFiredForCascadeDeletedCompanion()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            latch.countDown();
        }).build();

        CascadeChild child = new CascadeChild();
        child.name = "Cascade Child";
        CascadeParent parent = new CascadeParent();
        parent.name = "Cascade Parent";
        parent.child = child;
        Assert.assertTrue(runway.save(parent, child));

        parent.deleteOnSave();
        Assert.assertTrue(parent.save());

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(2, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(parent));
        Assert.assertTrue(deletedRecords.contains(child));
        Assert.assertNull(runway.load(CascadeParent.class, parent.id()));
        Assert.assertNull(runway.load(CascadeChild.class, child.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} which joins a
     * deletion through {@link JoinDelete} fires the delete listener alongside
     * the explicitly deleted {@link Record}.
     * <p>
     * <strong>Start state:</strong> A saved {@link JoinParent} whose
     * {@link JoinDelete} field links to a saved {@link JoinTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener.</li>
     * <li>Save a {@link JoinParent} linked to a {@link JoinTarget}.</li>
     * <li>Mark the target with {@link Record#deleteOnSave()} and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both the target
     * and the joined parent, and neither record loads afterwards.
     */
    @Test
    public void testDeleteListenerFiredForJoinDeletedRecord() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            latch.countDown();
        }).build();

        JoinTarget target = new JoinTarget();
        target.name = "Join Target";
        JoinParent parent = new JoinParent();
        parent.name = "Join Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(2, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(target));
        Assert.assertTrue(deletedRecords.contains(parent));
        Assert.assertNull(runway.load(JoinTarget.class, target.id()));
        Assert.assertNull(runway.load(JoinParent.class, parent.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} side-saved through
     * {@link CaptureDelete} cleanup fires the save listener while the deleted
     * {@link Record} fires the delete listener.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} whose
     * {@link CaptureDelete} field links to a saved {@link CaptureTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener and a delete listener.</li>
     * <li>Save a {@link CaptureParent} linked to a {@link CaptureTarget} and
     * await the two initial save notifications.</li>
     * <li>Mark the target with {@link Record#deleteOnSave()} and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for the target; the
     * save listener fires a third time for the side-saved parent; the parent's
     * reference is nullified in the database.
     */
    @Test
    public void testSaveListenerFiredForCaptureDeleteSideSave()
            throws Exception {
        CountDownLatch saveLatch = new CountDownLatch(3);
        CountDownLatch deleteLatch = new CountDownLatch(1);
        Set<Record> savedRecords = ConcurrentHashMap.newKeySet();
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onSave(record -> {
            savedRecords.add(record);
            saveLatch.countDown();
        }).onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        Assert.assertTrue(
                "Save listener did not fire for the side-saved record within timeout",
                saveLatch.await(5, TimeUnit.SECONDS));
        Assert.assertTrue("Delete listener was not called within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));

        Assert.assertTrue(savedRecords.contains(parent));
        Assert.assertEquals(1, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(target));

        // Assert against the raw stored data (before any load can perform
        // ad-hoc dangling link cleanup) to prove the capture cleanup was
        // committed durably
        Assert.assertTrue(
                "The capture cleanup must remove the stored reference",
                client.select("target", parent.id()).isEmpty());

        CaptureParent loaded = runway.load(CaptureParent.class, parent.id());
        Assert.assertNull(
                "Target reference should be null after the capture cleanup",
                loaded.target);
        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save which deletes both a
     * {@link CaptureTarget} and the {@link CaptureParent} that references it
     * fires the delete listener for both records and the save listener for
     * neither.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} whose
     * {@link CaptureDelete} field links to a saved {@link CaptureTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener and a delete listener.</li>
     * <li>Save the parent and target and await the two initial save
     * notifications.</li>
     * <li>Mark both records with {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the target ordered first so its
     * {@link CaptureDelete} cleanup runs before the parent.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both records,
     * the save listener fires no additional time, and neither record loads
     * afterwards.
     */
    @Test
    public void testDeleteListenerFiredWhenCaptureCleanedRecordDeletedInSameSave()
            throws Exception {
        CountDownLatch saveLatch = new CountDownLatch(2);
        CountDownLatch deleteLatch = new CountDownLatch(2);
        AtomicInteger saveCount = new AtomicInteger(0);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onSave(record -> {
            saveCount.incrementAndGet();
            saveLatch.countDown();
        }).onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));
        Assert.assertTrue(
                "Initial save notifications did not arrive within timeout",
                saveLatch.await(5, TimeUnit.SECONDS));

        target.deleteOnSave();
        parent.deleteOnSave();
        Assert.assertTrue(runway.save(target, parent));

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));

        // Give some time to catch any extra, misrouted notifications
        Thread.sleep(1000);

        Assert.assertEquals(2, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(parent));
        Assert.assertTrue(deletedRecords.contains(target));
        Assert.assertEquals(
                "Save listener should not fire for records that the save deleted",
                2, saveCount.get());
        Assert.assertNull(runway.load(CaptureParent.class, parent.id()));
        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a save which deletes both a
     * {@link CaptureParent} and its referenced {@link CaptureTarget} fires the
     * delete listener for both records when the parent is ordered first.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} whose
     * {@link CaptureDelete} field links to a saved {@link CaptureTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener and a delete listener.</li>
     * <li>Save the parent and target and await the two initial save
     * notifications.</li>
     * <li>Mark both records with {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the parent ordered first.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both records,
     * the save listener fires no additional time, and neither record loads
     * afterwards.
     */
    @Test
    public void testDeleteListenerFiredWhenCaptureParentOrderedFirstInSameSave()
            throws Exception {
        CountDownLatch saveLatch = new CountDownLatch(2);
        CountDownLatch deleteLatch = new CountDownLatch(2);
        AtomicInteger saveCount = new AtomicInteger(0);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onSave(record -> {
            saveCount.incrementAndGet();
            saveLatch.countDown();
        }).onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));
        Assert.assertTrue(
                "Initial save notifications did not arrive within timeout",
                saveLatch.await(5, TimeUnit.SECONDS));

        target.deleteOnSave();
        parent.deleteOnSave();
        Assert.assertTrue(runway.save(parent, target));

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));

        // Give some time to catch any extra, misrouted notifications
        Thread.sleep(1000);

        Assert.assertEquals(2, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(parent));
        Assert.assertTrue(deletedRecords.contains(target));
        Assert.assertEquals(
                "Save listener should not fire for records that the save deleted",
                2, saveCount.get());
        Assert.assertNull(runway.load(CaptureParent.class, parent.id()));
        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that when a save deletes a
     * {@link CaptureTarget} and also contains the modified
     * {@link CaptureParent} that references it, the save notification delivers
     * the caller's parent instance and checkpoints it.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} whose
     * {@link CaptureDelete} field links to a saved {@link CaptureTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener that records each notified instance and a
     * delete listener.</li>
     * <li>Modify the parent's name and mark the target with
     * {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the target ordered first so its
     * {@link CaptureDelete} cleanup runs before the parent.</li>
     * <li>Modify the parent again and save it with {@code preventStaleWrites}
     * enabled.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save notification for the parent delivers
     * the same instance that was saved, the parent's stored reference to the
     * target is removed, and the follow-up save with {@code preventStaleWrites}
     * succeeds without a {@link StaleDataException}.
     */
    @Test
    public void testCallerInstanceNotifiedWhenCaptureCleanedRecordInSameSave()
            throws Exception {
        CountDownLatch saveLatch = new CountDownLatch(3);
        CountDownLatch deleteLatch = new CountDownLatch(1);
        List<Record> savedRecords = new CopyOnWriteArrayList<>();
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onSave(record -> {
            savedRecords.add(record);
            saveLatch.countDown();
        }).onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        parent.name = "Capture Parent (Updated)";
        target.deleteOnSave();
        Assert.assertTrue(runway.save(target, parent));

        Assert.assertTrue("Save notifications did not arrive within timeout",
                saveLatch.await(5, TimeUnit.SECONDS));
        Assert.assertTrue("Delete listener was not called within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(target));

        // Assert against the raw stored data (before any load can perform
        // ad-hoc dangling link cleanup) to prove the capture cleanup was
        // committed durably
        Assert.assertTrue(
                "The capture cleanup must remove the stored reference",
                client.select("target", parent.id()).isEmpty());

        Record notified = null;
        for (Record record : savedRecords) {
            if(record.id() == parent.id()) {
                notified = record;
            }
        }
        Assert.assertSame(
                "The save notification must deliver the caller's instance",
                parent, notified);

        parent.name = "Capture Parent (Updated Again)";
        Assert.assertTrue(runway.save(true, parent));
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link CaptureDelete} cleanup for
     * a deleted {@link CaptureTarget} does not revert the unsaved changes of a
     * {@link CaptureParent} that is part of the same save.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} whose
     * {@link CaptureDelete} field links to a saved {@link CaptureTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener.</li>
     * <li>Modify the parent's name and mark the target with
     * {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the parent ordered first so its
     * changes stage before the cleanup for the target runs.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for the target, the
     * parent's updated name persists in the database, and the parent's stored
     * reference to the target is removed.
     */
    @Test
    public void testCaptureCleanupDoesNotRevertChangesOfRecordInSameSave()
            throws Exception {
        CountDownLatch deleteLatch = new CountDownLatch(1);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        parent.name = "Capture Parent (Updated)";
        target.deleteOnSave();
        Assert.assertTrue(runway.save(parent, target));

        Assert.assertTrue("Delete listener was not called within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));
        Assert.assertTrue(deletedRecords.contains(target));

        // Assert against the raw stored data (before any load can perform
        // ad-hoc dangling link cleanup) to prove the capture cleanup was
        // committed durably
        Assert.assertTrue(
                "The capture cleanup must remove the stored reference",
                client.select("target", parent.id()).isEmpty());

        CaptureParent loaded = runway.load(CaptureParent.class, parent.id());
        Assert.assertEquals(
                "The parent's unsaved changes must survive the capture cleanup",
                "Capture Parent (Updated)", loaded.name);
        Assert.assertNull(loaded.target);
        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link JoinParent} which joins a
     * deletion through {@link JoinDelete} fires the delete listener even when
     * an unchanged instance of it is part of the same save.
     * <p>
     * <strong>Start state:</strong> A saved {@link JoinParent} whose
     * {@link JoinDelete} field links to a saved {@link JoinTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener and a delete listener.</li>
     * <li>Save the parent and target and await the two initial save
     * notifications.</li>
     * <li>Mark only the target with {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the target ordered first so the join
     * deletion processes a freshly loaded copy of the parent before the
     * caller's unchanged instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both the target
     * and the joined parent, the save listener fires no additional time, and
     * neither record loads afterwards.
     */
    @Test
    public void testDeleteListenerFiredWhenJoinDeletedRecordAlsoInSameSave()
            throws Exception {
        CountDownLatch saveLatch = new CountDownLatch(2);
        CountDownLatch deleteLatch = new CountDownLatch(2);
        AtomicInteger saveCount = new AtomicInteger(0);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onSave(record -> {
            saveCount.incrementAndGet();
            saveLatch.countDown();
        }).onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        JoinTarget target = new JoinTarget();
        target.name = "Join Target";
        JoinParent parent = new JoinParent();
        parent.name = "Join Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));
        Assert.assertTrue(
                "Initial save notifications did not arrive within timeout",
                saveLatch.await(5, TimeUnit.SECONDS));

        target.deleteOnSave();
        Assert.assertTrue(runway.save(target, parent));

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));

        // Give some time to catch any extra, misrouted notifications
        Thread.sleep(1000);

        Assert.assertEquals(2, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(target));
        Assert.assertTrue(deletedRecords.contains(parent));
        Assert.assertEquals(
                "Save listener should not fire for records that the save deleted",
                2, saveCount.get());
        Assert.assertNull(runway.load(JoinTarget.class, target.id()));
        Assert.assertNull(runway.load(JoinParent.class, parent.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that the delete notification for a
     * {@link JoinParent} delivers the caller's instance when the parent is
     * ordered before the deleted {@link JoinTarget} in the same save.
     * <p>
     * <strong>Start state:</strong> A saved {@link JoinParent} whose
     * {@link JoinDelete} field links to a saved {@link JoinTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener that records each notified instance.</li>
     * <li>Mark only the target with {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the parent ordered first so the join
     * deletion finds the caller's instance already in the save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both records,
     * the notification for the parent delivers the caller's instance, and
     * neither record holds any stored data afterwards.
     */
    @Test
    public void testCallerInstanceNotifiedWhenJoinParentOrderedFirstInSameSave()
            throws Exception {
        CountDownLatch deleteLatch = new CountDownLatch(2);
        List<Record> deletedRecords = new CopyOnWriteArrayList<>();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        JoinTarget target = new JoinTarget();
        target.name = "Join Target";
        JoinParent parent = new JoinParent();
        parent.name = "Join Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        target.deleteOnSave();
        Assert.assertTrue(runway.save(parent, target));

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));

        Record notified = null;
        for (Record record : deletedRecords) {
            if(record.id() == parent.id()) {
                notified = record;
            }
        }
        Assert.assertSame(
                "The delete notification must deliver the caller's instance",
                parent, notified);
        Assert.assertTrue("The join deleted record must not survive the save",
                client.describe(parent.id()).isEmpty());
        Assert.assertNull(runway.load(JoinParent.class, parent.id()));
        Assert.assertNull(runway.load(JoinTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a caller-held {@link JoinParent} that
     * joined a deletion cannot re-create the deleted record through a later
     * save.
     * <p>
     * <strong>Start state:</strong> A saved {@link JoinParent} whose
     * {@link JoinDelete} field links to a saved {@link JoinTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener.</li>
     * <li>Mark only the target with {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the target ordered first so the join
     * deletion processes a loaded copy of the parent before the caller's
     * instance.</li>
     * <li>Modify the caller's parent instance and save it alone.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The later save repeats the deletion instead of
     * re-creating the record, so the parent holds no stored data and does not
     * load.
     */
    @Test
    public void testJoinDeletedRecordNotRecreatedByLaterSaveOfCallerInstance()
            throws Exception {
        CountDownLatch deleteLatch = new CountDownLatch(2);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        JoinTarget target = new JoinTarget();
        target.name = "Join Target";
        JoinParent parent = new JoinParent();
        parent.name = "Join Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        target.deleteOnSave();
        Assert.assertTrue(runway.save(target, parent));
        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));
        Assert.assertTrue(deletedRecords.contains(parent));

        parent.name = "Join Parent (Updated)";
        Assert.assertTrue(parent.save());

        Assert.assertTrue(
                "The later save must not re-create the deleted record",
                client.describe(parent.id()).isEmpty());
        Assert.assertNull(runway.load(JoinParent.class, parent.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link JoinDelete} pulls a record into
     * a deletion when the annotated field is an array.
     * <p>
     * <strong>Start state:</strong> A saved {@link ArrayJoinParent} whose array
     * holds one saved {@link JoinTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener.</li>
     * <li>Mark the target with {@link Record#deleteOnSave()}.</li>
     * <li>Save the target alone, so the join deletion must find the parent
     * through the array field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both the target
     * and the parent, and neither record loads afterwards.
     */
    @Test
    public void testJoinDeleteAppliesWhenAnnotatedFieldIsArray()
            throws Exception {
        CountDownLatch deleteLatch = new CountDownLatch(2);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        JoinTarget target = new JoinTarget();
        target.name = "Join Target";
        ArrayJoinParent parent = new ArrayJoinParent();
        parent.name = "Array Join Parent";
        parent.targets = new JoinTarget[] { target };
        Assert.assertTrue(runway.save(parent, target));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));
        Assert.assertTrue(deletedRecords.contains(target));
        Assert.assertTrue(deletedRecords.contains(parent));
        Assert.assertNull(runway.load(JoinTarget.class, target.id()));
        Assert.assertNull(runway.load(ArrayJoinParent.class, parent.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link JoinParent} with unsaved
     * changes cannot be re-created by its own staged writes when it joins a
     * deletion in the same save (GH-157).
     * <p>
     * <strong>Start state:</strong> A saved {@link JoinParent} whose
     * {@link JoinDelete} field links to a saved {@link JoinTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener.</li>
     * <li>Modify the parent's name and mark the target with
     * {@link Record#deleteOnSave()}.</li>
     * <li>Save both records in bulk, with the target ordered first so the join
     * deletion clears the parent before the parent's own modified instance
     * stages its writes.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete listener fires for both records and
     * neither record holds any stored data after the save.
     */
    @Test
    public void testJoinDeletedRecordNotResurrectedByModifiedInstanceInSameSave()
            throws Exception {
        CountDownLatch deleteLatch = new CountDownLatch(2);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        JoinTarget target = new JoinTarget();
        target.name = "Join Target";
        JoinParent parent = new JoinParent();
        parent.name = "Join Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        parent.name = "Join Parent (Updated)";
        target.deleteOnSave();
        Assert.assertTrue(runway.save(target, parent));

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(2, deletedRecords.size());
        Assert.assertTrue(deletedRecords.contains(target));
        Assert.assertTrue(deletedRecords.contains(parent));
        Assert.assertTrue("The join deleted record must not survive the save",
                client.describe(parent.id()).isEmpty());
        Assert.assertNull(runway.load(JoinParent.class, parent.id()));
        Assert.assertNull(runway.load(JoinTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a failed save restores the unsaved
     * changes of a caller-held {@link Record} even when the save also processed
     * an internally loaded copy of it for {@link CaptureDelete} cleanup.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} whose
     * {@link CaptureDelete} field links to a saved {@link CaptureTarget}, and a
     * saved {@link UniqueRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Modify the parent's name and mark the target with
     * {@link Record#deleteOnSave()}.</li>
     * <li>Save the target, the parent and a {@link UniqueRecord} that violates
     * its unique constraint, with the target ordered first so the cleanup copy
     * of the parent saves before the caller's instance.</li>
     * <li>After the save fails, save the parent alone.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The failed save does not consume the parent's
     * unsaved changes and does not touch its in-memory or stored reference to
     * the target; the follow-up save persists the updated name and completes
     * the deletion.
     */
    @Test
    public void testFailedSaveRestoresCallerInstanceProcessedAlongsideCopy()
            throws Exception {
        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        parent.target = target;
        UniqueRecord original = new UniqueRecord();
        original.token = "GH-157";
        Assert.assertTrue(runway.save(parent, target, original));

        parent.name = "Capture Parent (Updated)";
        target.deleteOnSave();
        UniqueRecord duplicate = new UniqueRecord();
        duplicate.token = "GH-157";
        Assert.assertFalse("The save must fail on the unique violation",
                runway.save(target, parent, duplicate));

        Assert.assertTrue("The failed save must not consume unsaved changes",
                parent.hasUnsavedChanges());
        Assert.assertSame(
                "The failed save must not remove the in-memory reference",
                target, parent.target);
        Assert.assertFalse(
                "The failed save must not remove the stored reference",
                client.select("target", parent.id()).isEmpty());

        Assert.assertTrue(runway.save(parent));
        CaptureParent loaded = runway.load(CaptureParent.class, parent.id());
        Assert.assertEquals("Capture Parent (Updated)", loaded.name);
        Assert.assertNull(loaded.target);
        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link CaptureDelete} cleanup works
     * for an immutable collection field when the record that holds it is part
     * of the same save as the deleted target.
     * <p>
     * <strong>Start state:</strong> A saved {@link CollectionCaptureParent}
     * whose immutable collection references two saved {@link CaptureTarget
     * CaptureTargets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener.</li>
     * <li>Mark one target with {@link Record#deleteOnSave()}.</li>
     * <li>Save the parent and both targets in bulk.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds, the delete listener fires
     * for the deleted target, and the parent's stored references contain only
     * the surviving target.
     */
    @Test
    public void testCaptureCleanupOfImmutableCollectionInSameSave()
            throws Exception {
        CountDownLatch deleteLatch = new CountDownLatch(1);
        Set<Record> deletedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        CaptureTarget kept = new CaptureTarget();
        kept.name = "Kept";
        CaptureTarget removed = new CaptureTarget();
        removed.name = "Removed";
        CollectionCaptureParent parent = new CollectionCaptureParent();
        parent.name = "Collection Parent";
        parent.targets = ImmutableList.of(kept, removed);
        Assert.assertTrue(runway.save(parent, kept, removed));

        removed.deleteOnSave();
        Assert.assertTrue(runway.save(parent, kept, removed));

        Assert.assertTrue("Delete listener was not called within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));
        Assert.assertTrue(deletedRecords.contains(removed));

        Set<Object> stored = client.select("targets", parent.id());
        Assert.assertEquals(1, stored.size());
        Assert.assertTrue(stored.contains(Link.to(kept.id())));
        Assert.assertNull(runway.load(CaptureTarget.class, removed.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a cascade deletion marks the caller's
     * in-save instance of the child, so the delete notification delivers it and
     * a later save cannot re-create the child.
     * <p>
     * <strong>Start state:</strong> A saved {@link CascadeParent} linked to a
     * saved {@link CascadeChild}, reloaded so the caller holds a child instance
     * distinct from the parent's linked copy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a delete listener that records each notified instance.</li>
     * <li>Load the child and the parent in separate calls.</li>
     * <li>Mark the parent with {@link Record#deleteOnSave()}.</li>
     * <li>Save the child and the parent in one call, with the child ordered
     * first.</li>
     * <li>Modify the caller's child instance and save it alone.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The delete notification for the child delivers
     * the caller's instance, and the later save repeats the deletion instead of
     * re-creating the child.
     */
    @Test
    public void testCallerInstanceAuthoritativeWhenCascadeChildAlsoInSameSave()
            throws Exception {
        CountDownLatch deleteLatch = new CountDownLatch(2);
        List<Record> deletedRecords = new CopyOnWriteArrayList<>();

        runway.close();
        runway = runwayBuilder().onDelete(record -> {
            deletedRecords.add(record);
            deleteLatch.countDown();
        }).build();

        CascadeChild child = new CascadeChild();
        child.name = "Cascade Child";
        CascadeParent parent = new CascadeParent();
        parent.name = "Cascade Parent";
        parent.child = child;
        Assert.assertTrue(runway.save(parent, child));

        CascadeChild c1 = runway.load(CascadeChild.class, child.id());
        CascadeParent p1 = runway.load(CascadeParent.class, parent.id());
        Assert.assertNotSame(c1, p1.child);

        p1.deleteOnSave();
        Assert.assertTrue(runway.save(c1, p1));

        Assert.assertTrue(
                "Delete listener did not fire for both records within timeout",
                deleteLatch.await(5, TimeUnit.SECONDS));
        Record notified = null;
        for (Record record : deletedRecords) {
            if(record.id() == c1.id()) {
                notified = record;
            }
        }
        Assert.assertSame(
                "The delete notification must deliver the caller's instance",
                c1, notified);

        c1.name = "Cascade Child (Updated)";
        Assert.assertTrue(c1.save());
        Assert.assertTrue("The later save must not re-create the deleted child",
                client.describe(c1.id()).isEmpty());
        Assert.assertNull(runway.load(CascadeChild.class, c1.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a record pulled into a deletion
     * through {@link CascadeDelete} is still deleted when the save that deletes
     * its parent is repeated after a failure.
     * <p>
     * <strong>Start state:</strong> A saved {@link CascadeParent} linked to a
     * saved {@link CascadeChild}, and a saved {@link UniqueRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Mark the parent with {@link Record#deleteOnSave()}.</li>
     * <li>Save the parent alongside a {@link UniqueRecord} that violates its
     * unique constraint, so the save fails after the cascade is staged.</li>
     * <li>Save the parent again, alone.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second save deletes both the parent and
     * the cascaded child, and neither record loads afterwards.
     */
    @Test
    public void testCascadeCompanionDeletedWhenDeleteIsRepeatedAfterFailedSave()
            throws Exception {
        CascadeChild child = new CascadeChild();
        child.name = "Cascade Child";
        CascadeParent parent = new CascadeParent();
        parent.name = "Cascade Parent";
        parent.child = child;
        UniqueRecord original = new UniqueRecord();
        original.token = "GH-65-cascade-repeat";
        Assert.assertTrue(runway.save(parent, child, original));

        parent.deleteOnSave();
        UniqueRecord duplicate = new UniqueRecord();
        duplicate.token = "GH-65-cascade-repeat";
        Assert.assertFalse("The save must fail on the unique violation",
                runway.save(parent, duplicate));

        Assert.assertTrue(runway.save(parent));

        Assert.assertNull(runway.load(CascadeParent.class, parent.id()));
        Assert.assertNull(runway.load(CascadeChild.class, child.id()));
        Assert.assertTrue("The cascaded child must not survive the deletion",
                client.describe(child.id()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a failed save clears the deletion mark
     * that {@link CascadeDelete} placed on a linked record, so a later save of
     * that record persists its data instead of deleting it.
     * <p>
     * <strong>Start state:</strong> A saved {@link CascadeParent} linked to a
     * saved {@link CascadeChild}, and a saved {@link UniqueRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Mark the parent with {@link Record#deleteOnSave()}.</li>
     * <li>Save the parent alongside a {@link UniqueRecord} that violates its
     * unique constraint, so the save fails after the cascade is staged.</li>
     * <li>Modify the child's name and save the child alone.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The child's save persists the updated name,
     * and the child still loads from the database.
     */
    @Test
    public void testFailedSaveDoesNotLeaveCascadeCompanionMarkedForDeletion()
            throws Exception {
        CascadeChild child = new CascadeChild();
        child.name = "Cascade Child";
        CascadeParent parent = new CascadeParent();
        parent.name = "Cascade Parent";
        parent.child = child;
        UniqueRecord original = new UniqueRecord();
        original.token = "GH-65-cascade-mark";
        Assert.assertTrue(runway.save(parent, child, original));

        parent.deleteOnSave();
        UniqueRecord duplicate = new UniqueRecord();
        duplicate.token = "GH-65-cascade-mark";
        Assert.assertFalse("The save must fail on the unique violation",
                runway.save(parent, duplicate));

        child.name = "Cascade Child (Updated)";
        Assert.assertTrue(child.save());

        CascadeChild loaded = runway.load(CascadeChild.class, child.id());
        Assert.assertNotNull("The child must survive its own save", loaded);
        Assert.assertEquals("Cascade Child (Updated)", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link CaptureDelete} cleanup succeeds
     * when the deleted record is the only element of a referencing record's
     * array field.
     * <p>
     * <strong>Start state:</strong> A saved {@link ArrayCaptureParent} whose
     * array holds one saved {@link CaptureTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Mark the target with {@link Record#deleteOnSave()}.</li>
     * <li>Save the target alone, so the cleanup loads a copy of the
     * parent.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds, the target no longer loads,
     * and the parent's stored references are empty.
     */
    @Test
    public void testCaptureCleanupSucceedsWhenArrayHoldsOnlyDeletedReference()
            throws Exception {
        CaptureTarget target = new CaptureTarget();
        target.name = "Only Target";
        ArrayCaptureParent parent = new ArrayCaptureParent();
        parent.name = "Array Parent";
        parent.targets = new CaptureTarget[] { target };
        Assert.assertTrue(runway.save(parent, target));

        target.deleteOnSave();
        Assert.assertTrue("The delete must not fail on the emptied array",
                target.save());

        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
        Assert.assertTrue(client.select("targets", parent.id()).isEmpty());
        ArrayCaptureParent loaded = runway.load(ArrayCaptureParent.class,
                parent.id());
        Assert.assertEquals("Array Parent", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that a committed save which deletes a
     * record removes a surviving {@link Record Record's} in-memory reference to
     * it, so a later save cannot re-create the stored link.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} whose
     * {@link CaptureDelete} field links to a saved {@link CaptureTarget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Modify the parent's name and mark the target with
     * {@link Record#deleteOnSave()}.</li>
     * <li>Save the parent and the target in one call.</li>
     * <li>Modify the parent's name again and save the parent alone.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> After the first save, the parent's in-memory
     * reference is {@code null} and the parent reports no unsaved changes.
     * After the second save, the parent's stored reference remains absent.
     */
    @Test
    public void testSurvivorInMemoryReferenceClearedWhenTargetDeletedInSameSave()
            throws Exception {
        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        parent.target = target;
        Assert.assertTrue(runway.save(parent, target));

        parent.name = "Capture Parent (Updated)";
        target.deleteOnSave();
        Assert.assertTrue(runway.save(parent, target));

        Assert.assertNull("The in-memory reference must be removed",
                parent.target);
        Assert.assertFalse(parent.hasUnsavedChanges());

        parent.name = "Capture Parent (Updated Again)";
        Assert.assertTrue(parent.save());

        Assert.assertTrue("The stored link must not be re-created",
                client.select("target", parent.id()).isEmpty());
        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that a committed save which deletes a
     * record removes it from a surviving {@link Record Record's} in-memory
     * immutable collection of {@link CaptureDelete} references.
     * <p>
     * <strong>Start state:</strong> A saved {@link CollectionCaptureParent}
     * whose immutable collection references two saved {@link CaptureTarget
     * CaptureTargets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Mark one target with {@link Record#deleteOnSave()}.</li>
     * <li>Save the parent and both targets in one call.</li>
     * <li>Modify the parent's name and save the parent alone.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> After the first save, the parent's in-memory
     * collection holds only the surviving target and the parent reports no
     * unsaved changes. After the second save, the stored references still hold
     * only the surviving target.
     */
    @Test
    public void testSurvivorInMemoryCollectionCleanedWhenTargetDeletedInSameSave()
            throws Exception {
        CaptureTarget kept = new CaptureTarget();
        kept.name = "Kept";
        CaptureTarget removed = new CaptureTarget();
        removed.name = "Removed";
        CollectionCaptureParent parent = new CollectionCaptureParent();
        parent.name = "Collection Parent";
        parent.targets = ImmutableList.of(kept, removed);
        Assert.assertTrue(runway.save(parent, kept, removed));

        removed.deleteOnSave();
        Assert.assertTrue(runway.save(parent, kept, removed));

        Assert.assertEquals("The in-memory reference must be removed",
                ImmutableList.of(kept), ImmutableList.copyOf(parent.targets));
        Assert.assertFalse(parent.hasUnsavedChanges());

        parent.name = "Collection Parent (Updated)";
        Assert.assertTrue(parent.save());

        Set<Object> stored = client.select("targets", parent.id());
        Assert.assertEquals(1, stored.size());
        Assert.assertTrue(stored.contains(Link.to(kept.id())));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link CaptureDelete} cleanup removes
     * a stored link to the deleted record even when the referencing
     * {@link Record Record's} in-memory instance in the same save never
     * observed the link.
     * <p>
     * <strong>Start state:</strong> A saved {@link CaptureParent} with no
     * reference to a saved {@link CaptureTarget}, and a stored link from the
     * parent to the target that another client added after the parent was
     * created in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Add the stored link through a raw connection.</li>
     * <li>Mark the target with {@link Record#deleteOnSave()}.</li>
     * <li>Save the parent and the target in one call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The target no longer loads and the parent's
     * stored reference is empty.
     */
    @Test
    public void testCaptureCleanupRemovesStoredLinkWhenInMemoryCopyIsStale()
            throws Exception {
        CaptureTarget target = new CaptureTarget();
        target.name = "Capture Target";
        CaptureParent parent = new CaptureParent();
        parent.name = "Capture Parent";
        Assert.assertTrue(runway.save(parent, target));

        client.link("target", target.id(), parent.id());

        target.deleteOnSave();
        Assert.assertTrue(runway.save(parent, target));

        Assert.assertTrue("The stored link must not survive the deletion",
                client.select("target", parent.id()).isEmpty());
        Assert.assertNull(runway.load(CaptureTarget.class, target.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link CaptureDelete} cleanup keeps a
     * stored collection member that the referencing {@link Record Record's}
     * in-memory instance in the same save never observed.
     * <p>
     * <strong>Start state:</strong> A saved {@link CollectionCaptureParent}
     * whose collection references two saved {@link CaptureTarget
     * CaptureTargets}, plus a stored link to a third saved target that another
     * client added after the parent was created in memory.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Add the third stored link through a raw connection.</li>
     * <li>Mark one of the two original targets with
     * {@link Record#deleteOnSave()}.</li>
     * <li>Save the parent and the two original targets in one call.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored references hold the surviving
     * original target and the third target; only the deleted {@link Record
     * Record's} link is removed.
     */
    @Test
    public void testCaptureCleanupPreservesStoredMemberUnseenByInMemoryCopy()
            throws Exception {
        CaptureTarget kept = new CaptureTarget();
        kept.name = "Kept";
        CaptureTarget removed = new CaptureTarget();
        removed.name = "Removed";
        CaptureTarget extra = new CaptureTarget();
        extra.name = "Extra";
        CollectionCaptureParent parent = new CollectionCaptureParent();
        parent.name = "Collection Parent";
        parent.targets = ImmutableList.of(kept, removed);
        Assert.assertTrue(runway.save(parent, kept, removed, extra));

        client.link("targets", extra.id(), parent.id());

        removed.deleteOnSave();
        Assert.assertTrue(runway.save(parent, kept, removed));

        Set<Object> stored = client.select("targets", parent.id());
        Assert.assertEquals(2, stored.size());
        Assert.assertTrue(stored.contains(Link.to(kept.id())));
        Assert.assertTrue("The unseen member must survive the cleanup",
                stored.contains(Link.to(extra.id())));
        Assert.assertNull(runway.load(CaptureTarget.class, removed.id()));
    }

    /**
     * A test {@link Record} whose lifecycle events are tracked by listeners.
     *
     * @author Jeff Nelson
     */
    public static class TrackedRecord extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;
    }

    /**
     * A test {@link Record} of a different type, used to verify typed listener
     * filtering.
     *
     * @author Jeff Nelson
     */
    public static class OtherRecord extends Record {

        /**
         * A label that identifies the record in tests.
         */
        public String label;
    }

    /**
     * A test {@link Record} whose linked {@link CascadeChild} is deleted with
     * it via {@link CascadeDelete}.
     *
     * @author Jeff Nelson
     */
    public static class CascadeParent extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;

        /**
         * The child that is deleted alongside this record.
         */
        @CascadeDelete
        public CascadeChild child;
    }

    /**
     * A test {@link Record} that is deleted when its owning
     * {@link CascadeParent} is deleted.
     *
     * @author Jeff Nelson
     */
    public static class CascadeChild extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;
    }

    /**
     * A test {@link Record} that joins the deletion of its linked
     * {@link JoinTarget} via {@link JoinDelete}.
     *
     * @author Jeff Nelson
     */
    public static class JoinParent extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;

        /**
         * The target whose deletion this record joins.
         */
        @JoinDelete
        public JoinTarget target;
    }

    /**
     * A test {@link Record} whose deletion pulls in linked {@link JoinParent
     * JoinParents}.
     *
     * @author Jeff Nelson
     */
    public static class JoinTarget extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;
    }

    /**
     * A test {@link Record} whose array of {@link JoinTarget JoinTargets} pulls
     * it into the deletion of any of them via {@link JoinDelete}.
     *
     * @author Jeff Nelson
     */
    public static class ArrayJoinParent extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;

        /**
         * The targets whose deletion this record joins.
         */
        @JoinDelete
        public JoinTarget[] targets;
    }

    /**
     * A test {@link Record} whose reference to a {@link CaptureTarget} is
     * removed via {@link CaptureDelete} when the target is deleted.
     *
     * @author Jeff Nelson
     */
    public static class CaptureParent extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;

        /**
         * The reference that is nullified when the target is deleted.
         */
        @CaptureDelete
        public CaptureTarget target;
    }

    /**
     * A test {@link Record} whose deletion triggers {@link CaptureDelete}
     * cleanup in referencing {@link CaptureParent CaptureParents}.
     *
     * @author Jeff Nelson
     */
    public static class CaptureTarget extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;
    }

    /**
     * A test {@link Record} whose immutable collection of {@link CaptureTarget
     * CaptureTargets} is cleaned when a target is deleted.
     *
     * @author Jeff Nelson
     */
    public static class CollectionCaptureParent extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;

        /**
         * The references that are removed when their targets are deleted.
         */
        @CaptureDelete
        public List<CaptureTarget> targets;
    }

    /**
     * A test {@link Record} whose array of {@link CaptureTarget CaptureTargets}
     * is cleaned when a target is deleted.
     *
     * @author Jeff Nelson
     */
    public static class ArrayCaptureParent extends Record {

        /**
         * A name that identifies the record in tests.
         */
        public String name;

        /**
         * The references that are removed when their targets are deleted.
         */
        @CaptureDelete
        public CaptureTarget[] targets;
    }

    /**
     * A test {@link Record} with a {@link Unique} field, used to force a save
     * to fail.
     *
     * @author Jeff Nelson
     */
    public static class UniqueRecord extends Record {

        /**
         * A value that must be unique across all {@link UniqueRecord
         * UniqueRecords}.
         */
        @Unique
        public String token;
    }
}
