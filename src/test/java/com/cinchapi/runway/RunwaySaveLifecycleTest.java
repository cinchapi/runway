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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.google.common.collect.Sets;

public class RunwaySaveLifecycleTest extends RunwayBaseClientServerTest {

    @Test
    public void testMultipleRecordSaveWithNoListener() throws Exception {
        runway.close();
        runway = Runway.builder().port(server.getClientPort()).build();

        // Create and save multiple records
        Player[] players = new Player[5];
        for (int i = 0; i < players.length; i++) {
            players[i] = new Player("Player " + i, i * 10);
        }

        boolean saved = runway.save(players);

        // Verify the save was successful
        Assert.assertTrue(
                "Multiple records should save successfully without a listener",
                saved);

        // Verify all records were saved
        for (Player player : players) {
            Player loaded = runway.load(Player.class, player.id());
            Assert.assertEquals(player.name, loaded.name);
            Assert.assertEquals(player.score, loaded.score);
        }
    }

    @Test
    public void testNoSaveListenerDoesNotCreateExecutor() throws Exception {
        runway.close();
        runway = Runway.builder().port(server.getClientPort()).build();

        // Verify the saveNotificationExecutor field is null
        Object executor = Reflection.get("saveNotificationExecutor", runway);
        Assert.assertNull(
                "Save notification executor should be null when no listener is provided",
                executor);

        // Save a record to ensure it works without a listener
        Player player = new Player("Test Player", 30);
        boolean saved = player.save();

        Assert.assertTrue("Record should save successfully without a listener",
                saved);
    }

    @Test
    public void testOverrideSaveForSingleRecord() {
        // Create a record that overrides save
        OverrideSaveRecord record = new OverrideSaveRecord();
        record.name = "Override Test";
        record.overrideResult = true;

        // Save the record
        boolean saved = record.save();

        // Verify the save result matches our override
        Assert.assertTrue("Save should return the overridden result", saved);
        Assert.assertTrue("overrideSave should have been called",
                record.overrideCalled);

        // Verify the record was not actually persisted
        Assert.assertNull("Record should not exist in the database",
                runway.load(OverrideSaveRecord.class, record.id()));
    }

    @Test
    public void testOverrideSaveInBulkSave() {
        // Create multiple records, some with overridden save
        PreSaveHookRecord record1 = new PreSaveHookRecord();
        OverrideSaveRecord record2 = new OverrideSaveRecord();
        PreSaveHookRecord record3 = new PreSaveHookRecord();

        record1.name = "Normal Record 1";
        record2.name = "Override Record";
        record2.overrideResult = true;
        record3.name = "Normal Record 2";

        // Save all records in a bulk operation
        boolean saved = runway.save(record1, record2, record3);

        // Verify the save was successful
        Assert.assertTrue("Bulk save should succeed", saved);

        // Verify overrideSave was called
        Assert.assertTrue("overrideSave should have been called",
                record2.overrideCalled);

        // Verify normal records were persisted
        PreSaveHookRecord loaded1 = runway.load(PreSaveHookRecord.class,
                record1.id());
        Assert.assertEquals("Normal record 1 should be persisted",
                "Normal Record 1 (Modified)", loaded1.name);

        PreSaveHookRecord loaded3 = runway.load(PreSaveHookRecord.class,
                record3.id());
        Assert.assertEquals("Normal record 2 should be persisted",
                "Normal Record 2 (Modified)", loaded3.name);

        // Verify the overridden record was not persisted
        Assert.assertNull("Overridden record should not exist in the database",
                runway.load(OverrideSaveRecord.class, record2.id()));
    }

    @Test
    public void testOverrideSaveReturnsFalse() {
        // Create a record that overrides save with a false result
        OverrideSaveRecord record = new OverrideSaveRecord();
        record.name = "Override Test";
        record.overrideResult = false;

        // Save the record
        boolean saved = record.save();

        // Verify the save result matches our override
        Assert.assertFalse("Save should return the overridden result", saved);
        Assert.assertTrue("overrideSave should have been called",
                record.overrideCalled);

        // Verify the record was not persisted
        Assert.assertNull("Record should not exist in the database",
                runway.load(OverrideSaveRecord.class, record.id()));
    }

    @Test
    public void testPreSaveHookCalledBeforeSave() throws Exception {
        // Create a record with a preSave implementation
        PreSaveHookRecord record = new PreSaveHookRecord();
        record.name = "Original Name";

        // Save the record
        boolean saved = record.save();

        // Verify the save was successful and preSave was called
        Assert.assertTrue("Record should save successfully", saved);
        Assert.assertTrue("preSave hook should have been called",
                record.preSaveCalled);
        Assert.assertEquals("Field should be modified by preSave",
                "Original Name (Modified)", record.name);

        // Load the record to verify the changes were persisted
        PreSaveHookRecord loaded = runway.load(PreSaveHookRecord.class,
                record.id());
        Assert.assertEquals("Modified value should be persisted",
                "Original Name (Modified)", loaded.name);
    }

    @Test
    public void testPreSaveHookCalledBeforeSaveListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean preSaveCalledBeforeListener = new AtomicBoolean(false);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    if(record instanceof PreSaveOrderRecord) {
                        PreSaveOrderRecord orderRecord = (PreSaveOrderRecord) record;
                        preSaveCalledBeforeListener
                                .set(orderRecord.preSaveCalled);
                        latch.countDown();
                    }
                }).build();

        // Create a record that tracks when preSave is called
        PreSaveOrderRecord record = new PreSaveOrderRecord();
        record.name = "Test Record";

        // Save the record
        boolean saved = record.save();

        // Verify the save was successful
        Assert.assertTrue("Record should save successfully", saved);

        // Wait for the save listener to be called
        Assert.assertTrue("Save listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        // Verify preSave was called before the save listener
        Assert.assertTrue("preSave should be called before save listener",
                preSaveCalledBeforeListener.get());
    }

    @Test
    public void testPreSaveHookCalledInTransaction() throws Exception {
        // Create multiple records with preSave implementations
        PreSaveHookRecord record1 = new PreSaveHookRecord();
        PreSaveHookRecord record2 = new PreSaveHookRecord();
        record1.name = "Record 1";
        record2.name = "Record 2";

        // Save the records in a single transaction
        boolean saved = runway.save(record1, record2);

        // Verify the save was successful and preSave was called for both
        // records
        Assert.assertTrue("Records should save successfully", saved);
        Assert.assertTrue("preSave hook should have been called for record1",
                record1.preSaveCalled);
        Assert.assertTrue("preSave hook should have been called for record2",
                record2.preSaveCalled);
        Assert.assertEquals("Field should be modified by preSave",
                "Record 1 (Modified)", record1.name);
        Assert.assertEquals("Field should be modified by preSave",
                "Record 2 (Modified)", record2.name);
    }

    @Test
    public void testPreSaveHookCalledWithBulkSave() throws Exception {
        // Create multiple records with preSave implementations
        int recordCount = 5;
        PreSaveHookRecord[] records = new PreSaveHookRecord[recordCount];

        for (int i = 0; i < recordCount; i++) {
            records[i] = new PreSaveHookRecord();
            records[i].name = "Bulk Record " + i;
        }

        // Save the records using bulk save
        boolean saved = runway.save(records);

        // Verify the save was successful
        Assert.assertTrue("Bulk save should succeed", saved);

        // Verify preSave was called for all records
        for (int i = 0; i < recordCount; i++) {
            Assert.assertTrue(
                    "preSave hook should have been called for record " + i,
                    records[i].preSaveCalled);
            Assert.assertEquals("Field should be modified by preSave",
                    "Bulk Record " + i + " (Modified)", records[i].name);

            // Load the record to verify the changes were persisted
            PreSaveHookRecord loaded = runway.load(PreSaveHookRecord.class,
                    records[i].id());
            Assert.assertEquals("Modified value should be persisted",
                    "Bulk Record " + i + " (Modified)", loaded.name);
        }
    }

    @Test
    public void testPreSaveHookExceptionAbortsBulkSave() throws Exception {
        // Create multiple records, with one that will throw an exception in
        // preSave
        PreSaveHookRecord record1 = new PreSaveHookRecord();
        PreSaveHookRecord record2 = new PreSaveHookRecord();
        PreSaveExceptionRecord exceptionRecord = new PreSaveExceptionRecord();
        PreSaveHookRecord record3 = new PreSaveHookRecord();

        record1.name = "Bulk Record 1";
        record2.name = "Bulk Record 2";
        exceptionRecord.name = "Exception Record";
        record3.name = "Bulk Record 3";

        // Attempt to save all records in a bulk operation
        boolean saved = runway.save(record1, record2, exceptionRecord, record3);

        // Verify the save failed
        Assert.assertFalse(
                "Bulk save should fail when a record's preSave throws an exception",
                saved);

        // Verify no records were persisted
        for (Record record : new Record[] { record1, record2, exceptionRecord,
                record3 }) {
            Assert.assertNull(
                    "Record with ID " + record.id()
                            + " should not exist in the database",
                    runway.load(record.getClass(), record.id()));
        }
    }

    @Test
    public void testPreSaveHookExceptionAbortsSave() throws Exception {
        // Create a record with a preSave implementation that throws an
        // exception
        PreSaveExceptionRecord record = new PreSaveExceptionRecord();
        record.name = "Test Record";

        // Attempt to save the record
        boolean saved = record.save();

        // Verify the save failed
        Assert.assertFalse("Save should fail when preSave throws an exception",
                saved);

        // Verify the record was not persisted
        Assert.assertNull("Record should not exist in the database",
                runway.load(PreSaveExceptionRecord.class, record.id()));
    }

    @Test
    public void testPreSaveHookExceptionInTransactionAbortsAllSaves()
            throws Exception {
        // Create multiple records, one with a preSave that throws an exception
        PreSaveHookRecord record1 = new PreSaveHookRecord();
        PreSaveExceptionRecord record2 = new PreSaveExceptionRecord();
        PreSaveHookRecord record3 = new PreSaveHookRecord();

        record1.name = "Record 1";
        record2.name = "Record 2";
        record3.name = "Record 3";

        // Attempt to save all records in a single transaction
        boolean saved = runway.save(record1, record2, record3);

        // Verify the transaction failed
        Assert.assertFalse(
                "Transaction should fail when preSave throws an exception",
                saved);

        // Verify no records were persisted
        Assert.assertNull("Record1 should not exist in the database",
                runway.load(PreSaveHookRecord.class, record1.id()));
        Assert.assertNull("Record2 should not exist in the database",
                runway.load(PreSaveExceptionRecord.class, record2.id()));
        Assert.assertNull("Record3 should not exist in the database",
                runway.load(PreSaveHookRecord.class, record3.id()));
    }

    @Test
    public void testPreSaveHookModifiesFieldsInBulkSave() throws Exception {
        // Create a record class that modifies multiple fields in preSave
        ComplexPreSaveRecord record1 = new ComplexPreSaveRecord();
        record1.name = "Complex Record 1";
        record1.count = 10;

        ComplexPreSaveRecord record2 = new ComplexPreSaveRecord();
        record2.name = "Complex Record 2";
        record2.count = 20;

        // Save the records using bulk save
        boolean saved = runway.save(record1, record2);

        // Verify the save was successful
        Assert.assertTrue("Bulk save should succeed", saved);

        // Verify preSave was called and modified multiple fields
        Assert.assertTrue("preSave hook should have been called for record1",
                record1.preSaveCalled);
        Assert.assertEquals("Name field should be modified by preSave",
                "Complex Record 1 (Modified)", record1.name);
        Assert.assertEquals("Count field should be incremented by preSave", 11,
                record1.count);
        Assert.assertNotNull("Timestamp field should be set by preSave",
                record1.timestamp);

        Assert.assertTrue("preSave hook should have been called for record2",
                record2.preSaveCalled);
        Assert.assertEquals("Name field should be modified by preSave",
                "Complex Record 2 (Modified)", record2.name);
        Assert.assertEquals("Count field should be incremented by preSave", 21,
                record2.count);
        Assert.assertNotNull("Timestamp field should be set by preSave",
                record2.timestamp);

        // Load the records to verify the changes were persisted
        ComplexPreSaveRecord loaded1 = runway.load(ComplexPreSaveRecord.class,
                record1.id());
        Assert.assertEquals("Modified name should be persisted",
                "Complex Record 1 (Modified)", loaded1.name);
        Assert.assertEquals("Modified count should be persisted", 11,
                loaded1.count);
        Assert.assertNotNull("Timestamp should be persisted",
                loaded1.timestamp);

        ComplexPreSaveRecord loaded2 = runway.load(ComplexPreSaveRecord.class,
                record2.id());
        Assert.assertEquals("Modified name should be persisted",
                "Complex Record 2 (Modified)", loaded2.name);
        Assert.assertEquals("Modified count should be persisted", 21,
                loaded2.count);
        Assert.assertNotNull("Timestamp should be persisted",
                loaded2.timestamp);
    }

    @Test
    public void testQueueSaveNotificationWithNoListenerIsNoOp()
            throws Exception {
        runway.close();
        runway = Runway.builder().port(server.getClientPort()).build();

        // Verify the saveNotificationQueue field is null
        Object queue = Reflection.get("saveNotificationQueue", runway);
        Assert.assertNull(
                "Save notification queue should be null when no listener is provided",
                queue);

        // Call queueSaveNotification directly to ensure it doesn't throw an
        // exception
        Player player = new Player("Test Player", 30);
        runway.enqueueSaveNotification(player);

        // If we got here without an exception, the test passes
        Assert.assertTrue(true);
    }

    @Test
    public void testSaveListenerCalledForBulkSave() throws Exception {
        int recordCount = 5;
        CountDownLatch latch = new CountDownLatch(recordCount);
        Set<Record> savedRecords = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                }).build();

        // Create multiple records with preSave implementations
        PreSaveHookRecord[] records = new PreSaveHookRecord[recordCount];

        for (int i = 0; i < recordCount; i++) {
            records[i] = new PreSaveHookRecord();
            records[i].name = "Listener Bulk Record " + i;
        }

        // Save the records using bulk save
        boolean saved = runway.save(records);

        // Verify the save was successful
        Assert.assertTrue("Bulk save should succeed", saved);

        // Wait for all save listeners to be called
        Assert.assertTrue("Not all save listeners were called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        // Verify all records were processed by the save listener
        Assert.assertEquals(recordCount, savedRecords.size());
        for (PreSaveHookRecord record : records) {
            Assert.assertTrue(
                    "Save listener should have processed record " + record.id(),
                    savedRecords.contains(record));
            Assert.assertTrue(
                    "preSave hook should have been called before save listener",
                    record.preSaveCalled);
            Assert.assertTrue("Field should be modified by preSave",
                    record.name.endsWith("(Modified)"));
        }
    }

    @Test
    public void testSaveListenerCalledForUpdates() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger callCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                }).build();

        // Create and save a record
        Player player = new Player("Initial Name", 25);
        player.save();

        // Update and save the record again
        player.name = "Updated Name";
        player.save();

        // Wait for both save listeners to be called
        Assert.assertTrue("Save listener was not called for both operations",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(2, callCount.get());
    }

    @Test
    public void testSaveListenerCalledOnMultipleRecordSave() throws Exception {
        int recordCount = 5;
        CountDownLatch latch = new CountDownLatch(recordCount);
        Set<Record> savedRecords = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                }).build();

        Player[] players = new Player[recordCount];
        for (int i = 0; i < recordCount; i++) {
            players[i] = new Player("Player " + i, i * 10);
        }

        runway.save(players);

        // Wait for all save listeners to be called
        Assert.assertTrue("Not all save listeners were called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(recordCount, savedRecords.size());
        for (Player player : players) {
            Assert.assertTrue(savedRecords.contains(player));
        }
    }

    @Test
    public void testSaveListenerCalledOnSingleRecordSave() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> savedRecords = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                }).build();

        Player player = new Player("Jeff Nelson", 42);
        player.save();

        // Wait for the save listener to be called
        Assert.assertTrue("Save listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, savedRecords.size());
        Assert.assertTrue(savedRecords.contains(player));
    }

    @Test
    public void testSaveListenerExceptionDoesNotAffectSave() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<String> processedNames = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    if(record instanceof Player) {
                        Player player = (Player) record;
                        processedNames.add(player.name);

                        // Throw an exception to test error handling
                        if("ThrowError".equals(player.name)) {
                            throw new RuntimeException("Test exception");
                        }

                        if("LastPlayer".equals(player.name)) {
                            latch.countDown();
                        }
                    }
                }).build();

        // Create and save records
        Player player1 = new Player("ThrowError", 10);
        Player player2 = new Player("LastPlayer", 20);

        player1.save();
        player2.save();

        // Wait for the last record to be processed
        Assert.assertTrue("Save listener did not process all records",
                latch.await(5, TimeUnit.SECONDS));

        // Verify both records were processed despite the exception
        Assert.assertTrue(processedNames.contains("ThrowError"));
        Assert.assertTrue(processedNames.contains("LastPlayer"));
    }

    @Test
    public void testSaveListenerNotCalledOnFailedSave() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                }).build();

        // Create a record with a unique field
        UniqueFieldRecord record1 = new UniqueFieldRecord();
        record1.uniqueField = "unique";
        record1.save();

        // Try to save another record with the same unique field (should fail)
        UniqueFieldRecord record2 = new UniqueFieldRecord();
        record2.uniqueField = "unique";
        boolean saved = record2.save();

        Assert.assertFalse("Save should have failed due to unique constraint",
                saved);

        // Give some time for any potential async processing
        Thread.sleep(1000);

        // Verify the save listener was only called once (for the first
        // successful save)
        Assert.assertEquals(1, callCount.get());
    }

    @Test
    public void testSaveListenerNotCalledWhenPreSaveThrowsException()
            throws Exception {
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    listenerCalled.set(true);
                }).build();

        // Create a record with a preSave implementation that throws an
        // exception
        PreSaveExceptionRecord record = new PreSaveExceptionRecord();
        record.name = "Test Record";

        // Attempt to save the record
        boolean saved = record.save();

        // Verify the save failed
        Assert.assertFalse("Save should fail when preSave throws an exception",
                saved);

        // Give some time for any potential async processing
        Thread.sleep(1000);

        // Verify the save listener was not called
        Assert.assertFalse("Save listener should not be called when save fails",
                listenerCalled.get());
    }

    @Test
    public void testSwitchingBetweenListenerAndNoListener() throws Exception {
        // Start with a listener
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                }).build();

        // Save a record with the listener
        Player player1 = new Player("Player with listener", 100);
        player1.save();

        // Wait for the listener to be called
        Assert.assertTrue("Save listener was not called",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, callCount.get());

        // Now switch to no listener
        runway.close();
        runway = Runway.builder().port(server.getClientPort()).build();

        // Save a record without a listener
        Player player2 = new Player("Player without listener", 200);
        player2.save();

        // Give some time for any potential async processing
        Thread.sleep(1000);

        // Verify the count hasn't changed
        Assert.assertEquals("Listener should not be called after being removed",
                1, callCount.get());

        // Verify both records were saved correctly
        Player loaded1 = runway.load(Player.class, player1.id());
        Player loaded2 = runway.load(Player.class, player2.id());

        Assert.assertEquals(player1.name, loaded1.name);
        Assert.assertEquals(player1.score, loaded1.score);
        Assert.assertEquals(player2.name, loaded2.name);
        Assert.assertEquals(player2.score, loaded2.score);
    }

    @Test
    public void testTypedSaveListenerOnlyFiresForMatchingType()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> playerSaves = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(Player.class, player -> {
                    playerSaves.add(player);
                    latch.countDown();
                }).build();

        // Save a non-Player record first
        PreSaveHookRecord hook = new PreSaveHookRecord();
        hook.name = "Not a Player";
        hook.save();

        // Save a Player record
        Player player = new Player("Typed Player", 99);
        player.save();

        Assert.assertTrue("Typed listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, playerSaves.size());
        Assert.assertTrue(playerSaves.contains(player));
    }

    @Test
    public void testMultipleTypedListeners() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Set<Record> playerSaves = Sets.newConcurrentHashSet();
        Set<Record> hookSaves = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(Player.class, player -> {
                    playerSaves.add(player);
                    latch.countDown();
                }).onSave(PreSaveHookRecord.class, record -> {
                    hookSaves.add(record);
                    latch.countDown();
                }).build();

        Player player = new Player("Typed Player", 50);
        player.save();

        PreSaveHookRecord hook = new PreSaveHookRecord();
        hook.name = "Hook Record";
        hook.save();

        Assert.assertTrue("Not all listeners were called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, playerSaves.size());
        Assert.assertTrue(playerSaves.contains(player));
        Assert.assertEquals(1, hookSaves.size());
        Assert.assertTrue(hookSaves.contains(hook));
    }

    @Test
    public void testCompositionMultipleListenersForSameType() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger firstCount = new AtomicInteger(0);
        AtomicInteger secondCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(Record.class, record -> {
                    firstCount.incrementAndGet();
                    latch.countDown();
                }).onSave(Record.class, record -> {
                    secondCount.incrementAndGet();
                    latch.countDown();
                }).build();

        Player player = new Player("Composed", 10);
        player.save();

        Assert.assertTrue("Not all listeners were called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, firstCount.get());
        Assert.assertEquals(1, secondCount.get());
    }

    @Test
    public void testMixedTypedAndUntypedListeners() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Set<Record> typedSaves = Sets.newConcurrentHashSet();
        Set<Record> untypedSaves = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(Player.class, player -> {
                    typedSaves.add(player);
                    latch.countDown();
                }).onSave(record -> {
                    untypedSaves.add(record);
                    latch.countDown();
                }).build();

        Player player = new Player("Mixed", 77);
        player.save();

        Assert.assertTrue("Not all listeners were called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        // Both should fire for a Player save
        Assert.assertEquals(1, typedSaves.size());
        Assert.assertTrue(typedSaves.contains(player));
        Assert.assertEquals(1, untypedSaves.size());
        Assert.assertTrue(untypedSaves.contains(player));
    }

    @Test
    public void testListenerErrorIsolation() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger secondCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(Record.class, record -> {
                    throw new RuntimeException(
                            "Intentional exception from first listener");
                }).onSave(Record.class, record -> {
                    secondCount.incrementAndGet();
                    latch.countDown();
                }).build();

        Player player = new Player("Error Isolation", 33);
        player.save();

        Assert.assertTrue(
                "Second listener was not called despite first throwing",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(
                "Second listener should still fire after first throws", 1,
                secondCount.get());
    }

    @Test
    public void testOnSaveAfterBuildWithNoBuilderListeners() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> savedRecords = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort()).build();

        // Register a listener after build
        runway.onSave(record -> {
            savedRecords.add(record);
            latch.countDown();
        });

        Player player = new Player("Post-Build Listener", 42);
        player.save();

        Assert.assertTrue("Post-build listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, savedRecords.size());
        Assert.assertTrue(savedRecords.contains(player));
    }

    @Test
    public void testOnSaveAfterBuildChainsWithBuilderListeners()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger builderCount = new AtomicInteger(0);
        AtomicInteger postBuildCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    builderCount.incrementAndGet();
                    latch.countDown();
                }).build();

        // Register an additional listener after build
        runway.onSave(record -> {
            postBuildCount.incrementAndGet();
            latch.countDown();
        });

        Player player = new Player("Chained Listener", 99);
        player.save();

        Assert.assertTrue("Not all listeners were called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals("Builder listener should have fired", 1,
                builderCount.get());
        Assert.assertEquals("Post-build listener should have fired", 1,
                postBuildCount.get());
    }

    @Test
    public void testTypedOnSaveAfterBuild() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> playerSaves = Sets.newConcurrentHashSet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort()).build();

        // Register a typed listener after build
        runway.onSave(Player.class, player -> {
            playerSaves.add(player);
            latch.countDown();
        });

        // Save a non-Player record first
        PreSaveHookRecord hook = new PreSaveHookRecord();
        hook.name = "Not a Player";
        hook.save();

        // Save a Player record
        Player player = new Player("Typed Post-Build", 88);
        player.save();

        Assert.assertTrue(
                "Typed post-build listener was not called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, playerSaves.size());
        Assert.assertTrue(playerSaves.contains(player));
    }

    @Test
    public void testMultiplePostBuildOnSave() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger firstCount = new AtomicInteger(0);
        AtomicInteger secondCount = new AtomicInteger(0);
        AtomicInteger thirdCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort()).build();

        runway.onSave(record -> {
            firstCount.incrementAndGet();
            latch.countDown();
        });
        runway.onSave(record -> {
            secondCount.incrementAndGet();
            latch.countDown();
        });
        runway.onSave(record -> {
            thirdCount.incrementAndGet();
            latch.countDown();
        });

        Player player = new Player("Multiple Post-Build", 55);
        player.save();

        Assert.assertTrue(
                "Not all post-build listeners were called within timeout",
                latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, firstCount.get());
        Assert.assertEquals(1, secondCount.get());
        Assert.assertEquals(1, thirdCount.get());
    }

    /**
     * <strong>Goal:</strong> Verify that saving a {@link Record} with a linked
     * {@link Record} fires save notifications for both the parent and the
     * child.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link ParentRecord} with
     * a linked {@link ChildRecord}, neither previously saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener that tracks notified {@link Record
     * Records}.</li>
     * <li>Create a {@link ParentRecord} with a linked {@link ChildRecord}.</li>
     * <li>Call {@code parent.save()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save listener fires for both the parent
     * and child {@link Record Records}.
     */
    @Test
    public void testSaveListenerFiredForLinkedRecordViaSave() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Set<Record> savedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                }).build();

        ChildRecord child = new ChildRecord();
        child.label = "Child1";
        ParentRecord parent = new ParentRecord();
        parent.name = "Parent1";
        parent.child = child;

        boolean saved = parent.save();

        Assert.assertTrue(saved);
        Assert.assertTrue("Save listener should fire for both records",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(2, savedRecords.size());
        Assert.assertTrue(savedRecords.contains(parent));
        Assert.assertTrue(savedRecords.contains(child));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(Record...)} with a
     * single {@link Record} that has a linked {@link Record} fires save
     * notifications for both.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link ParentRecord} with
     * a linked {@link ChildRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener.</li>
     * <li>Create a {@link ParentRecord} with a linked {@link ChildRecord}.</li>
     * <li>Call {@code runway.save(parent)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save listener fires for both the parent
     * and child {@link Record Records}.
     */
    @Test
    public void testSaveListenerFiredForLinkedRecordViaRunwaySave()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Set<Record> savedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                }).build();

        ChildRecord child = new ChildRecord();
        child.label = "Child1";
        ParentRecord parent = new ParentRecord();
        parent.name = "Parent1";
        parent.child = child;

        boolean saved = runway.save(parent);

        Assert.assertTrue(saved);
        Assert.assertTrue("Save listener should fire for both records",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(2, savedRecords.size());
        Assert.assertTrue(savedRecords.contains(parent));
        Assert.assertTrue(savedRecords.contains(child));
    }

    /**
     * <strong>Goal:</strong> Verify that bulk {@link Runway#save(Record...)}
     * fires save notifications for all {@link Record Records}, including linked
     * children.
     * <p>
     * <strong>Start state:</strong> Two freshly created {@link ParentRecord
     * ParentRecords}, each with a linked {@link ChildRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener.</li>
     * <li>Create two parent-child pairs.</li>
     * <li>Call {@code runway.save(parent1, parent2)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save listener fires for all four
     * {@link Record Records}.
     */
    @Test
    public void testSaveListenerFiredForLinkedRecordsViaBulkSave()
            throws Exception {
        CountDownLatch latch = new CountDownLatch(4);
        Set<Record> savedRecords = ConcurrentHashMap.newKeySet();

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                }).build();

        ChildRecord child1 = new ChildRecord();
        child1.label = "Child1";
        ParentRecord parent1 = new ParentRecord();
        parent1.name = "Parent1";
        parent1.child = child1;

        ChildRecord child2 = new ChildRecord();
        child2.label = "Child2";
        ParentRecord parent2 = new ParentRecord();
        parent2.name = "Parent2";
        parent2.child = child2;

        boolean saved = runway.save(parent1, parent2);

        Assert.assertTrue(saved);
        Assert.assertTrue("Save listener should fire for all four records",
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(4, savedRecords.size());
        Assert.assertTrue(savedRecords.contains(parent1));
        Assert.assertTrue(savedRecords.contains(child1));
        Assert.assertTrue(savedRecords.contains(parent2));
        Assert.assertTrue(savedRecords.contains(child2));
    }

    /**
     * <strong>Goal:</strong> Verify that a linked {@link Record} with no
     * unsaved changes does not trigger a save notification when the parent is
     * saved.
     * <p>
     * <strong>Start state:</strong> A {@link ChildRecord} that has already been
     * saved and has no modifications.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener with a call counter.</li>
     * <li>Save a {@link ChildRecord} and record the notification count.</li>
     * <li>Create a {@link ParentRecord} that links to the already-saved child
     * and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only the {@link ParentRecord} triggers a
     * notification; the unchanged {@link ChildRecord} does not.
     */
    @Test
    public void testSaveListenerNotFiredForUnchangedLinkedRecord()
            throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                }).build();

        // Save the child first so it has no unsaved changes
        ChildRecord child = new ChildRecord();
        child.label = "Already Saved";
        runway.save(child);

        // Wait for initial notification
        Thread.sleep(2000);
        int countAfterChildSave = callCount.get();

        // Now create and save a parent that links to the
        // already-saved child
        ParentRecord parent = new ParentRecord();
        parent.name = "Parent";
        parent.child = child;
        runway.save(parent);

        // Wait for notification
        Thread.sleep(2000);

        // Only the parent should trigger a notification (the
        // child had no unsaved changes)
        Assert.assertEquals(countAfterChildSave + 1, callCount.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(Record...)} with a
     * single {@link Record} fires the save listener exactly once, not twice.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a save listener with a call counter.</li>
     * <li>Save a single {@link Record} via {@code runway.save(record)}.</li>
     * <li>Wait to confirm no additional notifications arrive.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call count is exactly {@code 1}.
     */
    @Test
    public void testNoDoubleNotificationForSingleRecord() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                }).build();

        Player player = new Player("Test", 10);
        runway.save(player);

        // Wait to ensure no extra notification arrives
        Thread.sleep(2000);

        Assert.assertEquals("Save listener should be called exactly once", 1,
                callCount.get());
    }

    /**
     * A test {@link Record} that holds a link to a {@link ChildRecord}.
     *
     * @author Jeff Nelson
     */
    public static class ParentRecord extends Record {

        public String name;
        public ChildRecord child;
    }

    /**
     * A test {@link Record} used as a linked child in notification tests.
     *
     * @author Jeff Nelson
     */
    public static class ChildRecord extends Record {

        public String label;
    }

    /**
     * A test record class that modifies multiple fields in preSave.
     */
    public static class ComplexPreSaveRecord extends Record {

        public String name;
        public int count;
        public long timestamp;
        public boolean preSaveCalled = false;

        @Override
        protected void beforeSave() {
            preSaveCalled = true;
            if(name != null) {
                name = name + " (Modified)";
            }
            count++;
            timestamp = System.currentTimeMillis();
        }
    }

    /**
     * A test record class that overrides the save operation.
     */
    public static class OverrideSaveRecord extends Record {

        public String name;
        public boolean overrideResult = true;
        public boolean overrideCalled = false;

        @Override
        protected Supplier<Boolean> overrideSave() {
            return () -> {
                overrideCalled = true;
                return overrideResult;
            };
        }
    }

    /**
     * A test record class that throws an exception in preSave.
     */
    public static class PreSaveExceptionRecord extends Record {

        public String name;

        @Override
        protected void beforeSave() {
            throw new IllegalStateException("Intentional exception in preSave");
        }
    }

    /**
     * A test record class that implements preSave to modify a field.
     */
    public static class PreSaveHookRecord extends Record {

        public String name;
        public boolean preSaveCalled = false;

        @Override
        protected void beforeSave() {
            preSaveCalled = true;
            if(name != null) {
                name = name + " (Modified)";
            }
        }
    }

    /**
     * A test record class that tracks when preSave is called.
     */
    public static class PreSaveOrderRecord extends Record {

        public String name;
        public boolean preSaveCalled = false;

        @Override
        protected void beforeSave() {
            preSaveCalled = true;
        }
    }

    /**
     * A test record class with a unique field constraint.
     */
    public static class UniqueFieldRecord extends Record {

        @Unique
        public String uniqueField;
    }
}