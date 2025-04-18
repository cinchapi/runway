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
        try {
            runway.load(OverrideSaveRecord.class, record.id());
            Assert.fail("Record should not exist in the database");
        }
        catch (IllegalStateException e) {
            // Expected exception
        }
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
        try {
            runway.load(OverrideSaveRecord.class, record2.id());
            Assert.fail("Overridden record should not exist in the database");
        }
        catch (IllegalStateException e) {
            // Expected exception
        }
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
        try {
            runway.load(OverrideSaveRecord.class, record.id());
            Assert.fail("Record should not exist in the database");
        }
        catch (IllegalStateException e) {
            // Expected exception
        }
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
            try {
                runway.load(record.getClass(), record.id());
                Assert.fail("Record with ID " + record.id()
                        + " should not exist in the database");
            }
            catch (IllegalStateException e) {
                // Expected exception
            }
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
        try {
            runway.load(PreSaveExceptionRecord.class, record.id());
            Assert.fail("Record should not exist in the database");
        }
        catch (IllegalStateException e) {
            // Expected exception
        }
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
        try {
            runway.load(PreSaveHookRecord.class, record1.id());
            Assert.fail("Record1 should not exist in the database");
        }
        catch (IllegalStateException e) {
            // Expected exception
        }

        try {
            runway.load(PreSaveExceptionRecord.class, record2.id());
            Assert.fail("Record2 should not exist in the database");
        }
        catch (IllegalStateException e) {
            // Expected exception
        }

        try {
            runway.load(PreSaveHookRecord.class, record3.id());
            Assert.fail("Record3 should not exist in the database");
        }
        catch (IllegalStateException e) {
            // Expected exception
        }
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