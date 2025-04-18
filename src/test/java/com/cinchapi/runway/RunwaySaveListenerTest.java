package com.cinchapi.runway;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.google.common.collect.Sets;

public class RunwaySaveListenerTest extends RunwayBaseClientServerTest {

    @Test
    public void testSaveListenerCalledOnSingleRecordSave() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<Record> savedRecords = Sets.newConcurrentHashSet();
        
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                })
                .build();
        
        Player player = new Player("Jeff Nelson", 42);
        player.save();
        
        // Wait for the save listener to be called
        Assert.assertTrue("Save listener was not called within timeout", 
                latch.await(5, TimeUnit.SECONDS));
        
        Assert.assertEquals(1, savedRecords.size());
        Assert.assertTrue(savedRecords.contains(player));
    }
    
    @Test
    public void testSaveListenerCalledOnMultipleRecordSave() throws Exception {
        int recordCount = 5;
        CountDownLatch latch = new CountDownLatch(recordCount);
        Set<Record> savedRecords = Sets.newConcurrentHashSet();
        
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .onSave(record -> {
                    savedRecords.add(record);
                    latch.countDown();
                })
                .build();
        
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
    public void testSaveListenerNotCalledOnFailedSave() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                })
                .build();
        
        // Create a record with a unique field
        UniqueFieldRecord record1 = new UniqueFieldRecord();
        record1.uniqueField = "unique";
        record1.save();
        
        // Try to save another record with the same unique field (should fail)
        UniqueFieldRecord record2 = new UniqueFieldRecord();
        record2.uniqueField = "unique";
        boolean saved = record2.save();
        
        Assert.assertFalse("Save should have failed due to unique constraint", saved);
        
        // Give some time for any potential async processing
        Thread.sleep(1000);
        
        // Verify the save listener was only called once (for the first successful save)
        Assert.assertEquals(1, callCount.get());
    }
    
    @Test
    public void testSaveListenerExceptionDoesNotAffectSave() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Set<String> processedNames = ConcurrentHashMap.newKeySet();
        
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .onSave(record -> {
                    if (record instanceof Player) {
                        Player player = (Player) record;
                        processedNames.add(player.name);
                        
                        // Throw an exception to test error handling
                        if ("ThrowError".equals(player.name)) {
                            throw new RuntimeException("Test exception");
                        }
                        
                        if ("LastPlayer".equals(player.name)) {
                            latch.countDown();
                        }
                    }
                })
                .build();
        
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
    public void testNoSaveListenerDoesNotCreateExecutor() throws Exception {
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .build();
        
        // Verify the saveNotificationExecutor field is null
        Object executor = Reflection.get("saveNotificationExecutor", runway);
        Assert.assertNull("Save notification executor should be null when no listener is provided", 
                executor);
        
        // Save a record to ensure it works without a listener
        Player player = new Player("Test Player", 30);
        boolean saved = player.save();
        
        Assert.assertTrue("Record should save successfully without a listener", saved);
    }
    
    @Test
    public void testSaveListenerCalledForUpdates() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger callCount = new AtomicInteger(0);
        
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                })
                .build();
        
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
    public void testQueueSaveNotificationWithNoListenerIsNoOp() throws Exception {
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .build();
        
        // Verify the saveNotificationQueue field is null
        Object queue = Reflection.get("saveNotificationQueue", runway);
        Assert.assertNull("Save notification queue should be null when no listener is provided", 
                queue);
        
        // Call queueSaveNotification directly to ensure it doesn't throw an exception
        Player player = new Player("Test Player", 30);
        runway.queueSaveNotification(player);
        
        // If we got here without an exception, the test passes
        Assert.assertTrue(true);
    }
    
    @Test
    public void testMultipleRecordSaveWithNoListener() throws Exception {
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .build();
        
        // Create and save multiple records
        Player[] players = new Player[5];
        for (int i = 0; i < players.length; i++) {
            players[i] = new Player("Player " + i, i * 10);
        }
        
        boolean saved = runway.save(players);
        
        // Verify the save was successful
        Assert.assertTrue("Multiple records should save successfully without a listener", saved);
        
        // Verify all records were saved
        for (Player player : players) {
            Player loaded = runway.load(Player.class, player.id());
            Assert.assertEquals(player.name, loaded.name);
            Assert.assertEquals(player.score, loaded.score);
        }
    }
    
    @Test
    public void testSwitchingBetweenListenerAndNoListener() throws Exception {
        // Start with a listener
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);
        
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .onSave(record -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                })
                .build();
        
        // Save a record with the listener
        Player player1 = new Player("Player with listener", 100);
        player1.save();
        
        // Wait for the listener to be called
        Assert.assertTrue("Save listener was not called", 
                latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, callCount.get());
        
        // Now switch to no listener
        runway.close();
        runway = Runway.builder()
                .port(server.getClientPort())
                .build();
        
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
     * A test record class with a unique field constraint.
     */
    public static class UniqueFieldRecord extends Record {
        
        @Unique
        public String uniqueField;
    }
}