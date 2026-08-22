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

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.TransactionException;
import com.cinchapi.concourse.time.Time;

/**
 * Tests that a staged transaction refuses to commit when another writer changes
 * a value that the transaction read.
 * <p>
 * Every operation is a direct {@link Concourse} call on a connection from the
 * {@link Runway Runway's} pool. No {@link Record}, no save pipeline and no
 * retry logic participates, so a failure here is server behavior rather than
 * framework behavior. The tests differ only in which write the transaction
 * performs and whether another connection reads the record while the
 * transaction is open.
 * </p>
 * <p>
 * A field read through a transaction joins the conflict footprint on its own,
 * whether or not another session reads the whole record and whether or not the
 * transaction also writes that record. These pin the server behavior that
 * {@link Record#verifyOnSave(String...) verifyOnSave} rests on.
 * </p>
 *
 * @author Jeff Nelson
 */
public class TransactionReadConflictTest extends RunwayBaseClientServerTest {

    /**
     * Wait for the wall clock to advance one millisecond, so the competing
     * write owns its own millisecond.
     */
    private static void tick() {
        long millis = System.currentTimeMillis();
        while (System.currentTimeMillis() == millis) {
            Thread.yield();
        }
    }

    /**
     * Run the scenario and report whether the transaction refused to commit.
     * <p>
     * The transaction reads {@code bio} and writes {@code name} with the
     * requested operation. A separate connection then changes {@code bio}
     * before the commit, so the commit must be refused.
     * </p>
     *
     * @param write {@code "set"}, {@code "clear"} or {@code "verifyOrSet"}
     * @param anotherSessionReads whether a separate connection reads the record
     *            while the transaction is open
     * @return {@code true} if the commit was refused
     */
    private boolean conflicts(String write, boolean anotherSessionReads) {
        long id = Time.now();
        Concourse seed = runway.connections.request();
        try {
            seed.set("bio", "original", id);
            seed.set("name", "seed", id);
        }
        finally {
            runway.connections.release(seed);
        }
        Concourse transaction = runway.connections.request();
        try {
            transaction.stage();
            if(anotherSessionReads) {
                Concourse reader = runway.connections.request();
                try {
                    reader.select(id);
                }
                finally {
                    runway.connections.release(reader);
                }
            }
            transaction.select("bio", id);
            if(write.equals("verifyOrSet")) {
                transaction.verifyOrSet("name", "updated", id);
            }
            else if(write.equals("clear")) {
                transaction.clear("name", id);
            }
            else {
                transaction.set("name", "updated", id);
            }
            tick();
            Concourse writer = runway.connections.request();
            try {
                writer.set("bio", "external", id);
            }
            finally {
                runway.connections.release(writer);
            }
            tick();
            return !transaction.commit();
        }
        catch (TransactionException e) {
            return true;
        }
        finally {
            try {
                transaction.abort();
            }
            catch (Exception e) {/* the transaction already ended */}
            runway.connections.release(transaction);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a transaction that uses
     * {@code verifyOrSet} still conflicts when another session read the record.
     * <p>
     * <strong>Start state:</strong> A record that stores a bio and a name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a transaction.</li>
     * <li>Read the record through a separate connection.</li>
     * <li>Read the bio through the transaction and {@code verifyOrSet} the
     * name.</li>
     * <li>Change the bio through a third connection, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The commit is refused.
     */
    @Test
    public void testConflictWithVerifyOrSetWhenAnotherSessionReads() {
        Assert.assertTrue(conflicts("verifyOrSet", true));
    }

    /**
     * <strong>Goal:</strong> Verify that a transaction that uses
     * {@code verifyOrSet} conflicts when no other session reads the record.
     * <p>
     * <strong>Start state:</strong> A record that stores a bio and a name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a transaction.</li>
     * <li>Read the bio through the transaction and {@code verifyOrSet} the
     * name.</li>
     * <li>Change the bio through a separate connection, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The commit is refused.
     */
    @Test
    public void testConflictWithVerifyOrSetWhenNoOtherSessionReads() {
        Assert.assertTrue(conflicts("verifyOrSet", false));
    }

    /**
     * <strong>Goal:</strong> Verify that a transaction that uses {@code set}
     * conflicts when another session read the record.
     * <p>
     * <strong>Start state:</strong> A record that stores a bio and a name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a transaction.</li>
     * <li>Read the record through a separate connection.</li>
     * <li>Read the bio through the transaction and {@code set} the name.</li>
     * <li>Change the bio through a third connection, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The commit is refused.
     */
    @Test
    public void testConflictWithSetWhenAnotherSessionReads() {
        Assert.assertTrue(conflicts("set", true));
    }

    /**
     * <strong>Goal:</strong> Verify that a transaction that uses {@code clear}
     * conflicts when another session read the record.
     * <p>
     * <strong>Start state:</strong> A record that stores a bio and a name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage a transaction.</li>
     * <li>Read the record through a separate connection.</li>
     * <li>Read the bio through the transaction and {@code clear} the name.</li>
     * <li>Change the bio through a third connection, then commit.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The commit is refused.
     */
    @Test
    public void testConflictWithClearWhenAnotherSessionReads() {
        Assert.assertTrue(conflicts("clear", true));
    }

}
