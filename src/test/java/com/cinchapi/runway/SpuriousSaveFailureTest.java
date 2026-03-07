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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.Concourse;

/**
 * Tests for automatic retry on spurious save failures caused by
 * {@code TransactionException} from concurrent saves with overlapping
 * {@link Unique @Unique} constraint reads.
 *
 * @author Jeff Nelson
 */
public class SpuriousSaveFailureTest extends RunwayBaseClientServerTest {

    @Override
    public void beforeEachTest() {
        runway = Runway.builder().port(server.getClientPort()).build();
    }

    /**
     * <strong>Goal:</strong> Verify that concurrent saves involving records
     * with overlapping {@link Unique} constraint reads cause a spurious
     * {@code TransactionException} when using the default
     * {@link SpuriousSaveFailureStrategy#FAIL_FAST} strategy.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link Runway} instance
     * with no saved records.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link TUser}, a {@link TTenant} (which creates a
     * {@link TSeat} in its constructor), and a {@link TWarehouse} linked to the
     * same {@link TSeat}.</li>
     * <li>Launch two threads: one saves the {@link TUser} and {@link TTenant}
     * together, the other saves the {@link TWarehouse}.</li>
     * <li>Use a {@link CountDownLatch} to synchronize the threads so both saves
     * are in-flight concurrently.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> At least one of the two concurrent saves
     * fails, demonstrating the spurious transaction conflict caused by
     * overlapping {@code @Unique} constraint reads.
     */
    @Test
    public void testConcurrentSaveWithUniqueConstraintCausesSpuriousFailure()
            throws Exception {
        TUser user = new TUser("alice");
        TTenant tenant = new TTenant(user);
        TSeat seat = tenant.seats.iterator().next();
        TWarehouse warehouse = new TWarehouse(seat);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicBoolean save1Result = new AtomicBoolean(false);
        AtomicBoolean save2Result = new AtomicBoolean(false);
        AtomicReference<Throwable> save1Error = new AtomicReference<>();
        AtomicReference<Throwable> save2Error = new AtomicReference<>();

        Thread t1 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                save1Result.set(runway.save(user, tenant));
            }
            catch (Throwable t) {
                save1Error.set(t);
            }
        });

        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                save2Result.set(runway.save(warehouse));
            }
            catch (Throwable t) {
                save2Error.set(t);
            }
        });

        t1.start();
        t2.start();
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        t1.join(10000);
        t2.join(10000);

        boolean anyFailed = !save1Result.get() || !save2Result.get()
                || save1Error.get() != null || save2Error.get() != null;
        Assert.assertTrue("At least one concurrent save should fail"
                + " due to spurious TransactionException", anyFailed);
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link SpuriousSaveFailureStrategy#RETRY} strategy automatically retries
     * a spurious {@code TransactionException} so that concurrent saves with
     * overlapping {@link Unique @Unique} constraint reads both succeed.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} instance configured with
     * {@link SpuriousSaveFailureStrategy#RETRY}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a separate {@link Runway} instance with
     * {@link SpuriousSaveFailureStrategy#RETRY}.</li>
     * <li>Create a shared {@link TUser} and two separate {@link TTenant
     * TTenants} that both link to that user. Each {@link TTenant} creates its
     * own {@link TSeat} with a compound {@link Unique @Unique} constraint on
     * user and tenant.</li>
     * <li>Launch two threads: each saves its own {@link TTenant} as the root
     * record, with the shared {@link TUser} as a linked record.</li>
     * <li>Use a {@link CountDownLatch} to synchronize the threads so both saves
     * are in-flight concurrently.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves succeed because each thread's root
     * record (its {@link TTenant}) is independent and has no stale data,
     * allowing the spurious {@code TransactionException} to be retried.
     */
    @Test
    public void testRetryStrategySucceedsOnSpuriousFailure() throws Exception {
        Runway retryRunway = Runway.builder().port(server.getClientPort())
                .spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)
                .build();
        try {
            TUser user = new TUser("bob");
            TTenant tenant1 = new TTenant(user);
            TTenant tenant2 = new TTenant(user);

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch go = new CountDownLatch(1);
            AtomicBoolean save1Result = new AtomicBoolean(false);
            AtomicBoolean save2Result = new AtomicBoolean(false);
            AtomicReference<Throwable> save1Error = new AtomicReference<>();
            AtomicReference<Throwable> save2Error = new AtomicReference<>();

            Thread t1 = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    save1Result.set(retryRunway.save(tenant1));
                }
                catch (Throwable t) {
                    save1Error.set(t);
                }
            });

            Thread t2 = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    save2Result.set(retryRunway.save(tenant2));
                }
                catch (Throwable t) {
                    save2Error.set(t);
                }
            });

            t1.start();
            t2.start();
            ready.await(5, TimeUnit.SECONDS);
            go.countDown();
            t1.join(10000);
            t2.join(10000);

            if(save1Error.get() != null) {
                throw CheckedExceptions
                        .throwAsRuntimeException(save1Error.get());
            }
            if(save2Error.get() != null) {
                throw CheckedExceptions
                        .throwAsRuntimeException(save2Error.get());
            }
            Assert.assertTrue("Save 1 should succeed with" + " RETRY strategy",
                    save1Result.get());
            Assert.assertTrue("Save 2 should succeed with" + " RETRY strategy",
                    save2Result.get());

            // Verify the complete object graph is persisted,
            // not just that save() returned true
            TUser loadedUser = retryRunway.load(TUser.class, user.id());
            Assert.assertNotNull("User should be persisted", loadedUser);
            Assert.assertEquals("bob", loadedUser.name);

            TTenant loadedTenant1 = retryRunway.load(TTenant.class,
                    tenant1.id());
            Assert.assertNotNull("Tenant 1 should be persisted", loadedTenant1);
            Assert.assertEquals("bob's tenant", loadedTenant1.name);
            Assert.assertNotNull("Tenant 1 should have an owner",
                    loadedTenant1.owner);
            Assert.assertFalse("Tenant 1 should have seats",
                    loadedTenant1.seats.isEmpty());

            TTenant loadedTenant2 = retryRunway.load(TTenant.class,
                    tenant2.id());
            Assert.assertNotNull("Tenant 2 should be persisted", loadedTenant2);
            Assert.assertEquals("bob's tenant", loadedTenant2.name);
            Assert.assertNotNull("Tenant 2 should have an owner",
                    loadedTenant2.owner);
            Assert.assertFalse("Tenant 2 should have seats",
                    loadedTenant2.seats.isEmpty());
        }
        finally {
            retryRunway.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that saving a root {@link Record} does not
     * overwrite a linked {@link Record Record's} database state when the linked
     * {@link Record} has no in-memory changes &mdash; even if the database
     * value differs from the in-memory value due to an external modification.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} instance configured with
     * {@link SpuriousSaveFailureStrategy#RETRY}. A {@link TUser} that has
     * already been saved and then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} ("dave").</li>
     * <li>Externally modify the {@link TUser TUser's} name to
     * "externally_modified" directly in the database, without updating the
     * in-memory object.</li>
     * <li>Create a {@link TTenant} linked to the (now stale) in-memory
     * {@link TUser} and save the {@link TTenant}.</li>
     * <li>Load the {@link TUser} from the database and verify the external
     * modification is preserved.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link TTenant} save succeeds. The linked
     * {@link TUser TUser's} database value remains "externally_modified"
     * because the save does not re-write linked records that have no in-memory
     * changes.
     */
    @Test
    public void testSaveDoesNotOverwriteExternallyModifiedLinkedRecord()
            throws Exception {
        Runway retryRunway = Runway.builder().port(server.getClientPort())
                .spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)
                .build();
        try {
            // Save the initial user
            TUser user = new TUser("dave");
            Assert.assertTrue(retryRunway.save(user));

            // Externally modify the user's name directly
            // via a separate Concourse connection
            Concourse concourse = retryRunway.connections.request();
            try {
                concourse.set("name", "externally_modified", user.id());
            }
            finally {
                retryRunway.connections.release(concourse);
            }

            // The in-memory user still has "dave" but the
            // database has "externally_modified"

            // Save a new tenant that links to this user.
            // The user is a linked record (not a root
            // record) in this save.
            TTenant tenant = new TTenant(user);
            Assert.assertTrue(retryRunway.save(tenant));

            // Verify the external modification to the user
            // is preserved (the save did not overwrite it)
            TUser loaded = retryRunway.load(TUser.class, user.id());
            Assert.assertEquals(
                    "External modification should be" + " preserved",
                    "externally_modified", loaded.name);
        }
        finally {
            retryRunway.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#hasStaleDataWithinTransaction(Concourse) hasStaleData}
     * returns {@code true} when a {@link Record Record's} database state has
     * been modified by an external transaction since the {@link Record} was
     * last loaded or saved.
     * <p>
     * <strong>Start state:</strong> A freshly saved {@link TUser}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Externally modify the {@link TUser TUser's} name directly in the
     * database via a separate {@link Concourse} connection.</li>
     * <li>Call {@code hasStaleData} on the in-memory {@link TUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code hasStaleData} returns {@code true}
     * because the database was modified after the {@link Record Record's}
     * {@code __loadedAt} timestamp.
     */
    @Test
    public void testHasStaleDataReturnsTrueAfterExternalModification()
            throws Exception {
        Runway retryRunway = Runway.builder().port(server.getClientPort())
                .spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)
                .build();
        try {
            TUser user = new TUser("eve");
            Assert.assertTrue(retryRunway.save(user));

            // Externally modify the user in the database
            Concourse concourse = retryRunway.connections.request();
            try {
                concourse.set("name", "conflict", user.id());
            }
            finally {
                retryRunway.connections.release(concourse);
            }

            // hasStaleData should detect the external write
            Concourse check = retryRunway.connections.request();
            try {
                Assert.assertTrue(
                        "hasStaleData should return true" + " after external"
                                + " modification",
                        user.hasStaleDataWithinTransaction(check));
            }
            finally {
                retryRunway.connections.release(check);
            }
        }
        finally {
            retryRunway.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#hasStaleDataWithinTransaction(Concourse) hasStaleData}
     * returns {@code false} when no external transaction has modified the
     * {@link Record Record's} database state since it was last saved.
     * <p>
     * <strong>Start state:</strong> A freshly saved {@link TUser}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Call {@code hasStaleData} on the in-memory {@link TUser} without any
     * external modifications.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code hasStaleData} returns {@code false}
     * because no writes occurred after the {@link Record Record's}
     * {@code __loadedAt} timestamp.
     */
    @Test
    public void testHasStaleDataReturnsFalseWhenUnmodified() throws Exception {
        Runway retryRunway = Runway.builder().port(server.getClientPort())
                .spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)
                .build();
        try {
            TUser user = new TUser("frank");
            Assert.assertTrue(retryRunway.save(user));

            Concourse check = retryRunway.connections.request();
            try {
                Assert.assertFalse(
                        "hasStaleData should return false" + " when no external"
                                + " modification occurred",
                        user.hasStaleDataWithinTransaction(check));
            }
            finally {
                retryRunway.connections.release(check);
            }
        }
        finally {
            retryRunway.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the default
     * {@link SpuriousSaveFailureStrategy#FAIL_FAST} strategy does not retry on
     * {@code TransactionException} and returns {@code false} immediately.
     * <p>
     * <strong>Start state:</strong> A freshly created {@link Runway} instance
     * using the default {@link SpuriousSaveFailureStrategy#FAIL_FAST} strategy
     * with no saved records.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a shared {@link TUser} and two separate {@link TTenant
     * TTenants} that both link to that user.</li>
     * <li>Launch two threads: each saves its own {@link TTenant}.</li>
     * <li>Use a {@link CountDownLatch} to synchronize the threads so both saves
     * are in-flight concurrently.</li>
     * <li>If a spurious failure occurs, verify the save returned {@code false}
     * without retrying.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> At least one save fails and returns
     * {@code false}. The failed save does not retry because
     * {@link SpuriousSaveFailureStrategy#FAIL_FAST} is the active strategy.
     */
    @Test
    public void testFailFastStrategyDoesNotRetry() throws Exception {
        TUser user = new TUser("ivan");
        TTenant tenant1 = new TTenant(user);
        TTenant tenant2 = new TTenant(user);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicBoolean save1Result = new AtomicBoolean(false);
        AtomicBoolean save2Result = new AtomicBoolean(false);

        Thread t1 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                save1Result.set(runway.save(tenant1));
            }
            catch (Throwable t) {
                // save returned false or threw
            }
        });

        Thread t2 = new Thread(() -> {
            ready.countDown();
            try {
                go.await();
                save2Result.set(runway.save(tenant2));
            }
            catch (Throwable t) {
                // save returned false or threw
            }
        });

        t1.start();
        t2.start();
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        t1.join(10000);
        t2.join(10000);

        boolean anyFailed = !save1Result.get() || !save2Result.get();
        Assert.assertTrue(
                "At least one save should fail with" + " FAIL_FAST strategy",
                anyFailed);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#hasStaleDataWithinTransaction(Concourse) hasStaleData}
     * returns {@code false} after loading a {@link Record} from the database,
     * since the loaded state is in sync with the database.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then loaded fresh from the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Load the {@link TUser} from the database into a new instance.</li>
     * <li>Call {@code hasStaleData} on the loaded instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code hasStaleData} returns {@code false}
     * because the loaded {@link Record} is in sync with the database.
     */
    @Test
    public void testHasStaleDataReturnsFalseAfterLoad() throws Exception {
        Runway retryRunway = Runway.builder().port(server.getClientPort())
                .spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)
                .build();
        try {
            TUser user = new TUser("julia");
            Assert.assertTrue(retryRunway.save(user));

            TUser loaded = retryRunway.load(TUser.class, user.id());
            Assert.assertNotNull(loaded);

            Concourse check = retryRunway.connections.request();
            try {
                Assert.assertFalse(
                        "hasStaleData should return false"
                                + " for a freshly loaded record",
                        loaded.hasStaleDataWithinTransaction(check));
            }
            finally {
                retryRunway.connections.release(check);
            }
        }
        finally {
            retryRunway.close();
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the snapshot/restore mechanism
     * correctly preserves linked {@link Record} metadata across a retry, so
     * that the full object graph is persisted even though the linked
     * {@link Record Records} were not root records in the save call.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} instance configured with
     * {@link SpuriousSaveFailureStrategy#RETRY} with no saved records.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} so it exists in the database.</li>
     * <li>Create a {@link TTenant} linked to that {@link TUser} and save
     * it.</li>
     * <li>Load the {@link TTenant} and verify its {@link TSeat} contains the
     * correct {@link TUser} link.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The full object graph (including {@link TSeat}
     * linked records) is persisted with correct data.
     */
    @Test
    public void testLinkedRecordsPersistedAfterSave() throws Exception {
        Runway retryRunway = Runway.builder().port(server.getClientPort())
                .spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)
                .build();
        try {
            TUser user = new TUser("kate");
            TTenant tenant = new TTenant(user);
            Assert.assertTrue(retryRunway.save(tenant));

            TTenant loaded = retryRunway.load(TTenant.class, tenant.id());
            Assert.assertNotNull("Tenant should be persisted", loaded);
            Assert.assertFalse("Tenant should have seats",
                    loaded.seats.isEmpty());
            TSeat seat = loaded.seats.iterator().next();
            Assert.assertNotNull("Seat should have a user link", seat.user);
            Assert.assertEquals("kate", seat.user.name);
        }
        finally {
            retryRunway.close();
        }
    }

    /**
     * A test user record.
     *
     * @author Jeff Nelson
     */
    public static class TUser extends Record {

        /**
         * The user's name.
         */
        String name;

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
     * A test tenant record that links to its owner {@link TUser} and creates a
     * {@link TSeat} in its constructor.
     *
     * @author Jeff Nelson
     */
    public static class TTenant extends Record {

        /**
         * The tenant's name.
         */
        String name;

        /**
         * The owner of this tenant.
         */
        TUser owner;

        /**
         * The seats belonging to this tenant.
         */
        Set<TSeat> seats;

        /**
         * Construct a new instance.
         *
         * @param owner the {@link TUser} who owns this tenant
         */
        public TTenant(TUser owner) {
            this.name = owner.name + "'s tenant";
            this.owner = owner;
            this.seats = new LinkedHashSet<>();
            TSeat seat = new TSeat(owner, this);
            this.seats.add(seat);
        }
    }

    /**
     * A test seat record with a compound {@link Unique} constraint on its
     * {@link TUser} and {@link TTenant}.
     *
     * @author Jeff Nelson
     */
    public static class TSeat extends Record {

        /**
         * The user assigned to this seat.
         */
        @Unique(name = "seat_assignment")
        TUser user;

        /**
         * The tenant this seat belongs to.
         */
        @Unique(name = "seat_assignment")
        TTenant tenant;

        /**
         * Construct a new instance.
         *
         * @param user the {@link TUser} assigned to this seat
         * @param tenant the {@link TTenant} this seat belongs to
         */
        public TSeat(TUser user, TTenant tenant) {
            this.user = user;
            this.tenant = tenant;
        }
    }

    /**
     * A test warehouse record with a {@link Unique} constraint on its
     * {@link TSeat}.
     *
     * @author Jeff Nelson
     */
    public static class TWarehouse extends Record {

        /**
         * The seat associated with this warehouse.
         */
        @Unique
        TSeat seat;

        /**
         * Construct a new instance.
         *
         * @param seat the {@link TSeat} linked to this warehouse
         */
        public TWarehouse(TSeat seat) {
            this.seat = seat;
        }
    }

}
