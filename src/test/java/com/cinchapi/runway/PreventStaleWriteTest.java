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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;

/**
 * Tests for {@link Runway#save(boolean, Record...)} with
 * {@code preventStaleWrites} enabled.
 *
 * @author Jeff Nelson
 */
public class PreventStaleWriteTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} throws a {@link StaleDataException} when the
     * {@link Record} has been externally modified since it was last saved.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "alice".</li>
     * <li>Externally modify the name to "conflict" directly in the database via
     * a separate {@link Concourse} connection.</li>
     * <li>Modify the in-memory name to "local_change".</li>
     * <li>Call {@code runway.save(true, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown because
     * the {@link Record} has stale data relative to the database.
     */
    @Test(expected = StaleDataException.class)
    public void testPreventStaleWriteThrowsWhenStale() {
        TUser user = new TUser("alice");
        Assert.assertTrue(runway.save(user));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "conflict", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        user.name = "local_change";
        runway.save(true, user);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} succeeds when the {@link Record} has not been externally
     * modified since it was last saved.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved with no
     * external modifications.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "bob".</li>
     * <li>Modify the in-memory name to "updated" without any external database
     * changes.</li>
     * <li>Call {@code runway.save(true, user)}.</li>
     * <li>Load the {@link TUser} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and the loaded
     * {@link TUser TUser's} name equals "updated".
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenNotStale() {
        TUser user = new TUser("bob");
        Assert.assertTrue(runway.save(user));

        user.name = "updated";
        Assert.assertTrue(runway.save(true, user));

        TUser loaded = runway.load(TUser.class, user.id());
        Assert.assertEquals("updated", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link StaleDataException} thrown
     * by {@link Runway#save(boolean, Record...) save(true, ...)} carries the
     * correct primary key of the stale {@link Record}.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "charlie".</li>
     * <li>Externally modify the name directly in the database.</li>
     * <li>Call {@code runway.save(true, user)} and catch the
     * {@link StaleDataException}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The caught {@link StaleDataException
     * StaleDataException's} {@link StaleDataException#id() id()} matches the
     * {@link TUser TUser's} primary key.
     */
    @Test
    public void testPreventStaleWriteIdentifiesStaleRecord() {
        TUser user = new TUser("charlie");
        Assert.assertTrue(runway.save(user));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "external", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        try {
            user.name = "local";
            runway.save(true, user);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(user.id(), e.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} does not prevent a save when the {@link Record} was
     * freshly loaded from the database.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved and then loaded fresh
     * from the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "dave".</li>
     * <li>Load the {@link TUser} from the database into a new instance.</li>
     * <li>Modify the loaded instance's name to "modified".</li>
     * <li>Call {@code runway.save(true, loaded)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} because the
     * loaded {@link Record} is in sync with the database.
     */
    @Test
    public void testPreventStaleWriteSucceedsAfterLoad() {
        TUser user = new TUser("dave");
        Assert.assertTrue(runway.save(user));

        TUser loaded = runway.load(TUser.class, user.id());
        loaded.name = "modified";
        Assert.assertTrue(runway.save(true, loaded));

        TUser reloaded = runway.load(TUser.class, user.id());
        Assert.assertEquals("modified", reloaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} succeeds after calling {@link Record#refresh()} on a
     * previously stale {@link Record}.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "eve".</li>
     * <li>Externally modify the name to "refreshed" directly in the
     * database.</li>
     * <li>Call {@link Record#refresh()} to re-sync the in-memory state.</li>
     * <li>Modify the name to "final_value".</li>
     * <li>Call {@code runway.save(true, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} because
     * {@link Record#refresh()} brought the {@link Record} back in sync with the
     * database.
     */
    @Test
    public void testPreventStaleWriteSucceedsAfterRefresh() {
        TUser user = new TUser("eve");
        Assert.assertTrue(runway.save(user));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "refreshed", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        user.refresh();
        user.name = "final_value";
        Assert.assertTrue(runway.save(true, user));

        TUser loaded = runway.load(TUser.class, user.id());
        Assert.assertEquals("final_value", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(false, ...)} does not throw {@link StaleDataException} even when the
     * {@link Record} has been externally modified &mdash; the stale check only
     * applies when {@code preventStaleWrites} is {@code true}.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "frank".</li>
     * <li>Externally modify the name to "external" directly in the
     * database.</li>
     * <li>Modify the in-memory name to "overwrite".</li>
     * <li>Call {@code runway.save(false, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} because the
     * stale check is disabled.
     */
    @Test
    public void testSaveWithoutPreventStaleWriteIgnoresStaleness() {
        TUser user = new TUser("frank");
        Assert.assertTrue(runway.save(user));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "external", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        user.name = "overwrite";
        Assert.assertTrue(runway.save(false, user));
    }

    /**
     * <strong>Goal:</strong> Verify that the default
     * {@link Runway#save(Record...) save(records)} (without the boolean
     * parameter) does not perform stale checks, preserving backward
     * compatibility.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "gina".</li>
     * <li>Externally modify the name to "external" directly in the
     * database.</li>
     * <li>Modify the in-memory name to "overwrite".</li>
     * <li>Call {@code runway.save(user)} (no boolean parameter).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} because the
     * original {@code save} method delegates to {@code save(false, records)}.
     */
    @Test
    public void testDefaultSaveDoesNotPreventStaleWrites() {
        TUser user = new TUser("gina");
        Assert.assertTrue(runway.save(user));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "external", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        user.name = "overwrite";
        Assert.assertTrue(runway.save(user));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} throws a {@link StaleDataException} when a linked
     * {@link Record} in the object graph has been externally modified.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} and a {@link TTenant}
     * linked to it, both saved. The {@link TUser} is then externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "hank".</li>
     * <li>Create and save a {@link TTenant} linked to that {@link TUser}.</li>
     * <li>Externally modify the {@link TUser TUser's} name in the
     * database.</li>
     * <li>Modify the {@link TTenant TTenant's} name in memory.</li>
     * <li>Call {@code runway.save(true, tenant)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown because
     * the linked {@link TUser} has stale data.
     */
    @Test(expected = StaleDataException.class)
    public void testPreventStaleWriteDetectsStaleLinkedRecord() {
        TUser user = new TUser("hank");
        TTenant tenant = new TTenant(user);
        Assert.assertTrue(runway.save(tenant));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "external_hank", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        tenant.name = "modified_tenant";
        runway.save(true, tenant);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} does not persist any data when a
     * {@link StaleDataException} is thrown &mdash; the transaction is fully
     * rolled back.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "iris".</li>
     * <li>Externally modify the name to "external" directly in the
     * database.</li>
     * <li>Modify the in-memory name to "should_not_persist" and attempt
     * {@code runway.save(true, user)}.</li>
     * <li>Catch the {@link StaleDataException}.</li>
     * <li>Load the {@link TUser} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link TUser TUser's} name equals
     * "external" (the external modification), not "should_not_persist".
     */
    @Test
    public void testPreventStaleWriteDoesNotPersistOnFailure() {
        TUser user = new TUser("iris");
        Assert.assertTrue(runway.save(user));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "external", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        user.name = "should_not_persist";
        try {
            runway.save(true, user);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            // expected
        }

        TUser loaded = runway.load(TUser.class, user.id());
        Assert.assertEquals("external", loaded.name);
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
     * A test tenant record that links to its owner {@link TUser}.
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
     * A test seat record linked to a {@link TUser} and {@link TTenant}.
     *
     * @author Jeff Nelson
     */
    public static class TSeat extends Record {

        /**
         * The user assigned to this seat.
         */
        TUser user;

        /**
         * The tenant this seat belongs to.
         */
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

}
