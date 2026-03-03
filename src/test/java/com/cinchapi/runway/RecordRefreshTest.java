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

/**
 * Tests for {@link Record#refresh()}.
 *
 * @author Jeff Nelson
 */
public class RecordRefreshTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@link Record#refresh()} reloads the
     * latest persisted data from the database, replacing the in-memory state.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "alice".</li>
     * <li>Externally modify the name to "updated" directly in the database via
     * a separate {@link Concourse} connection.</li>
     * <li>Call {@link Record#refresh()} on the in-memory {@link TUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The in-memory {@link TUser TUser's} name
     * equals "updated" after the refresh.
     */
    @Test
    public void testRefreshReloadsLatestData() {
        TUser user = new TUser("alice");
        Assert.assertTrue(runway.save(user));

        Concourse concourse = runway.connections.request();
        try {
            concourse.set("name", "updated", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        user.refresh();
        Assert.assertEquals("updated", user.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#refresh()} discards
     * unsaved in-memory changes by replacing them with the latest persisted
     * state.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "bob".</li>
     * <li>Modify the in-memory name to "local" without saving.</li>
     * <li>Call {@link Record#refresh()} on the {@link TUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The in-memory {@link TUser TUser's} name
     * reverts to "bob" because the unsaved change is overwritten by the
     * persisted state.
     */
    @Test
    public void testRefreshClearsUnsavedChanges() {
        TUser user = new TUser("bob");
        Assert.assertTrue(runway.save(user));

        user.name = "local";
        user.refresh();
        Assert.assertEquals("bob", user.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#refresh()} throws an
     * {@link IllegalStateException} when called on a {@link Record} that is not
     * pinned to a {@link Runway} instance.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link TUser} that
     * has never been saved to any {@link Runway} instance.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a new {@link TUser} without saving it.</li>
     * <li>Call {@link Record#refresh()} on the unpinned {@link TUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalStateException} is thrown
     * because the {@link Record} has no associated {@link Runway} instance to
     * refresh from.
     */
    @Test(expected = IllegalStateException.class)
    public void testRefreshThrowsWhenNotPinnedToRunway() {
        TUser user = new TUser("unpinned");
        user.refresh();
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Record#hasStaleDataWithinTransaction(com.cinchapi.concourse.Concourse)
     * hasStaleDataWithinTransaction} returns {@code false} after calling
     * {@link Record#refresh()} on a previously stale {@link Record}.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database, making it stale.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "stale".</li>
     * <li>Externally modify the name to "external" directly in the
     * database.</li>
     * <li>Verify that {@code hasStaleDataWithinTransaction} returns
     * {@code true}.</li>
     * <li>Call {@link Record#refresh()} on the {@link TUser}.</li>
     * <li>Check {@code hasStaleDataWithinTransaction} again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> After {@link Record#refresh()},
     * {@code hasStaleDataWithinTransaction} returns {@code false} because the
     * {@link Record} is back in sync with the database.
     */
    @Test
    public void testHasStaleDataReturnsFalseAfterRefresh() {
        TUser user = new TUser("stale");
        Assert.assertTrue(runway.save(user));

        com.cinchapi.concourse.Concourse concourse = runway.connections
                .request();
        try {
            concourse.set("name", "external", user.id());
        }
        finally {
            runway.connections.release(concourse);
        }

        // Confirm the record is stale before refresh
        com.cinchapi.concourse.Concourse check = runway.connections.request();
        try {
            Assert.assertTrue("Should be stale before refresh",
                    user.hasStaleDataWithinTransaction(check));
        }
        finally {
            runway.connections.release(check);
        }

        user.refresh();

        // After refresh, stale data should be cleared
        com.cinchapi.concourse.Concourse check2 = runway.connections.request();
        try {
            Assert.assertFalse("Should not be stale after refresh",
                    user.hasStaleDataWithinTransaction(check2));
        }
        finally {
            runway.connections.release(check2);
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

}
