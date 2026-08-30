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
 * Tests for {@link Runway#create(Class, Object...)}.
 *
 * @author Jeff Nelson
 */
public class RunwayCreateTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} created through a
     * {@link Runway} is bound to it, so a direct {@code save()} persists within
     * it.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code runway.create(Widget.class)}.</li>
     * <li>Set the name and {@code save()} the {@link Widget} directly.</li>
     * <li>Load the {@link Widget} through the {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Widget} is durable with the saved
     * name.
     */
    @Test
    public void testCreateBindsRecordToInstance() {
        Widget widget = runway.create(Widget.class);
        widget.name = "gear";
        Assert.assertTrue(widget.save());
        Assert.assertEquals("gear",
                runway.load(Widget.class, widget.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} created through a
     * {@link Runway} is bound to it even when multiple {@link Runway} instances
     * are open and no instance is pinned.
     * <p>
     * <strong>Start state:</strong> The test {@link #runway} is open.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a second {@link Runway} against the test server.</li>
     * <li>Call {@code runway.create(Widget.class)}, set the name and
     * {@code save()} the {@link Widget} directly.</li>
     * <li>Load the {@link Widget} through the {@link #runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the {@link Widget} is
     * durable, because the create bound the record to the instance.
     */
    @Test
    public void testCreateBindsRecordWhenNoInstanceIsPinned() {
        Runway other = runwayBuilder().build();
        try {
            Widget widget = runway.create(Widget.class);
            widget.name = "gear";
            Assert.assertTrue(widget.save());
            Assert.assertNotNull(runway.load(Widget.class, widget.id()));
        }
        finally {
            try {
                other.close();
            }
            catch (Exception ignored) {/* close failure not under test */}
        }
    }

    /**
     * A minimal named {@link Record}.
     *
     * @author Jeff Nelson
     */
    public static class Widget extends Record {

        /**
         * The display name.
         */
        public String name;

    }

}
