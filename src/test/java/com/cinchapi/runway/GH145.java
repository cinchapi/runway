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

import com.cinchapi.concourse.time.Time;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/145">GH-145</a>: loading a
 * record by id through an abstract class (most importantly, {@link Record}
 * itself) must resolve the record's concrete class instead of throwing
 * {@code InstantiationException}.
 *
 * @author Jeff Nelson
 */
public class GH145 extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@code load(Record.class, id)}
     * resolves the stored record's concrete class.
     * <p>
     * <strong>Start state:</strong> A saved {@link Player}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the record by id with {@link Record} as the requested
     * class.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded record is a {@link Player} with its
     * saved state intact.
     */
    @Test
    public void testLoadByRecordClassResolvesConcreteClass() {
        Player player = new Player("Magic Johnson", 25);
        runway.save(player);

        Record record = runway.load(Record.class, player.id());

        Assert.assertTrue(record instanceof Player);
        Assert.assertEquals("Magic Johnson", ((Player) record).name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code load(Record.class, id)}
     * resolves a record to its most specific class, not just any concrete
     * ancestor.
     * <p>
     * <strong>Start state:</strong> A saved {@link PointGuard}, which is a
     * subclass of {@link Player}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the record by id with {@link Record} as the requested
     * class.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded record is a {@link PointGuard} with
     * its subclass state intact.
     */
    @Test
    public void testLoadByRecordClassResolvesMostSpecificClass() {
        PointGuard pointGuard = new PointGuard("John Stockton", 13, 15);
        runway.save(pointGuard);

        Record record = runway.load(Record.class, pointGuard.id());

        Assert.assertTrue(record instanceof PointGuard);
        Assert.assertEquals(15, ((PointGuard) record).assists);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code load(Record.class, id)} returns
     * {@code null} for an id that names no record.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load a never-used id with {@link Record} as the requested class.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load returns {@code null} without
     * throwing.
     */
    @Test
    public void testLoadByRecordClassWithUnknownIdReturnsNull() {
        Assert.assertNull(runway.load(Record.class, Time.now()));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code load(Record.class, id)} returns
     * {@code null} for an id whose data was not written by Runway and therefore
     * has no class section.
     * <p>
     * <strong>Start state:</strong> A raw Concourse record written directly
     * through the client.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Add data to an id with {@code client.add(...)}.</li>
     * <li>Load that id with {@link Record} as the requested class.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load returns {@code null} without
     * throwing, consistent with how invalid records are indistinguishable from
     * invisible ones.
     */
    @Test
    public void testLoadByRecordClassOfNonRunwayRecordReturnsNull() {
        long id = Time.now();
        client.add("name", "not a runway record", id);

        Assert.assertNull(runway.load(Record.class, id));
    }
}
