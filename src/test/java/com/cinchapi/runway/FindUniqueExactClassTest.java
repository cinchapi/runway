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

import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests that verify {@link Runway#findUnique} only searches the exact class
 * specified, not subclasses. This addresses a bug where {@code findUnique}
 * incorrectly used {@code filterAny} (which searches the class hierarchy)
 * instead of {@code filter} (exact class only).
 * <p>
 * Before the fix, calling {@code findUnique(Player.class, criteria)} when both
 * a {@link Player} and {@link PointGuard} matched the criteria would throw a
 * {@link DuplicateEntryException} because it found records in the hierarchy.
 * After the fix, it correctly returns only the exact class match.
 * </p>
 *
 * @author Jeff Nelson
 */
public class FindUniqueExactClassTest extends RunwayBaseClientServerTest {

    @Test
    public void testFindUniqueOnlySearchesExactClass() {
        // Create a Player with a specific name
        Player player = new Player("UniqueTest", 50);
        runway.save(player);

        // Create a PointGuard (subclass) with the same name
        PointGuard pointGuard = new PointGuard("UniqueTest", 60, 10);
        runway.save(pointGuard);

        // findUnique on Player.class should only find the Player, not the
        // PointGuard. Before the bug fix, this would throw
        // DuplicateEntryException
        // because filterAny was used which searches the hierarchy.
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("UniqueTest");

        Player result = runway.findUnique(Player.class, criteria);

        Assert.assertNotNull(result);
        Assert.assertEquals("UniqueTest", result.name);
        Assert.assertEquals(50, result.score);
        Assert.assertEquals(player.id(), result.id());
    }

    @Test
    public void testFindUniqueReturnsNullWhenNoExactClassMatch() {
        // Create only a PointGuard (subclass), no exact Player
        PointGuard pointGuard = new PointGuard("OnlySubclass", 60, 10);
        runway.save(pointGuard);

        // findUnique on Player.class should return null because there's no
        // exact Player match (only a subclass match exists)
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("OnlySubclass");

        Player result = runway.findUnique(Player.class, criteria);

        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findAnyUnique} returns the sole
     * exact-class match.
     * <p>
     * <strong>Start state:</strong> One {@link Player} saved with a known name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Player} with a known name.</li>
     * <li>Call {@code runway.findAnyUnique(Player.class, criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The saved {@link Player} is returned.
     */
    @Test
    public void testFindAnyUniqueReturnsSoleExactClassMatch() {
        // Create a Player with a specific name
        Player player = new Player("HierarchyTest", 50);
        runway.save(player);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("HierarchyTest");

        Player result = runway.findAnyUnique(Player.class, criteria);

        Assert.assertNotNull(result);
        Assert.assertEquals("HierarchyTest", result.name);
    }
}
