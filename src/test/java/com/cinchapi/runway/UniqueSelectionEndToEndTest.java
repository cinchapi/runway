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
 * End-to-end tests that verify {@link UniqueSelection} dispatch, execution, and
 * result handling through a live {@link Runway} instance.
 *
 * @author Jeff Nelson
 */
public class UniqueSelectionEndToEndTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} with
     * criteria returns the single matching {@link Record}.
     * <p>
     * <strong>Start state:</strong> One {@link Player} saved with a unique
     * name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Player} with a known name.</li>
     * <li>Fetch via
     * {@code Selection.ofUnique(Player.class).where(criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The fetched {@link Player} is the same record
     * that was saved.
     */
    @Test
    public void testOfUniqueReturnsSingleMatch() {
        Player player = new Player("Solo", 99);
        runway.save(player);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Solo");
        Player result = runway
                .fetch(Selection.ofUnique(Player.class).where(criteria));
        Assert.assertNotNull(result);
        Assert.assertEquals(player.id(), result.id());
        Assert.assertEquals("Solo", result.name);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} returns
     * {@code null} when no {@link Record} matches.
     * <p>
     * <strong>Start state:</strong> No matching records exist.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Fetch via {@code Selection.ofUnique(Player.class)} with criteria that
     * match nothing.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testOfUniqueReturnsNullWhenNoMatch() {
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Ghost");
        Player result = runway
                .fetch(Selection.ofUnique(Player.class).where(criteria));
        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} throws
     * {@link DuplicateEntryException} when more than one {@link Record}
     * matches.
     * <p>
     * <strong>Start state:</strong> Two {@link Player Players} saved with the
     * same name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Player Players} with the same name.</li>
     * <li>Fetch via
     * {@code Selection.ofUnique(Player.class).where(criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown.
     */
    @Test(expected = DuplicateEntryException.class)
    public void testOfUniqueThrowsOnDuplicate() {
        runway.save(new Player("Dupe", 10));
        runway.save(new Player("Dupe", 20));

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Dupe");
        runway.fetch(Selection.ofUnique(Player.class).where(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Selection#ofAnyUnique(Class)}
     * searches the class hierarchy and returns the single match when only one
     * exists.
     * <p>
     * <strong>Start state:</strong> One {@link PointGuard} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link PointGuard} with a known name.</li>
     * <li>Fetch via
     * {@code Selection.ofAnyUnique(Player.class).where(criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link PointGuard} is returned because
     * {@code ofAnyUnique} includes descendants.
     */
    @Test
    public void testOfAnyUniqueSearchesHierarchy() {
        PointGuard pg = new PointGuard("HierarchyOnly", 80, 12);
        runway.save(pg);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("HierarchyOnly");
        Player result = runway
                .fetch(Selection.ofAnyUnique(Player.class).where(criteria));
        Assert.assertNotNull(result);
        Assert.assertEquals(pg.id(), result.id());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Selection#ofAnyUnique(Class)}
     * throws {@link DuplicateEntryException} when both a parent and descendant
     * match.
     * <p>
     * <strong>Start state:</strong> One {@link Player} and one
     * {@link PointGuard} saved with the same name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Player} and a {@link PointGuard} with the same
     * name.</li>
     * <li>Fetch via
     * {@code Selection.ofAnyUnique(Player.class).where(criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown
     * because two records exist in the hierarchy.
     */
    @Test(expected = DuplicateEntryException.class)
    public void testOfAnyUniqueThrowsWhenMultipleInHierarchy() {
        runway.save(new Player("Shared", 50));
        runway.save(new PointGuard("Shared", 60, 10));

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Shared");
        runway.fetch(Selection.ofAnyUnique(Player.class).where(criteria));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Selection#ofUnique(Class)}
     * excludes descendants, so a subclass match does not count.
     * <p>
     * <strong>Start state:</strong> Only a {@link PointGuard} saved (no exact
     * {@link Player}).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link PointGuard} with a known name.</li>
     * <li>Fetch via
     * {@code Selection.ofUnique(Player.class).where(criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} because there is no
     * exact {@link Player} match.
     */
    @Test
    public void testOfUniqueExcludesDescendants() {
        runway.save(new PointGuard("SubOnly", 70, 8));

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("SubOnly");
        Player result = runway
                .fetch(Selection.ofUnique(Player.class).where(criteria));
        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that the chained {@code .unique()} method
     * on {@link Selection.QueryBuilder} produces the same result as the
     * top-level {@link Selection#ofUnique(Class)} factory.
     * <p>
     * <strong>Start state:</strong> One {@link Player} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Player}.</li>
     * <li>Fetch via
     * {@code Selection.of(Player.class).where(criteria).unique()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The single matching {@link Player} is
     * returned.
     */
    @Test
    public void testChainedUniqueReturnsSingleMatch() {
        Player player = new Player("Chained", 42);
        runway.save(player);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Chained");
        Player result = runway
                .fetch(Selection.of(Player.class).where(criteria).unique());
        Assert.assertNotNull(result);
        Assert.assertEquals(player.id(), result.id());
    }

    /**
     * <strong>Goal:</strong> Verify that the existing
     * {@link DatabaseInterface#findUnique(Class, Criteria)} method still works
     * correctly after being refactored to delegate to the {@link Selection}
     * API.
     * <p>
     * <strong>Start state:</strong> One {@link Player} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Player}.</li>
     * <li>Call {@code runway.findUnique(Player.class, criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The single matching {@link Player} is
     * returned, confirming backward compatibility.
     */
    @Test
    public void testFindUniqueStillWorksAfterRefactor() {
        Player player = new Player("Legacy", 33);
        runway.save(player);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Legacy");
        Player result = runway.findUnique(Player.class, criteria);
        Assert.assertNotNull(result);
        Assert.assertEquals(player.id(), result.id());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link DatabaseInterface#findAnyUnique(Class, Criteria)} still works
     * correctly after being refactored to delegate to the {@link Selection}
     * API.
     * <p>
     * <strong>Start state:</strong> One {@link Player} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Player}.</li>
     * <li>Call {@code runway.findAnyUnique(Player.class, criteria)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The single matching {@link Player} is
     * returned, confirming backward compatibility.
     */
    @Test
    public void testFindAnyUniqueStillWorksAfterRefactor() {
        Player player = new Player("LegacyAny", 44);
        runway.save(player);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("LegacyAny");
        Player result = runway.findAnyUnique(Player.class, criteria);
        Assert.assertNotNull(result);
        Assert.assertEquals(player.id(), result.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} without
     * criteria returns the single {@link Record} when only one of that exact
     * class exists.
     * <p>
     * <strong>Start state:</strong> One {@link Player} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a single {@link Player}.</li>
     * <li>Fetch via {@code Selection.ofUnique(Player.class)} with no
     * {@code where} clause.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The single {@link Player} is returned.
     */
    @Test
    public void testOfUniqueWithNoCriteriaReturnsSingleRecord() {
        Player player = new Player("OnlyOne", 77);
        runway.save(player);

        Player result = runway.fetch(Selection.ofUnique(Player.class));
        Assert.assertNotNull(result);
        Assert.assertEquals(player.id(), result.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} without
     * criteria throws {@link DuplicateEntryException} when more than one
     * {@link Record} of that class exists.
     * <p>
     * <strong>Start state:</strong> Two {@link Player Players} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Player Players}.</li>
     * <li>Fetch via {@code Selection.ofUnique(Player.class)} with no
     * {@code where} clause.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown.
     */
    @Test(expected = DuplicateEntryException.class)
    public void testOfUniqueWithNoCriteriaThrowsOnMultiple() {
        runway.save(new Player("First", 10));
        runway.save(new Player("Second", 20));

        runway.fetch(Selection.ofUnique(Player.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} without
     * criteria returns {@code null} when no {@link Record Records} of that
     * class exist.
     * <p>
     * <strong>Start state:</strong> No {@link Player Players} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Fetch via {@code Selection.ofUnique(Player.class)} with no
     * {@code where} clause and no saved data.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testOfUniqueWithNoCriteriaReturnsNullWhenEmpty() {
        Player result = runway.fetch(Selection.ofUnique(Player.class));
        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} with a
     * client-side filter returns the single matching {@link Record} after
     * filtering.
     * <p>
     * <strong>Start state:</strong> Two {@link Player Players} saved with
     * different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Player Players} with different scores.</li>
     * <li>Fetch via {@code Selection.ofUnique(Player.class)} with a filter that
     * accepts only one of them.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The single matching {@link Player} is
     * returned.
     */
    @Test
    public void testOfUniqueWithFilterReturnsSingleMatch() {
        Player low = new Player("Low", 5);
        Player high = new Player("High", 95);
        runway.save(low);
        runway.save(high);

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.REGEX).value(".*");
        Player result = runway.fetch(Selection.ofUnique(Player.class)
                .where(criteria).filter(p -> p.score > 50));
        Assert.assertNotNull(result);
        Assert.assertEquals(high.id(), result.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link UniqueSelection} with a
     * client-side filter returns {@code null} when the filter excludes all
     * matches.
     * <p>
     * <strong>Start state:</strong> One {@link Player} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Player} with a known score.</li>
     * <li>Fetch via {@code Selection.ofUnique(Player.class)} with a filter that
     * rejects the saved record.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null}.
     */
    @Test
    public void testOfUniqueWithFilterReturnsNullWhenFilteredOut() {
        runway.save(new Player("Filtered", 10));

        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Filtered");
        Player result = runway.fetch(Selection.ofUnique(Player.class)
                .where(criteria).filter(p -> p.score > 99));
        Assert.assertNull(result);
    }
}
