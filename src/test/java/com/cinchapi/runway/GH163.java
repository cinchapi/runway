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

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.MergeStrategy.Strategy;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/163">GH-163</a>: a save
 * must write only the difference between a {@link Record Record's} current
 * state and the state it last loaded or saved, so concurrent changes to fields
 * the instance never touched survive the save.
 *
 * @author Jeff Nelson
 */
public class GH163 extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that saving a stale instance keeps a link
     * that another instance added to a collection field after the stale
     * instance was loaded.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team} with one
     * {@link Member}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Team} twice, as {@code stale} and {@code fresh}.</li>
     * <li>Add a second {@link Member} through {@code fresh} and save it.</li>
     * <li>Change only {@code name} on {@code stale} and save it.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} has both
     * {@link Member Members} and the {@code name} change from {@code stale}.
     */
    @Test
    public void testUnrelatedSaveKeepsLinkAddedToCollectionByAnotherInstance() {
        Member a = new Member("a");
        Team team = new Team("Original", 10);
        team.members.add(a);
        Assert.assertTrue(runway.save(team, a));

        Team stale = runway.load(Team.class, team.id());
        Team fresh = runway.load(Team.class, team.id());
        Member b = new Member("b");
        fresh.members.add(b);
        Assert.assertTrue(runway.save(fresh, b));

        stale.name = "Renamed";
        Assert.assertTrue(stale.save());

        Team loaded = runway.load(Team.class, team.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertEquals(2, loaded.members.size());
        Assert.assertTrue(loaded.members.stream()
                .anyMatch(member -> member.id() == b.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that saving a stale instance keeps a scalar
     * link that another instance set after the stale instance was loaded.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team} with no
     * {@code captain}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Team} twice, as {@code stale} and {@code fresh}.</li>
     * <li>Set {@code captain} through {@code fresh} and save it.</li>
     * <li>Change only {@code name} on {@code stale} and save it.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} keeps the
     * {@code captain} that {@code fresh} set.
     */
    @Test
    public void testUnrelatedSaveKeepsScalarLinkSetByAnotherInstance() {
        Team team = new Team("Original", 10);
        Assert.assertTrue(runway.save(team));

        Team stale = runway.load(Team.class, team.id());
        Team fresh = runway.load(Team.class, team.id());
        Member captain = new Member("captain");
        fresh.captain = captain;
        Assert.assertTrue(runway.save(fresh, captain));

        stale.name = "Renamed";
        Assert.assertTrue(stale.save());

        Team loaded = runway.load(Team.class, team.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertNotNull(loaded.captain);
        Assert.assertEquals(captain.id(), loaded.captain.id());
    }

    /**
     * <strong>Goal:</strong> Verify that saving a stale instance does not
     * resurrect a collection element that another instance removed after the
     * stale instance was loaded.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team} with two {@link Member
     * Members}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Team} twice, as {@code stale} and {@code fresh}.</li>
     * <li>Remove one {@link Member} through {@code fresh} and save it.</li>
     * <li>Change only {@code name} on {@code stale} and save it.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The removed {@link Member} stays removed even
     * though {@code stale} still held it in memory.
     */
    @Test
    public void testUnrelatedSaveDoesNotResurrectRemovedElement() {
        Member a = new Member("a");
        Member b = new Member("b");
        Team team = new Team("Original", 10);
        team.members.add(a);
        team.members.add(b);
        Assert.assertTrue(runway.save(team, a, b));

        Team stale = runway.load(Team.class, team.id());
        Team fresh = runway.load(Team.class, team.id());
        fresh.members.removeIf(member -> member.id() == b.id());
        Assert.assertTrue(fresh.save());

        stale.name = "Renamed";
        Assert.assertTrue(stale.save());

        Team loaded = runway.load(Team.class, team.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertEquals(1, loaded.members.size());
        Assert.assertTrue(loaded.members.stream()
                .anyMatch(member -> member.id() == a.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that saving a stale instance keeps a scalar
     * value that another instance changed after the stale instance was loaded,
     * when the stale instance never touched that field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Team} twice, as {@code stale} and {@code fresh}.</li>
     * <li>Change {@code wins} through {@code fresh} and save it.</li>
     * <li>Change only {@code name} on {@code stale} and save it.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} has the {@code wins}
     * value from {@code fresh} and the {@code name} value from {@code stale}.
     */
    @Test
    public void testUnrelatedSaveKeepsConcurrentScalarChange() {
        Team team = new Team("Original", 10);
        Assert.assertTrue(runway.save(team));

        Team stale = runway.load(Team.class, team.id());
        Team fresh = runway.load(Team.class, team.id());
        fresh.wins = 50;
        Assert.assertTrue(fresh.save());

        stale.name = "Renamed";
        Assert.assertTrue(stale.save());

        Team loaded = runway.load(Team.class, team.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertEquals(50, loaded.wins);
    }

    /**
     * <strong>Goal:</strong> Verify that a save writes only the keys whose
     * values changed and leaves the revision history of untouched keys alone.
     * <p>
     * <strong>Start state:</strong> A saved and reloaded {@link Team}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record the audit sizes of {@code name}, {@code wins} and
     * {@code members}.</li>
     * <li>Change only {@code name} and save.</li>
     * <li>Compare the audit sizes after the save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code name} history grows while the
     * {@code wins} and {@code members} histories are unchanged.
     */
    @Test
    public void testDirtySaveWritesOnlyChangedKeys() {
        Member a = new Member("a");
        Team team = new Team("Original", 10);
        team.members.add(a);
        Assert.assertTrue(runway.save(team, a));

        Team loaded = runway.load(Team.class, team.id());
        int names = client.audit("name", team.id()).size();
        int wins = client.audit("wins", team.id()).size();
        int members = client.audit("members", team.id()).size();
        loaded.name = "Renamed";
        Assert.assertTrue(loaded.save());

        Assert.assertTrue(client.audit("name", team.id()).size() > names);
        Assert.assertEquals(wins, client.audit("wins", team.id()).size());
        Assert.assertEquals(members, client.audit("members", team.id()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that emptying a collection removes only the
     * elements the instance knew about, so an element that another instance
     * added concurrently survives.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team} with one
     * {@link Member}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Team} twice, as {@code stale} and {@code fresh}.</li>
     * <li>Add a second {@link Member} through {@code fresh} and save it.</li>
     * <li>Clear the {@code members} collection on {@code stale} and save
     * it.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} contains exactly the
     * {@link Member} that {@code fresh} added.
     */
    @Test
    public void testEmptyingCollectionRemovesOnlyKnownElements() {
        Member a = new Member("a");
        Team team = new Team("Original", 10);
        team.members.add(a);
        Assert.assertTrue(runway.save(team, a));

        Team stale = runway.load(Team.class, team.id());
        Team fresh = runway.load(Team.class, team.id());
        Member b = new Member("b");
        fresh.members.add(b);
        Assert.assertTrue(runway.save(fresh, b));

        stale.members.clear();
        Assert.assertTrue(stale.save());

        Team loaded = runway.load(Team.class, team.id());
        Assert.assertEquals(1, loaded.members.size());
        Assert.assertTrue(loaded.members.stream()
                .anyMatch(member -> member.id() == b.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that assigning {@code null} to a scalar
     * field deletes the stored value.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team} with a
     * {@code captain}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Reload the {@link Team}.</li>
     * <li>Assign {@code null} to {@code captain} and save.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} has no
     * {@code captain}.
     */
    @Test
    public void testNullingScalarClearsStoredValue() {
        Member captain = new Member("captain");
        Team team = new Team("Original", 10);
        team.captain = captain;
        Assert.assertTrue(runway.save(team, captain));

        Team loaded = runway.load(Team.class, team.id());
        loaded.captain = null;
        Assert.assertTrue(loaded.save());

        loaded = runway.load(Team.class, team.id());
        Assert.assertNull(loaded.captain);
    }

    /**
     * <strong>Goal:</strong> Verify that realm changes made through separate
     * instances merge instead of the later save erasing the earlier one.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team} in no explicit realm.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Team} twice, as {@code a} and {@code b}.</li>
     * <li>Add realm {@code "east"} through {@code a} and save it.</li>
     * <li>Add realm {@code "west"} through {@code b} and save it.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} is in both realms.
     */
    @Test
    public void testRealmChangesFromSeparateInstancesBothSurvive() {
        Team team = new Team("Original", 10);
        Assert.assertTrue(runway.save(team));

        Team a = runway.load(Team.class, team.id());
        Team b = runway.load(Team.class, team.id());
        a.addRealm("east");
        Assert.assertTrue(a.save());
        b.addRealm("west");
        Assert.assertTrue(b.save());

        Team loaded = runway.load(Team.class, team.id());
        Set<String> realms = loaded.realms();
        Assert.assertTrue(realms.contains("east"));
        Assert.assertTrue(realms.contains("west"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} that was never saved
     * persists all of its state on its first save.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Team} with a {@code name}, {@code wins}, a
     * {@code captain} and one {@link Member}.</li>
     * <li>Save and reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} carries every value
     * the instance held at save time.
     */
    @Test
    public void testNewRecordPersistsAllState() {
        Member a = new Member("a");
        Member captain = new Member("captain");
        Team team = new Team("Original", 10);
        team.members.add(a);
        team.captain = captain;
        Assert.assertTrue(runway.save(team, a, captain));

        Team loaded = runway.load(Team.class, team.id());
        Assert.assertEquals("Original", loaded.name);
        Assert.assertEquals(10, loaded.wins);
        Assert.assertNotNull(loaded.captain);
        Assert.assertEquals(captain.id(), loaded.captain.id());
        Assert.assertEquals(1, loaded.members.size());
        Assert.assertTrue(loaded.members.stream()
                .anyMatch(member -> member.id() == a.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that saving a stale instance through a
     * {@link Transaction} keeps a link that another instance added to a
     * collection field after the stale instance was loaded.
     * <p>
     * <strong>Start state:</strong> A saved {@link Team} with one
     * {@link Member}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Team} twice, as {@code stale} and {@code fresh}.</li>
     * <li>Add a second {@link Member} through {@code fresh} and save it.</li>
     * <li>Change only {@code name} on {@code stale}, save it through a
     * {@link Transaction} and commit.</li>
     * <li>Reload the {@link Team}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Team} has both
     * {@link Member Members} and the {@code name} change from {@code stale}.
     */
    @Test
    public void testStaleSaveWithinTransactionKeepsConcurrentlyAddedElement() {
        Member a = new Member("a");
        Team team = new Team("Original", 10);
        team.members.add(a);
        Assert.assertTrue(runway.save(team, a));

        Team stale = runway.load(Team.class, team.id());
        Team fresh = runway.load(Team.class, team.id());
        Member b = new Member("b");
        fresh.members.add(b);
        Assert.assertTrue(runway.save(fresh, b));

        try (Transaction transaction = runway.startTransaction()) {
            stale.name = "Renamed";
            Assert.assertTrue(transaction.save(stale));
            Assert.assertTrue(transaction.commit());
        }

        Team loaded = runway.load(Team.class, team.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertEquals(2, loaded.members.size());
        Assert.assertTrue(loaded.members.stream()
                .anyMatch(member -> member.id() == b.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Strategy#OVERWRITE
     * OVERWRITE} collection field writes its full current state on save, so a
     * concurrent change to the field is overwritten.
     * <p>
     * <strong>Start state:</strong> A saved {@link Pipeline} with two
     * {@code steps}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Pipeline} twice, as {@code stale} and
     * {@code fresh}.</li>
     * <li>Add a step through {@code fresh} and save it.</li>
     * <li>Change only {@code name} on {@code stale} and save it.</li>
     * <li>Reload the {@link Pipeline}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Pipeline} holds exactly
     * the {@code steps} that {@code stale} held, so the step {@code fresh}
     * added is gone.
     */
    @Test
    public void testOverwriteCollectionWritesFullStateOnSave() {
        Pipeline pipeline = new Pipeline("Original");
        pipeline.steps.add("extract");
        pipeline.steps.add("transform");
        Assert.assertTrue(runway.save(pipeline));

        Pipeline stale = runway.load(Pipeline.class, pipeline.id());
        Pipeline fresh = runway.load(Pipeline.class, pipeline.id());
        fresh.steps.add("load");
        Assert.assertTrue(fresh.save());

        stale.name = "Renamed";
        Assert.assertTrue(stale.save());

        Pipeline loaded = runway.load(Pipeline.class, pipeline.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertEquals(Sets.newHashSet("extract", "transform"),
                Sets.newHashSet(loaded.steps));
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Strategy#OVERWRITE
     * OVERWRITE} scalar field asserts its in-memory value whenever the
     * {@link Record} saves, so stored drift from writes made outside of Runway
     * is repaired.
     * <p>
     * <strong>Start state:</strong> A saved {@link Pipeline}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Reload the {@link Pipeline}.</li>
     * <li>Change the stored {@code label} directly through the database
     * client.</li>
     * <li>Change only {@code name} on the loaded instance and save it.</li>
     * <li>Reload the {@link Pipeline}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Pipeline Pipeline's}
     * {@code label} matches the instance, so the direct write is overwritten.
     */
    @Test
    public void testOverwriteScalarAssertsValueWheneverRecordSaves() {
        Pipeline pipeline = new Pipeline("Original");
        pipeline.label = "stable";
        Assert.assertTrue(runway.save(pipeline));

        Pipeline loaded = runway.load(Pipeline.class, pipeline.id());
        client.set("label", "drifted", pipeline.id());
        loaded.name = "Renamed";
        Assert.assertTrue(loaded.save());

        loaded = runway.load(Pipeline.class, pipeline.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertEquals("stable", loaded.label);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Strategy#OVERWRITE
     * OVERWRITE} field does not make a clean {@link Record} dirty, so a save of
     * an unchanged record writes nothing.
     * <p>
     * <strong>Start state:</strong> A saved and reloaded {@link Pipeline}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Record the audit size of {@code steps}.</li>
     * <li>Save the reloaded {@link Pipeline} without changing it.</li>
     * <li>Compare the audit size after the save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Pipeline} reports no
     * unsaved changes and the {@code steps} history is unchanged.
     */
    @Test
    public void testOverwriteFieldDoesNotMakeCleanRecordDirty() {
        Pipeline pipeline = new Pipeline("Original");
        pipeline.steps.add("extract");
        Assert.assertTrue(runway.save(pipeline));

        Pipeline loaded = runway.load(Pipeline.class, pipeline.id());
        Assert.assertFalse(loaded.hasUnsavedChanges());
        int steps = client.audit("steps", pipeline.id()).size();
        Assert.assertTrue(loaded.save());

        Assert.assertEquals(steps, client.audit("steps", pipeline.id()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that a failed save does not consume a
     * pending realm change, so the change stages again on the next save.
     * <p>
     * <strong>Start state:</strong> A saved {@link Project} with a valid
     * {@code title}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Reload the {@link Project}.</li>
     * <li>Add realm {@code "east"}, assign {@code null} to the required
     * {@code title} and save; the save fails.</li>
     * <li>Assign a valid {@code title} and save again.</li>
     * <li>Reload the {@link Project}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Project} is in realm
     * {@code "east"} and carries the valid {@code title}.
     */
    @Test
    public void testFailedSaveDoesNotConsumePendingRealmChange() {
        Project project = new Project("Original");
        Assert.assertTrue(runway.save(project));

        Project loaded = runway.load(Project.class, project.id());
        loaded.addRealm("east");
        loaded.title = null;
        Assert.assertFalse(loaded.save());

        loaded.title = "Restored";
        Assert.assertTrue(loaded.save());

        loaded = runway.load(Project.class, project.id());
        Assert.assertEquals("Restored", loaded.title);
        Assert.assertTrue(loaded.realms().contains("east"));
    }

    /**
     * <strong>Goal:</strong> Verify that a realm change made inside the
     * {@link Record#beforeSave() beforeSave} hook persists with the save in
     * which the hook runs.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Journal}, whose {@code beforeSave} hook adds realm
     * {@code "audited"}, and save it.</li>
     * <li>Reload the {@link Journal}, change {@code name} and save it.</li>
     * <li>Reload the {@link Journal}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reloaded {@link Journal} is in realm
     * {@code "audited"} and carries the {@code name} change.
     */
    @Test
    public void testRealmAddedInBeforeSaveHookPersists() {
        Journal journal = new Journal("Original");
        Assert.assertTrue(runway.save(journal));

        Journal loaded = runway.load(Journal.class, journal.id());
        loaded.name = "Renamed";
        Assert.assertTrue(loaded.save());

        loaded = runway.load(Journal.class, journal.id());
        Assert.assertEquals("Renamed", loaded.name);
        Assert.assertTrue(loaded.realms().contains("audited"));
    }

    /**
     * A {@link Team} exercises a scalar field, a scalar link, and a collection
     * of links in one {@link Record}.
     *
     * @author Jeff Nelson
     */
    public static class Team extends Record {

        /**
         * The team name.
         */
        public String name;

        /**
         * The number of wins.
         */
        public int wins;

        /**
         * The {@link Member} that captains the team.
         */
        public Member captain;

        /**
         * The {@link Member Members} of the team.
         */
        public Set<Member> members = Sets.newLinkedHashSet();

        /**
         * Construct a new instance.
         *
         * @param name the {@link #name} value
         * @param wins the {@link #wins} value
         */
        public Team(String name, int wins) {
            this.name = name;
            this.wins = wins;
        }

    }

    /**
     * A {@link Member} is a link target for {@link Team} fields.
     *
     * @author Jeff Nelson
     */
    public static class Member extends Record {

        /**
         * The member name.
         */
        public String name;

        /**
         * Construct a new instance.
         *
         * @param name the {@link #name} value
         */
        public Member(String name) {
            this.name = name;
        }

    }

    /**
     * A {@link Pipeline} exercises the {@link Strategy#OVERWRITE OVERWRITE}
     * merge strategy on a collection field and a scalar field.
     *
     * @author Jeff Nelson
     */
    public static class Pipeline extends Record {

        /**
         * The pipeline name.
         */
        public String name;

        /**
         * A label that this instance always asserts when it saves.
         */
        @MergeStrategy(Strategy.OVERWRITE)
        public String label;

        /**
         * The ordered steps, always written in their entirety.
         */
        @MergeStrategy(Strategy.OVERWRITE)
        public List<String> steps = Lists.newArrayList();

        /**
         * Construct a new instance.
         *
         * @param name the {@link #name} value
         */
        public Pipeline(String name) {
            this.name = name;
        }

    }

    /**
     * A {@link Project} carries a {@link Required} field so a save can be made
     * to fail on demand.
     *
     * @author Jeff Nelson
     */
    public static class Project extends Record {

        /**
         * The required title.
         */
        @Required
        public String title;

        /**
         * Construct a new instance.
         *
         * @param title the {@link #title} value
         */
        public Project(String title) {
            this.title = title;
        }

    }

    /**
     * A {@link Journal} adds itself to a bookkeeping realm in
     * {@link #beforeSave()}.
     *
     * @author Jeff Nelson
     */
    public static class Journal extends Record {

        /**
         * The journal name.
         */
        public String name;

        /**
         * Construct a new instance.
         *
         * @param name the {@link #name} value
         */
        public Journal(String name) {
            this.name = name;
        }

        @Override
        protected void beforeSave() {
            addRealm("audited");
        }

    }

}
