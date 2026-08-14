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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.runway.MergeStrategy.Strategy;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link Runway#save(boolean, Record...)} with
 * {@code preventStaleWrites} enabled. Each test runs once against the
 * {@link com.cinchapi.runway.db.BatchSaver BatchSaver} and once against the
 * {@link com.cinchapi.runway.db.IncrementalSaver IncrementalSaver} so the
 * stale-write contract is verified on both save paths regardless of the
 * connected server's Command-API capability.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class PreventStaleWriteTest extends RunwayBaseClientServerTest {

    /**
     * Return the parameter matrix that drives each test once per save path.
     *
     * @return one row per {@link com.cinchapi.runway.db.Saver Saver}
     *         implementation
     */
    @Parameters(name = "bulkCommands={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to drive saves through the
     *            {@link com.cinchapi.runway.db.BatchSaver BatchSaver};
     *            {@code false} for the
     *            {@link com.cinchapi.runway.db.IncrementalSaver
     *            IncrementalSaver}
     */
    public PreventStaleWriteTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * Wait for the wall clock to advance one millisecond.
     * <p>
     * A {@link Record Record's} checkpoint comes from the test JVM's clock and
     * a revision comes from the server's. The two clocks agree only to the
     * millisecond, so two events in the same millisecond cannot be ordered
     * ({@code GH-123}). Waiting gives an external write a millisecond of its
     * own, which removes the ambiguity.
     * </p>
     */
    private static void tick() {
        long millis = System.currentTimeMillis();
        while (System.currentTimeMillis() == millis) {
            Thread.yield();
        }
    }

    /**
     * Apply {@code write} directly to the database, as a writer outside of this
     * {@link Runway} would. The write gets a millisecond of its own, so the
     * test can order events around it.
     *
     * @param write the write to apply
     */
    private void externallyWrite(Consumer<Concourse> write) {
        tick();
        Concourse concourse = runway.connections.request();
        try {
            write.accept(concourse);
        }
        finally {
            runway.connections.release(concourse);
        }
        tick();
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} throws a {@link StaleDataException} when another writer
     * changed the same realm that the save writes.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved in one realm whose
     * stored realm membership another writer then replaced.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} that belongs to realm "a".</li>
     * <li>Externally replace the stored realm membership with "c".</li>
     * <li>Add realm "c" in memory and call
     * {@code runway.save(true, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} that names the
     * {@link TUser} is thrown, because the save writes realm "c" and that realm
     * changed externally.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenTheSameRealmChangedExternally() {
        TUser user = new TUser("sasha");
        user.addRealm("a");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("_realms", "c", user.id()));

        user.addRealm("c");
        try {
            runway.save(true, user);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(user.id(), e.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} succeeds when another writer changed a realm that the
     * save does not write.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved in one realm whose
     * stored realm membership another writer then replaced.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} that belongs to realm "a".</li>
     * <li>Externally replace the stored realm membership with "c".</li>
     * <li>Add realm "b" in memory and call
     * {@code runway.save(true, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and the stored
     * realm membership holds both the external realm and the new one.
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenAnotherRealmChangedExternally() {
        TUser user = new TUser("sanjay");
        user.addRealm("a");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("_realms", "c", user.id()));

        user.addRealm("b");
        Assert.assertTrue(runway.save(true, user));

        TUser loaded = runway.load(TUser.class, user.id());
        Assert.assertEquals(ImmutableSet.of("b", "c"), loaded.realms());
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

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

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

        externallyWrite(
                connection -> connection.set("name", "refreshed", user.id()));

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

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

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

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

        user.name = "overwrite";
        Assert.assertTrue(runway.save(user));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} throws a {@link StaleDataException} when the save writes
     * a value of a linked {@link Record} that was externally modified.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} and a {@link TTenant}
     * linked to it, both saved. The {@link TUser TUser's} name is then
     * externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "hank".</li>
     * <li>Create and save a {@link TTenant} linked to that {@link TUser}.</li>
     * <li>Externally modify the {@link TUser TUser's} name in the
     * database.</li>
     * <li>Modify both the {@link TUser TUser's} name and the {@link TTenant
     * TTenant's} name in memory, so the save writes the linked
     * {@link TUser}.</li>
     * <li>Call {@code runway.save(true, tenant)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} that names the
     * {@link TUser} is thrown, because the save writes the linked {@link TUser
     * TUser's} name and that value changed externally.
     */
    @Test
    public void testPreventStaleWriteDetectsStaleLinkedRecordThatTheSaveWrites() {
        TUser user = new TUser("hank");
        TTenant tenant = new TTenant(user);
        Assert.assertTrue(runway.save(tenant));

        externallyWrite(connection -> connection.set("name", "external_hank",
                user.id()));

        user.name = "local_hank";
        tenant.name = "modified_tenant";
        try {
            runway.save(true, tenant);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(user.id(), e.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} succeeds when a linked {@link Record} that the save
     * writes nothing to was externally modified.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} and a {@link TTenant}
     * linked to it, both saved. The {@link TUser TUser's} name is then
     * externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "mona".</li>
     * <li>Create and save a {@link TTenant} linked to that {@link TUser}.</li>
     * <li>Externally modify the {@link TUser TUser's} name in the
     * database.</li>
     * <li>Modify only the {@link TTenant TTenant's} name in memory.</li>
     * <li>Call {@code runway.save(true, tenant)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true}, the
     * {@link TTenant TTenant's} new name persists, and the external
     * {@link TUser} name survives.
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenLinkedRecordChangeIsNotWritten() {
        TUser user = new TUser("mona");
        TTenant tenant = new TTenant(user);
        Assert.assertTrue(runway.save(tenant));

        externallyWrite(connection -> connection.set("name", "external_mona",
                user.id()));

        tenant.name = "modified_tenant";
        Assert.assertTrue(runway.save(true, tenant));

        TTenant loadedTenant = runway.load(TTenant.class, tenant.id());
        Assert.assertEquals("modified_tenant", loadedTenant.name);
        TUser loadedUser = runway.load(TUser.class, user.id());
        Assert.assertEquals("external_mona", loadedUser.name);
    }

    /**
     * <strong>Goal:</strong> Verify that two instances of one {@link Record}
     * that change independent fields can both save with
     * {@code preventStaleWrites} enabled, and that both values persist.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} and a second instance
     * of it loaded from the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "kate".</li>
     * <li>Load a second instance of the same {@link Record}.</li>
     * <li>Change the first instance's name and save it.</li>
     * <li>Change the second instance's bio and call
     * {@code runway.save(true, second)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves return {@code true} and the stored
     * {@link Record} carries the new name and the new bio.
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenExternalChangeTouchesAnotherField() {
        TUser first = new TUser("kate");
        Assert.assertTrue(runway.save(first));

        TUser second = runway.load(TUser.class, first.id());
        first.name = "kate_renamed";
        Assert.assertTrue(runway.save(first));

        second.bio = "engineer";
        Assert.assertTrue(runway.save(true, second));

        TUser loaded = runway.load(TUser.class, first.id());
        Assert.assertEquals("kate_renamed", loaded.name);
        Assert.assertEquals("engineer", loaded.bio);
    }

    /**
     * <strong>Goal:</strong> Verify that a value another writer changed and
     * changed back does not fail the save, because the stored value is the one
     * the instance loaded.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} whose name an
     * external writer changed and then restored.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "nadia".</li>
     * <li>Externally change the name and then externally change it back.</li>
     * <li>Change the name in memory and call
     * {@code runway.save(true, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and the
     * in-memory name persists.
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenExternalChangeWasReverted() {
        TUser user = new TUser("nadia");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("name", "detour", user.id()));
        externallyWrite(
                connection -> connection.set("name", "nadia", user.id()));

        user.name = "nadia_final";
        Assert.assertTrue(runway.save(true, user));

        TUser loaded = runway.load(TUser.class, user.id());
        Assert.assertEquals("nadia_final", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that two instances of one {@link Record}
     * that change the same field cannot both save when the second save enables
     * {@code preventStaleWrites}.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} and a second instance
     * of it loaded from the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "liam".</li>
     * <li>Load a second instance of the same {@link Record}.</li>
     * <li>Change the first instance's name and save it.</li>
     * <li>Change the second instance's name and call
     * {@code runway.save(true, second)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown and the
     * first instance's name survives.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenExternalChangeTouchesTheSameField() {
        TUser first = new TUser("liam");
        Assert.assertTrue(runway.save(first));

        TUser second = runway.load(TUser.class, first.id());
        first.name = "liam_first";
        Assert.assertTrue(runway.save(first));

        second.name = "liam_second";
        try {
            runway.save(true, second);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(first.id(), e.id());
        }

        TUser loaded = runway.load(TUser.class, first.id());
        Assert.assertEquals("liam_first", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that an external change to a collection
     * field that the save does not write does not fail a save that writes a
     * different collection field.
     * <p>
     * <strong>Start state:</strong> A saved {@link TDoc} whose reviewers were
     * externally extended.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TDoc} with one tag.</li>
     * <li>Externally add a reviewer directly in the database.</li>
     * <li>Add a second tag in memory.</li>
     * <li>Call {@code runway.save(true, doc)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and the stored
     * {@link TDoc} holds both tags and the external reviewer.
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenExternalChangeTouchesAnotherCollection() {
        TDoc doc = new TDoc();
        doc.tags.add("draft");
        Assert.assertTrue(runway.save(doc));

        externallyWrite(
                connection -> connection.add("reviewers", "quinn", doc.id()));

        doc.tags.add("reviewed");
        Assert.assertTrue(runway.save(true, doc));

        TDoc loaded = runway.load(TDoc.class, doc.id());
        Assert.assertEquals(ImmutableSet.of("draft", "reviewed"), loaded.tags);
        Assert.assertEquals(ImmutableSet.of("quinn"), loaded.reviewers);
    }

    /**
     * <strong>Goal:</strong> Verify that an external change to an element of a
     * collection field that the save does not write does not fail a save that
     * writes a different element of that same field.
     * <p>
     * <strong>Start state:</strong> A saved {@link TDoc} whose tags were
     * externally extended.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TDoc} with one tag.</li>
     * <li>Externally add a second tag directly in the database.</li>
     * <li>Add a third tag in memory.</li>
     * <li>Call {@code runway.save(true, doc)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and the stored
     * {@link TDoc} holds the original, the external, and the new tag.
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenExternalChangeTouchesAnotherElement() {
        TDoc doc = new TDoc();
        doc.tags.add("draft");
        Assert.assertTrue(runway.save(doc));

        externallyWrite(
                connection -> connection.add("tags", "external", doc.id()));

        doc.tags.add("reviewed");
        Assert.assertTrue(runway.save(true, doc));

        TDoc loaded = runway.load(TDoc.class, doc.id());
        Assert.assertEquals(ImmutableSet.of("draft", "external", "reviewed"),
                loaded.tags);
    }

    /**
     * <strong>Goal:</strong> Verify that an external change to the element of a
     * collection field that the save writes fails the save.
     * <p>
     * <strong>Start state:</strong> A saved {@link TDoc} one of whose tags
     * another writer removed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TDoc} with two tags.</li>
     * <li>Externally remove one of them directly in the database.</li>
     * <li>Remove that same tag in memory and add another.</li>
     * <li>Call {@code runway.save(true, doc)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown and
     * neither in-memory change persists.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenExternalChangeTouchesTheSameElement() {
        TDoc doc = new TDoc();
        doc.tags.add("draft");
        doc.tags.add("stale");
        Assert.assertTrue(runway.save(doc));

        externallyWrite(
                connection -> connection.remove("tags", "stale", doc.id()));

        doc.tags.remove("stale");
        doc.tags.add("reviewed");
        try {
            runway.save(true, doc);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(doc.id(), e.id());
        }

        TDoc loaded = runway.load(TDoc.class, doc.id());
        Assert.assertEquals(ImmutableSet.of("draft"), loaded.tags);
    }

    /**
     * <strong>Goal:</strong> Verify that a field annotated
     * {@link MergeStrategy}{@code (OVERWRITE)} always belongs to its
     * {@link Record Record's} write set, so an external change to it fails a
     * save that changed only another field.
     * <p>
     * <strong>Start state:</strong> A saved {@link TProfile} whose motto was
     * externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TProfile} with a handle and a motto.</li>
     * <li>Externally modify the motto directly in the database.</li>
     * <li>Change only the handle in memory.</li>
     * <li>Call {@code runway.save(true, profile)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown because
     * the save would overwrite the externally modified motto.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenOverwriteFieldChangedExternally() {
        TProfile profile = new TProfile("opal", "carpe diem");
        Assert.assertTrue(runway.save(profile));

        externallyWrite(connection -> connection.set("motto", "external motto",
                profile.id()));

        profile.handle = "opal_renamed";
        try {
            runway.save(true, profile);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(profile.id(), e.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a save that attributes an author
     * succeeds when another writer changed the stored author, because
     * authorship is a marker that every save re-asserts.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} whose stored author
     * another writer then set.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an author {@link TUser} and a subject {@link TUser}.</li>
     * <li>Externally link the subject's author directly in the database.</li>
     * <li>Attribute the subject to the author, change the bio, and call
     * {@code runway.save(true, subject)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and the new bio
     * persists.
     */
    @Test
    public void testPreventStaleWriteSucceedsWhenAuthorChangedExternally() {
        TUser author = new TUser("uma");
        Assert.assertTrue(runway.save(author));
        TUser subject = new TUser("umberto");
        Assert.assertTrue(runway.save(subject));

        externallyWrite(connection -> connection.set("_author",
                Link.to(author.id()), subject.id()));

        subject.bio = "engineer";
        Reflection.set("_author", author, subject); // (authorized)
        Assert.assertTrue(runway.save(true, subject));

        TUser loaded = runway.load(TUser.class, subject.id());
        Assert.assertEquals("engineer", loaded.bio);
    }

    /**
     * <strong>Goal:</strong> Verify that a save whose
     * {@link Record#beforeSave() beforeSave} hook restores the only changed
     * field still judges the {@link MergeStrategy}{@code (OVERWRITE)} fields
     * that the save writes.
     * <p>
     * <strong>Start state:</strong> A saved {@link TReverting} whose motto was
     * externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TReverting} with a title and a motto.</li>
     * <li>Externally modify the motto directly in the database.</li>
     * <li>Change the title in memory, which the hook restores, and call
     * {@code runway.save(true, record)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown and the
     * external motto survives.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenHookRestoresTheOnlyChange() {
        TReverting record = new TReverting("alpha", "carpe diem");
        Assert.assertTrue(runway.save(record));

        externallyWrite(connection -> connection.set("motto", "external motto",
                record.id()));

        record.title = "beta";
        try {
            runway.save(true, record);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(record.id(), e.id());
        }

        TReverting loaded = runway.load(TReverting.class, record.id());
        Assert.assertEquals("external motto", loaded.motto);
    }

    /**
     * <strong>Goal:</strong> Verify that a field that only
     * {@link Record#beforeSave() beforeSave} writes belongs to its
     * {@link Record Record's} write set, so an external change to it fails the
     * save.
     * <p>
     * <strong>Start state:</strong> A saved {@link TSlugged} whose slug was
     * externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TSlugged} named "alpha", whose hook derives the
     * slug.</li>
     * <li>Externally modify the slug directly in the database.</li>
     * <li>Change only the name in memory.</li>
     * <li>Call {@code runway.save(true, record)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown and the
     * external slug survives.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenHookWritesExternallyChangedField() {
        TSlugged record = new TSlugged("alpha");
        Assert.assertTrue(runway.save(record));

        externallyWrite(
                connection -> connection.set("slug", "external", record.id()));

        record.name = "beta";
        try {
            runway.save(true, record);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(record.id(), e.id());
        }

        TSlugged loaded = runway.load(TSlugged.class, record.id());
        Assert.assertEquals("external", loaded.slug);
    }

    /**
     * <strong>Goal:</strong> Verify that a save of a {@link Record} staged for
     * deletion fails when any value of that {@link Record} changed externally,
     * including a value that no field of the {@link Record} declares.
     * <p>
     * <strong>Start state:</strong> A saved {@link TUser} that an external
     * writer extended with a key the class does not declare.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "rowan".</li>
     * <li>Externally add a value under a key the {@link TUser} does not
     * declare.</li>
     * <li>Stage the {@link TUser} for deletion and call
     * {@code runway.save(true, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown and the
     * {@link TUser} still exists.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenDeletingRecordChangedOnAnotherKey() {
        TUser user = new TUser("rowan");
        Assert.assertTrue(runway.save(user));

        externallyWrite(connection -> connection.add("undeclared", "external",
                user.id()));

        user.deleteOnSave();
        try {
            runway.save(true, user);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(user.id(), e.id());
        }

        Assert.assertNotNull(runway.load(TUser.class, user.id()));
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

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

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
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} throws a {@link StaleDataException} identifying the
     * stale {@link Record} when saving multiple root {@link Record Records} and
     * only one of them is stale.
     * <p>
     * <strong>Start state:</strong> Two {@link TUser TUsers} that have been
     * saved. One is then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link TUser TUsers}: "jack" and "jill".</li>
     * <li>Externally modify "jack" in the database.</li>
     * <li>Modify both {@link TUser TUsers} in memory.</li>
     * <li>Call {@code runway.save(true, jack, jill)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown whose
     * {@link StaleDataException#id() id()} matches "jack". Neither
     * {@link Record} is persisted with the in-memory changes.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenOneOfMultipleRootsIsStale() {
        TUser jack = new TUser("jack");
        TUser jill = new TUser("jill");
        Assert.assertTrue(runway.save(jack, jill));

        externallyWrite(connection -> connection.set("name", "external_jack",
                jack.id()));

        jack.name = "local_jack";
        jill.name = "local_jill";
        try {
            runway.save(true, jack, jill);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(jack.id(), e.id());
        }

        TUser loadedJill = runway.load(TUser.class, jill.id());
        Assert.assertEquals("jill", loadedJill.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} succeeds for a brand-new {@link Record} that has never
     * been saved before, since a new {@link Record} cannot have stale data.
     * <p>
     * <strong>Start state:</strong> A freshly constructed {@link TUser} that
     * has never been saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a new {@link TUser} with name "newbie".</li>
     * <li>Call {@code runway.save(true, user)}.</li>
     * <li>Load the {@link TUser} from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} and the loaded
     * {@link TUser TUser's} name equals "newbie".
     */
    @Test
    public void testPreventStaleWriteSucceedsForNewRecord() {
        TUser user = new TUser("newbie");
        Assert.assertTrue(runway.save(true, user));

        TUser loaded = runway.load(TUser.class, user.id());
        Assert.assertNotNull(loaded);
        Assert.assertEquals("newbie", loaded.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#save(boolean)
     * save(true)} on a pinned {@link Record} correctly delegates to
     * {@link Runway#save(boolean, Record...)} and throws a
     * {@link StaleDataException} when the {@link Record} is stale.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved via {@link Runway}
     * and then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "pinned".</li>
     * <li>Externally modify the name in the database.</li>
     * <li>Modify the in-memory name and call {@code user.save(true)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown because
     * the pinned {@link Record Record's} {@link Record#save(boolean)} delegates
     * to {@link Runway#save(boolean, Record...)} with
     * {@code preventStaleWrite = true}.
     */
    @Test(expected = StaleDataException.class)
    public void testRecordSaveWithPreventStaleWriteOnPinnedRecord() {
        TUser user = new TUser("pinned");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

        user.name = "local";
        user.save(true);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Runway#save(boolean, Record...)
     * save(true, ...)} throws a {@link StaleDataException} when a
     * {@link Record} that has been marked for deletion via
     * {@link Record#deleteOnSave()} is also stale.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has been saved and
     * then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "doomed".</li>
     * <li>Externally modify the name in the database.</li>
     * <li>Mark the {@link TUser} for deletion via
     * {@link Record#deleteOnSave()}.</li>
     * <li>Call {@code runway.save(true, user)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown because
     * the stale check occurs before the deletion is processed. The
     * {@link Record} should still exist in the database.
     */
    @Test
    public void testPreventStaleWriteThrowsWhenDeletingStaleRecord() {
        TUser user = new TUser("doomed");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

        user.deleteOnSave();
        try {
            runway.save(true, user);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(user.id(), e.id());
        }

        TUser loaded = runway.load(TUser.class, user.id());
        Assert.assertNotNull("Record should still exist after failed delete",
                loaded);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#save(boolean)
     * save(false)} on a pinned {@link Record} does not throw a
     * {@link StaleDataException} even when the {@link Record} is stale, because
     * the stale check is disabled.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved via {@link Runway}
     * and then externally modified in the database.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with name "pinned_no_check".</li>
     * <li>Externally modify the name in the database.</li>
     * <li>Modify the in-memory name and call {@code user.save(false)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true} because the
     * stale check is disabled.
     */
    @Test
    public void testRecordSaveWithoutPreventStaleWriteOnPinnedRecord() {
        TUser user = new TUser("pinned_no_check");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

        user.name = "overwrite";
        Assert.assertTrue(user.save(false));
    }

    /**
     * <strong>Goal:</strong> Verify that a declared value is checked by a save
     * that does not prevent stale writes.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved through
     * {@link Runway} whose bio is then externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Externally modify the bio in the database.</li>
     * <li>Declare {@code bio}, modify the in-memory name, and save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown.
     */
    @Test(expected = StaleDataException.class)
    public void testDeclaredValueIsCheckedBySaveThatDoesNotPreventStaleWrites() {
        TUser user = new TUser("verify_plain_save");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("bio", "external", user.id()));

        user.verifyOnSave("bio");
        user.name = "updated";
        runway.save(user);
    }

    /**
     * <strong>Goal:</strong> Verify that a save succeeds when the database
     * still holds the declared value that the {@link Record} last saw.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved through
     * {@link Runway} whose bio no other writer changes.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Externally modify a field that is not touched.</li>
     * <li>Declare {@code bio}, modify the in-memory bio, and save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true}.
     */
    @Test
    public void testDeclaredValueAllowsSaveWhenTheDatabaseStillHoldsIt() {
        TUser user = new TUser("verify_fresh");
        user.bio = "original";
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("name", "external", user.id()));

        user.verifyOnSave("bio");
        user.bio = "updated";
        Assert.assertTrue(runway.save(user));
    }

    /**
     * <strong>Goal:</strong> Verify that a declared value is checked even when
     * the save writes nothing.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved through
     * {@link Runway} whose bio is then externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Externally modify the bio in the database.</li>
     * <li>Declare {@code bio} and save without modifying anything.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown.
     */
    @Test(expected = StaleDataException.class)
    public void testDeclaredValueIsCheckedWhenTheSaveWritesNothing() {
        TUser user = new TUser("verify_no_write");
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("bio", "external", user.id()));

        user.verifyOnSave("bio");
        runway.save(user);
    }

    /**
     * <strong>Goal:</strong> Verify that declaring a name that is not an
     * intrinsic field is refused.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has never been saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code verifyOnSave} with a name that no field carries.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testVerifyOnSaveRefusesAKeyThatIsNotAnIntrinsicField() {
        TUser user = new TUser("verify_bad_key");
        user.verifyOnSave("nonexistent");
    }

    /**
     * <strong>Goal:</strong> Verify that a declaration has no effect on a
     * {@link Record} that the database does not yet hold.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that has never been saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Declare {@code bio} on an unsaved {@link TUser} and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true}.
     */
    @Test
    public void testVerifyOnSaveHasNoEffectBeforeTheRecordIsPersisted() {
        TUser user = new TUser("verify_new_record");
        user.verifyOnSave("bio");
        Assert.assertTrue(runway.save(user));
    }

    /**
     * <strong>Goal:</strong> Verify that a value this instance wrote through an
     * atomic operation does not fail its own later save.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved through
     * {@link Runway} whose bio this instance then exchanges.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser} with a bio.</li>
     * <li>Declare {@code bio} and exchange it through this instance.</li>
     * <li>Modify the in-memory name and save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save returns {@code true}, because the
     * database still stores what this instance last saw for {@code bio}.
     */
    @Test
    public void testDeclaredValueAllowsSaveAfterThisInstanceAtomicallyWroteIt() {
        TUser user = new TUser("verify_own_atomic_write");
        user.bio = "original";
        Assert.assertTrue(runway.save(user));

        user.verifyOnSave("bio");
        Assert.assertTrue(user.exchange("bio", "exchanged"));

        user.name = "updated";
        Assert.assertTrue(runway.save(user));
    }

    /**
     * <strong>Goal:</strong> Verify that an element another writer adds to a
     * declared collection fails the save.
     * <p>
     * <strong>Start state:</strong> A {@link TDoc} saved through {@link Runway}
     * whose stored tags another writer then adds to.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TDoc} that carries one tag.</li>
     * <li>Externally add a second tag, leaving the first in place.</li>
     * <li>Declare {@code tags}, add a reviewer, and save.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown, even
     * though every tag the {@link TDoc} read is still stored.
     */
    @Test(expected = StaleDataException.class)
    public void testDeclaredCollectionFailsSaveWhenAnotherWriterAddsAnElement() {
        TDoc doc = new TDoc();
        doc.tags.add("draft");
        Assert.assertTrue(runway.save(doc));

        externallyWrite(
                connection -> connection.add("tags", "urgent", doc.id()));

        doc.verifyOnSave("tags");
        doc.reviewers.add("alice");
        runway.save(doc);
    }

    /**
     * <strong>Goal:</strong> Verify that a declaration does not carry past the
     * save that commits it.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} that declared its bio and
     * saved since.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}, declare {@code bio}, and save again.</li>
     * <li>Externally modify the bio in the database.</li>
     * <li>Modify the in-memory name and save a third time.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The third save returns {@code true}, because
     * the second save spent the declaration.
     */
    @Test
    public void testDeclaredValueDoesNotCarryPastTheSaveThatCommitsIt() {
        TUser user = new TUser("verify_one_shot");
        user.bio = "original";
        Assert.assertTrue(runway.save(user));

        user.verifyOnSave("bio");
        user.name = "first";
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("bio", "external", user.id()));

        user.name = "second";
        Assert.assertTrue(runway.save(user));
    }

    /**
     * <strong>Goal:</strong> Verify that a declaration stays in place when the
     * save that carried it does not commit.
     * <p>
     * <strong>Start state:</strong> A {@link TUser} saved through
     * {@link Runway} whose bio is then externally modified.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TUser}.</li>
     * <li>Externally modify the bio in the database.</li>
     * <li>Declare {@code bio} and save, catching the failure.</li>
     * <li>Modify the in-memory name and save again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} is thrown by the
     * second save as well.
     */
    @Test(expected = StaleDataException.class)
    public void testDeclaredValueStaysInPlaceWhenTheSaveDoesNotCommit() {
        TUser user = new TUser("verify_retained_on_failure");
        user.bio = "original";
        Assert.assertTrue(runway.save(user));

        externallyWrite(
                connection -> connection.set("bio", "external", user.id()));

        user.verifyOnSave("bio");
        try {
            runway.save(user);
            Assert.fail("The save should have failed on the declared value");
        }
        catch (StaleDataException e) {/* expected */}

        user.name = "updated";
        runway.save(user);
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
         * The user's biography, an independent field that a writer can change
         * without touching the {@link #name}.
         */
        String bio;

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
     * A test record with two independent collection fields.
     *
     * @author Jeff Nelson
     */
    public static class TDoc extends Record {

        /**
         * The document's tags.
         */
        Set<String> tags = new LinkedHashSet<>();

        /**
         * The document's reviewers.
         */
        Set<String> reviewers = new LinkedHashSet<>();
    }

    /**
     * A test record with a field that writes its full state on every save of
     * the record.
     *
     * @author Jeff Nelson
     */
    public static class TProfile extends Record {

        /**
         * The profile's handle.
         */
        String handle;

        /**
         * The profile's motto, which every save of a {@link TProfile}
         * overwrites regardless of whether this instance changed it.
         */
        @MergeStrategy(Strategy.OVERWRITE)
        String motto;

        /**
         * Construct a new instance.
         *
         * @param handle the profile's handle
         * @param motto the profile's motto
         */
        public TProfile(String handle, String motto) {
            this.handle = handle;
            this.motto = motto;
        }
    }

    /**
     * A test record whose {@link #beforeSave()} hook restores its title, so a
     * caller's change to that field leaves no unsaved change behind.
     *
     * @author Jeff Nelson
     */
    public static class TReverting extends Record {

        /**
         * The record's title, which {@link #beforeSave()} restores.
         */
        String title;

        /**
         * The record's motto, which every save of a {@link TReverting}
         * overwrites regardless of whether this instance changed it.
         */
        @MergeStrategy(Strategy.OVERWRITE)
        String motto;

        /**
         * Construct a new instance.
         *
         * @param title the record's title
         * @param motto the record's motto
         */
        public TReverting(String title, String motto) {
            this.title = title;
            this.motto = motto;
        }

        @Override
        protected void beforeSave() {
            title = "alpha";
        }
    }

    /**
     * A test record whose {@link #beforeSave()} hook derives a field that no
     * caller sets.
     *
     * @author Jeff Nelson
     */
    public static class TSlugged extends Record {

        /**
         * The record's name.
         */
        String name;

        /**
         * The slug that {@link #beforeSave()} derives from the {@link #name}.
         */
        String slug;

        /**
         * Construct a new instance.
         *
         * @param name the record's name
         */
        public TSlugged(String name) {
            this.name = name;
        }

        @Override
        protected void beforeSave() {
            slug = name + "-slug";
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
