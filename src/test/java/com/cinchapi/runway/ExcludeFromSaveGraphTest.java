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
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link ExcludeFromSaveGraph}.
 *
 * @author Jeff Nelson
 */
public class ExcludeFromSaveGraphTest extends RunwayBaseClientServerTest {

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
     * <strong>Goal:</strong> Verify that a save of a changed holder does not
     * write a {@link Record} it reaches only through an
     * {@link ExcludeFromSaveGraph} field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Change the {@link Referenced Referenced's} label in memory.</li>
     * <li>Change the {@link Holder Holder's} own name in memory.</li>
     * <li>Save the {@link Holder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored {@link Holder} has the new name and
     * the stored {@link Referenced} keeps its original label.
     */
    @Test
    public void testSaveDoesNotPersistReferenceChangesWhenFieldIsExcluded() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        referenced.label = "changed";
        holder.name = "second";
        Assert.assertTrue(holder.save());

        Assert.assertEquals("second",
                runway.load(Holder.class, holder.id()).name);
        Assert.assertEquals("original",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a save of an unchanged holder does not
     * write a {@link Record} it reaches only through an
     * {@link ExcludeFromSaveGraph} field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Change the {@link Referenced Referenced's} label in memory and leave
     * the {@link Holder} untouched.</li>
     * <li>Save the {@link Holder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored {@link Referenced} keeps its
     * original label.
     */
    @Test
    public void testSaveDoesNotPersistReferenceChangesWhenHolderIsUnchanged() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        referenced.label = "changed";
        Assert.assertTrue(holder.save());

        Assert.assertEquals("original",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a save does not write a {@link Record}
     * that a collection-valued {@link ExcludeFromSaveGraph} field holds.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded
     * collection field holds a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Change the {@link Referenced Referenced's} label in memory.</li>
     * <li>Change the {@link Holder Holder's} own name and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored {@link Referenced} keeps its
     * original label.
     */
    @Test
    public void testSaveDoesNotPersistReferenceChangesWhenExcludedFieldIsACollection() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(null);
        holder.excludedMany = Lists.newArrayList(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        referenced.label = "changed";
        holder.name = "second";
        Assert.assertTrue(holder.save());

        Assert.assertEquals("original",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a save does not write a {@link Record}
     * that a {@link DeferredReference} in an {@link ExcludeFromSaveGraph} field
     * holds.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded
     * deferred field holds a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Change the {@link Referenced Referenced's} label in memory.</li>
     * <li>Change the {@link Holder Holder's} own name and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored {@link Referenced} keeps its
     * original label.
     */
    @Test
    public void testSaveDoesNotPersistReferenceChangesWhenExcludedFieldIsDeferred() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(null);
        holder.excludedDeferred = new DeferredReference<>(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        referenced.label = "changed";
        holder.name = "second";
        Assert.assertTrue(holder.save());

        Assert.assertEquals("original",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a save writes the link values of an
     * {@link ExcludeFromSaveGraph} field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Referenced} and an unsaved
     * {@link Holder} whose excluded field links it.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the {@link Holder} alone.</li>
     * <li>Load the {@link Holder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Holder Holder's} excluded
     * field points at the {@link Referenced}.
     */
    @Test
    public void testSaveWritesLinkValuesWhenFieldIsExcluded() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Assert.assertTrue(runway.save(referenced));

        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(holder.save());

        Assert.assertEquals(referenced.id(),
                runway.load(Holder.class, holder.id()).excluded.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a save does not create a
     * {@link Record} it reaches only through an {@link ExcludeFromSaveGraph}
     * field, so the link it writes resolves to nothing.
     * <p>
     * <strong>Start state:</strong> An unsaved {@link Holder} whose excluded
     * field links an unsaved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the {@link Holder} alone.</li>
     * <li>Load the {@link Referenced} by its id.</li>
     * <li>Load the {@link Holder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Referenced} does not exist, and the
     * loaded {@link Holder Holder's} excluded field is {@code null} under the
     * governing {@link ReferenceNotFoundPolicy#SKIP} policy.
     */
    @Test
    public void testSaveDoesNotCreateRecordBehindExcludedField() {
        Referenced referenced = new Referenced();
        referenced.label = "never saved";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(holder.save());

        Assert.assertNull(runway.load(Referenced.class, referenced.id()));
        Assert.assertNull(runway.load(Holder.class, holder.id()).excluded);
    }

    /**
     * <strong>Goal:</strong> Verify that a save writes every changed field of
     * the holder, the {@link ExcludeFromSaveGraph} field included.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links one saved {@link Referenced}, alongside a second saved
     * {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Point the excluded field at the second {@link Referenced} and change
     * the {@link Holder Holder's} own name.</li>
     * <li>Save the {@link Holder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored {@link Holder} has the new name and
     * links the second {@link Referenced}.
     */
    @Test
    public void testSaveWritesOwnChangedFieldsWhenFieldIsExcluded() {
        Referenced first = new Referenced();
        first.label = "first";
        Referenced second = new Referenced();
        second.label = "second";
        Holder holder = new Holder(first);
        holder.name = "before";
        Assert.assertTrue(runway.save(holder, first, second));

        holder.excluded = second;
        holder.name = "after";
        Assert.assertTrue(holder.save());

        Holder stored = runway.load(Holder.class, holder.id());
        Assert.assertEquals("after", stored.name);
        Assert.assertEquals(second.id(), stored.excluded.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} behind an
     * {@link ExcludeFromSaveGraph} field persists normally when the caller
     * saves it itself.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Change the {@link Referenced Referenced's} label in memory.</li>
     * <li>Save the {@link Referenced} directly.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored {@link Referenced} has the new
     * label.
     */
    @Test
    public void testSaveOfReferencePersistsWhenCallerSavesItSeparately() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        Assert.assertTrue(runway.save(holder, referenced));

        referenced.label = "changed";
        Assert.assertTrue(referenced.save());

        Assert.assertEquals("changed",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} behind an
     * {@link ExcludeFromSaveGraph} field does not join the save's conflict
     * footprint.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced} whose stored label another writer then
     * replaced.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Externally replace the stored label of the {@link Referenced}.</li>
     * <li>Change the same label in memory, so a save that reached the
     * {@link Referenced} would overwrite the external write.</li>
     * <li>Change the {@link Holder Holder's} own name and call
     * {@code runway.save(true, holder)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the external label
     * stands.
     */
    @Test
    public void testSaveSucceedsWhenAnotherWriterChangedRecordBehindExcludedField() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        externallyWrite(
                concourse -> concourse.set("label", "theirs", referenced.id()));

        referenced.label = "mine";
        holder.name = "second";
        Assert.assertTrue(runway.save(true, holder));

        Assert.assertEquals("theirs",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link PlainHolder}, whose link
     * field carries no {@link ExcludeFromSaveGraph}, does join the save's
     * conflict footprint, so the coverage above is attributable to the
     * annotation.
     * <p>
     * <strong>Start state:</strong> A saved {@link PlainHolder} whose reference
     * field links a saved {@link Referenced} whose stored label another writer
     * then replaced.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Externally replace the stored label of the {@link Referenced}.</li>
     * <li>Change the same label in memory.</li>
     * <li>Change the {@link PlainHolder PlainHolder's} own name and call
     * {@code runway.save(true, holder)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link StaleDataException} that names the
     * {@link Referenced} is thrown.
     */
    @Test
    public void testSaveFailsWhenAnotherWriterChangedRecordBehindPlainField() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        PlainHolder holder = new PlainHolder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        externallyWrite(
                concourse -> concourse.set("label", "theirs", referenced.id()));

        referenced.label = "mine";
        holder.name = "second";
        try {
            runway.save(true, holder);
            Assert.fail("Expected StaleDataException");
        }
        catch (StaleDataException e) {
            Assert.assertEquals(referenced.id(), e.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a save succeeds when a {@link Record}
     * behind an {@link ExcludeFromSaveGraph} field holds no data in the
     * database, which is the state a concurrent save leaves it in while it
     * stages.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced} whose stored data another writer then
     * erased.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Externally clear every value the {@link Referenced} stores.</li>
     * <li>Change the {@link Referenced Referenced's} label in memory, so a save
     * that reached it would probe for its existence.</li>
     * <li>Change the {@link Holder Holder's} own name and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the {@link Holder} has
     * the new name.
     */
    @Test
    public void testSaveSucceedsWhenRecordBehindExcludedFieldHoldsNoData() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        externallyWrite(concourse -> concourse.clear(referenced.id()));

        referenced.label = "changed";
        holder.name = "second";
        Assert.assertTrue(holder.save());

        Assert.assertEquals("second",
                runway.load(Holder.class, holder.id()).name);
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link PlainHolder}, whose link
     * field carries no {@link ExcludeFromSaveGraph}, is refused when the
     * {@link Record} behind it holds no data, so the coverage above is
     * attributable to the annotation.
     * <p>
     * <strong>Start state:</strong> A saved {@link PlainHolder} whose reference
     * field links a saved {@link Referenced} whose stored data another writer
     * then erased.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Externally clear every value the {@link Referenced} stores.</li>
     * <li>Change the {@link Referenced Referenced's} label in memory.</li>
     * <li>Change the {@link PlainHolder PlainHolder's} own name and save
     * it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DeletedRecordException} that names
     * the {@link Referenced} is thrown.
     */
    @Test
    public void testSaveFailsWhenRecordBehindPlainFieldHoldsNoData() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        PlainHolder holder = new PlainHolder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        externallyWrite(concourse -> concourse.clear(referenced.id()));

        referenced.label = "changed";
        holder.name = "second";
        try {
            holder.save();
            Assert.fail("Expected DeletedRecordException");
        }
        catch (DeletedRecordException e) {
            Assert.assertEquals(referenced.id(), e.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a save within a {@link Transaction}
     * does not write a {@link Record} it reaches only through an
     * {@link ExcludeFromSaveGraph} field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Change the {@link Referenced Referenced's} label in memory.</li>
     * <li>Start a {@link Transaction}, change the {@link Holder Holder's} own
     * name, save the {@link Holder} through the {@link Transaction} and commit
     * it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The commit stores the new {@link Holder} name
     * and the stored {@link Referenced} keeps its original label.
     */
    @Test
    public void testTransactionSaveDoesNotPersistReferenceChangesWhenFieldIsExcluded() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        referenced.label = "changed";
        try (Transaction transaction = runway.startTransaction()) {
            holder.name = "second";
            Assert.assertTrue(transaction.save(holder));
            Assert.assertTrue(transaction.commit());
        }
        Assert.assertEquals("second",
                runway.load(Holder.class, holder.id()).name);
        Assert.assertEquals("original",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * <strong>Goal:</strong> Verify that a save within a {@link Transaction}
     * binds a {@link Record} it reaches only through an
     * {@link ExcludeFromSaveGraph} field, because the exclusion bounds the save
     * graph rather than the scope a {@link Record} belongs to.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and save the {@link Holder} through
     * it.</li>
     * <li>While the {@link Transaction} is open, change the {@link Referenced}
     * and save it through the {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Runway} save is refused with an
     * {@link IllegalStateException}, because the {@link Transaction} owns the
     * {@link Referenced}.
     */
    @Test
    public void testTransactionSaveClaimsRecordBehindExcludedField() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        try (Transaction transaction = runway.startTransaction()) {
            holder.name = "second";
            Assert.assertTrue(transaction.save(holder));

            referenced.label = "changed";
            try {
                runway.save(referenced);
                Assert.fail("Expected IllegalStateException");
            }
            catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("Transaction"));
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Transaction} binds a
     * {@link Record} it loads through an {@link ExcludeFromSaveGraph} field,
     * because the exclusion bounds what a save takes on its own rather than
     * what the caller reads.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} and load the {@link Holder} through
     * it.</li>
     * <li>Change the {@link Referenced} the load resolved and save it through
     * the {@link Runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Runway} save is refused with an
     * {@link IllegalStateException}, because the {@link Transaction} owns
     * everything it loaded.
     */
    @Test
    public void testTransactionLoadClaimsRecordBehindExcludedField() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        try (Transaction transaction = runway.startTransaction()) {
            Holder loaded = transaction.load(Holder.class, holder.id());
            loaded.excluded.label = "changed";
            try {
                runway.save(loaded.excluded);
                Assert.fail("Expected IllegalStateException");
            }
            catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("Transaction"));
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a load resolves a {@link Record}
     * through an {@link ExcludeFromSaveGraph} field.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link Holder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Holder} carries a fully
     * loaded {@link Referenced}.
     */
    @Test
    public void testLoadResolvesReferenceWhenFieldIsExcluded() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        Holder stored = runway.load(Holder.class, holder.id());
        Assert.assertNotNull(stored.excluded);
        Assert.assertEquals("original", stored.excluded.label);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link CascadeDelete} still deletes
     * through a field that also carries {@link ExcludeFromSaveGraph}.
     * <p>
     * <strong>Start state:</strong> A saved {@link CascadingHolder} whose child
     * field links a saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Mark the {@link CascadingHolder} for deletion and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both the {@link CascadingHolder} and the
     * {@link Referenced} are deleted.
     */
    @Test
    public void testDeleteCascadesWhenFieldIsExcluded() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        CascadingHolder holder = new CascadingHolder(referenced);
        Assert.assertTrue(runway.save(holder, referenced));

        holder.deleteOnSave();
        Assert.assertTrue(holder.save());

        Assert.assertNull(runway.load(CascadingHolder.class, holder.id()));
        Assert.assertNull(runway.load(Referenced.class, referenced.id()));
    }

    /**
     * <strong>Goal:</strong> Verify that the boundary belongs to the field and
     * not to the {@link Record} it points at, so a save that also reaches the
     * {@link Record} through an ordinary field writes it there.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} whose excluded field
     * and ordinary field both link the same saved {@link Referenced}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Change the {@link Referenced Referenced's} label in memory.</li>
     * <li>Change the {@link Holder Holder's} own name and save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored {@link Referenced} has the new
     * label.
     */
    @Test
    public void testSaveCascadesWhenAnotherFieldReachesTheSameRecord() {
        Referenced referenced = new Referenced();
        referenced.label = "original";
        Holder holder = new Holder(referenced);
        holder.included = referenced;
        holder.name = "first";
        Assert.assertTrue(runway.save(holder, referenced));

        referenced.label = "changed";
        holder.name = "second";
        Assert.assertTrue(holder.save());

        Assert.assertEquals("changed",
                runway.load(Referenced.class, referenced.id()).label);
    }

    /**
     * A test record that a {@link Holder} links.
     *
     * @author Jeff Nelson
     */
    public static class Referenced extends Record {

        /**
         * The record's label.
         */
        String label;
    }

    /**
     * A test record with one link field that is excluded from the save graph
     * and one that is not.
     *
     * @author Jeff Nelson
     */
    public static class Holder extends Record {

        /**
         * A link that a save writes without visiting the {@link Referenced} it
         * points at.
         */
        @ExcludeFromSaveGraph
        Referenced excluded;

        /**
         * A collection of links that a save writes without visiting the
         * {@link Referenced Referenceds} they point at.
         */
        @ExcludeFromSaveGraph
        List<Referenced> excludedMany;

        /**
         * A deferred link that a save writes without visiting the
         * {@link Referenced} it points at.
         */
        @ExcludeFromSaveGraph
        DeferredReference<Referenced> excludedDeferred;

        /**
         * A link that a save follows.
         */
        Referenced included;

        /**
         * The record's name.
         */
        String name;

        /**
         * Construct a new instance.
         *
         * @param excluded the {@link Referenced} the excluded field links
         */
        public Holder(Referenced excluded) {
            this.excluded = excluded;
        }
    }

    /**
     * A test record whose link field carries no {@link ExcludeFromSaveGraph},
     * so it serves as the control for the {@link Holder Holder's} behavior.
     *
     * @author Jeff Nelson
     */
    public static class PlainHolder extends Record {

        /**
         * A link that a save follows.
         */
        Referenced reference;

        /**
         * The record's name.
         */
        String name;

        /**
         * Construct a new instance.
         *
         * @param reference the {@link Referenced} the link field points at
         */
        public PlainHolder(Referenced reference) {
            this.reference = reference;
        }
    }

    /**
     * A test record whose link field is both excluded from the save graph and
     * marked for cascading deletion.
     *
     * @author Jeff Nelson
     */
    public static class CascadingHolder extends Record {

        /**
         * A link that a save does not follow and a deletion does.
         */
        @CascadeDelete
        @ExcludeFromSaveGraph
        Referenced child;

        /**
         * Construct a new instance.
         *
         * @param child the {@link Referenced} the link field points at
         */
        public CascadingHolder(Referenced child) {
            this.child = child;
        }
    }
}
