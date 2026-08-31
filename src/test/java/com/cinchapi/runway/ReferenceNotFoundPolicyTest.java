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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for {@link ReferenceNotFoundPolicy} and the
 * {@link ReferenceNotFound} declaration that overrides it.
 *
 * @author Jeff Nelson
 */
public class ReferenceNotFoundPolicyTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that the default policy skips a stale
     * reference and leaves it in the database.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} that references a
     * saved {@link Target} through a field that declares no policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load the {@link Holder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Holder} loads, its field is
     * {@code null}, and the database still stores the reference.
     */
    @Test
    public void testSkipResolvesToNothingAndKeepsStoredReference() {
        Target target = new Target();
        Holder holder = new Holder();
        holder.target = target;
        Assert.assertTrue(runway.save(target, holder));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        Holder loaded = runway.load(Holder.class, holder.id());
        Assert.assertNotNull(loaded);
        Assert.assertNull(loaded.target);
        Assert.assertFalse(client.select("target", holder.id()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link ReferenceNotFoundPolicy#REPAIR}
     * skips a stale reference and deletes it from the database.
     * <p>
     * <strong>Start state:</strong> A saved {@link RepairingHolder} that
     * references a saved {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load the {@link RepairingHolder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The holder loads, its field is {@code null},
     * and the database no longer stores the reference.
     */
    @Test
    public void testRepairRemovesStoredReference() {
        Target target = new Target();
        RepairingHolder holder = new RepairingHolder();
        holder.target = target;
        Assert.assertTrue(runway.save(target, holder));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        RepairingHolder loaded = runway.load(RepairingHolder.class,
                holder.id());
        Assert.assertNotNull(loaded);
        Assert.assertNull(loaded.target);
        Assert.assertTrue(client.select("target", holder.id()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link ReferenceNotFoundPolicy#ERROR}
     * fails the load of the housing record.
     * <p>
     * <strong>Start state:</strong> A saved {@link StrictHolder} that
     * references a saved {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load the {@link StrictHolder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load throws a
     * {@link ReferenceNotFoundException} that names the field and the
     * referenced record.
     */
    @Test
    public void testErrorFailsTheLoadOfTheHoldingRecord() {
        Target target = new Target();
        StrictHolder holder = new StrictHolder();
        holder.target = target;
        Assert.assertTrue(runway.save(target, holder));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        try {
            runway.load(StrictHolder.class, holder.id());
            Assert.fail("The load must not succeed");
        }
        catch (ReferenceNotFoundException e) {
            Assert.assertEquals("target", e.key());
            Assert.assertEquals(target.id(), e.target());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a declared policy overrides the one
     * that the {@link Runway} applies.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} whose policy is
     * {@link ReferenceNotFoundPolicy#ERROR}, holding a saved {@link Holder}
     * that declares no policy and a saved {@link RepairingHolder} that declares
     * {@link ReferenceNotFoundPolicy#REPAIR}, both referencing one
     * {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load each holder.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The holder that declares no policy fails its
     * load under the database's policy, and the one that declares
     * {@link ReferenceNotFoundPolicy#REPAIR} skips the stale reference and
     * deletes it.
     */
    @Test
    public void testDeclaredPolicyOverridesTheDatabasePolicy()
            throws Exception {
        runway.close();
        runway = runwayBuilder()
                .referenceNotFoundPolicy(ReferenceNotFoundPolicy.ERROR).build();

        Target target = new Target();
        Holder undeclared = new Holder();
        undeclared.target = target;
        RepairingHolder declared = new RepairingHolder();
        declared.target = target;
        Assert.assertTrue(runway.save(target, undeclared, declared));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        try {
            runway.load(Holder.class, undeclared.id());
            Assert.fail("The undeclared holder must follow the database");
        }
        catch (ReferenceNotFoundException e) {
            Assert.assertEquals("target", e.key());
        }

        RepairingHolder loaded = runway.load(RepairingHolder.class,
                declared.id());
        Assert.assertNotNull(loaded);
        Assert.assertNull(loaded.target);
    }

    /**
     * <strong>Goal:</strong> Verify that a collection omits a stale reference
     * rather than holding a {@code null} element in its place.
     * <p>
     * <strong>Start state:</strong> A saved {@link CollectionHolder} that
     * references two saved {@link Target Targets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete one {@link Target}.</li>
     * <li>Load the {@link CollectionHolder}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The holder loads and its collection contains
     * only the surviving {@link Target}.
     */
    @Test
    public void testSkipOmitsTheElementFromACollection() {
        Target kept = new Target();
        Target removed = new Target();
        CollectionHolder holder = new CollectionHolder();
        holder.targets = ImmutableList.of(kept, removed);
        Assert.assertTrue(runway.save(kept, removed, holder));

        removed.deleteOnSave();
        Assert.assertTrue(removed.save());

        CollectionHolder loaded = runway.load(CollectionHolder.class,
                holder.id());
        Assert.assertNotNull(loaded);
        Assert.assertEquals(1, loaded.targets.size());
        Assert.assertEquals(kept.id(), loaded.targets.get(0).id());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link ReferenceNotFoundPolicy#REPAIR}
     * deletes a stale reference from the record that actually houses it, even
     * when the load reached that record through another one and resolves its
     * fields under a navigation prefix.
     * <p>
     * <strong>Start state:</strong> A saved {@link Outer} that references a
     * saved {@link Inner}, which in turn references two saved {@link Target
     * Targets} through a field that declares
     * {@link ReferenceNotFoundPolicy#REPAIR}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete one {@link Target}.</li>
     * <li>Load the {@link Outer}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link Outer} loads, the {@link Inner
     * Inner's} collection holds only the surviving {@link Target}, and the
     * database stores only the surviving reference on the {@link Inner} rather
     * than on the {@link Outer}.
     */
    @Test
    public void testRepairRemovesAStaleElementFromANestedCollection() {
        Target kept = new Target();
        Target removed = new Target();
        Inner inner = new Inner();
        inner.targets = ImmutableList.of(kept, removed);
        Outer outer = new Outer();
        outer.inner = inner;
        Assert.assertTrue(runway.save(kept, removed, inner, outer));

        removed.deleteOnSave();
        Assert.assertTrue(removed.save());

        Outer loaded = runway.load(Outer.class, outer.id());
        Assert.assertNotNull(loaded);
        Assert.assertNotNull(loaded.inner);
        Assert.assertEquals(1, loaded.inner.targets.size());
        Assert.assertEquals(kept.id(), loaded.inner.targets.get(0).id());
        Assert.assertEquals(1, client.select("targets", inner.id()).size());
    }

    /**
     * <strong>Goal:</strong> Verify that a stale {@link DeferredReference}
     * answers {@code null} under the default policy, without failing the load
     * of the housing record.
     * <p>
     * <strong>Start state:</strong> A saved {@link DeferredHolder} that
     * references a saved {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load the {@link DeferredHolder}.</li>
     * <li>Access the reference.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The holder loads and the access answers
     * {@code null}.
     */
    @Test
    public void testSkipAnswersNullForADeferredReference() {
        Target target = new Target();
        DeferredHolder holder = new DeferredHolder();
        holder.target = new DeferredReference<>(target);
        Assert.assertTrue(runway.save(target, holder));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        DeferredHolder loaded = runway.load(DeferredHolder.class, holder.id());
        Assert.assertNotNull(loaded);
        Assert.assertNull(loaded.target.get());
        Assert.assertFalse(client.select("target", holder.id()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link ReferenceNotFoundPolicy#REPAIR}
     * deletes a stale {@link DeferredReference} when the reference is accessed,
     * not when the housing record loads.
     * <p>
     * <strong>Start state:</strong> A saved {@link RepairingDeferredHolder}
     * that references a saved {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load the {@link RepairingDeferredHolder}.</li>
     * <li>Access the reference.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The stored reference survives the load and is
     * gone after the access, which answers {@code null}.
     */
    @Test
    public void testRepairDeletesADeferredReferenceOnAccess() {
        Target target = new Target();
        RepairingDeferredHolder holder = new RepairingDeferredHolder();
        holder.target = new DeferredReference<>(target);
        Assert.assertTrue(runway.save(target, holder));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        RepairingDeferredHolder loaded = runway
                .load(RepairingDeferredHolder.class, holder.id());
        Assert.assertNotNull(loaded);
        Assert.assertFalse(client.select("target", holder.id()).isEmpty());

        Assert.assertNull(loaded.target.get());
        Assert.assertTrue(client.select("target", holder.id()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link ReferenceNotFoundPolicy#ERROR}
     * on a {@link DeferredReference} field reports a stale reference when the
     * reference is accessed rather than when the housing record loads.
     * <p>
     * <strong>Start state:</strong> A saved {@link StrictDeferredHolder} that
     * references a saved {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load the {@link StrictDeferredHolder}.</li>
     * <li>Access the reference.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load succeeds, and the access throws a
     * {@link ReferenceNotFoundException} that names the field and the
     * referenced record.
     */
    @Test
    public void testErrorFailsADeferredReferenceOnAccess() {
        Target target = new Target();
        StrictDeferredHolder holder = new StrictDeferredHolder();
        holder.target = new DeferredReference<>(target);
        Assert.assertTrue(runway.save(target, holder));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        StrictDeferredHolder loaded = runway.load(StrictDeferredHolder.class,
                holder.id());
        Assert.assertNotNull(loaded);

        try {
            loaded.target.get();
            Assert.fail("The access must not succeed");
        }
        catch (ReferenceNotFoundException e) {
            Assert.assertEquals("target", e.key());
            Assert.assertEquals(target.id(), e.target());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a field holding a
     * {@link java.util.Collection} of {@link DeferredReference
     * DeferredReferences} applies its policy to each element on its own, so a
     * live element still resolves alongside a stale one.
     * <p>
     * <strong>Start state:</strong> A saved
     * {@link StrictDeferredCollectionHolder} that references two saved
     * {@link Target Targets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete one {@link Target}.</li>
     * <li>Load the holder.</li>
     * <li>Access each reference.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load succeeds, the collection still holds
     * both references, the surviving one resolves, and the stale one throws a
     * {@link ReferenceNotFoundException}.
     */
    @Test
    public void testErrorAppliesPerElementInADeferredCollection() {
        Target kept = new Target();
        Target removed = new Target();
        StrictDeferredCollectionHolder holder = new StrictDeferredCollectionHolder();
        holder.targets = ImmutableList.of(new DeferredReference<>(kept),
                new DeferredReference<>(removed));
        Assert.assertTrue(runway.save(kept, removed, holder));

        removed.deleteOnSave();
        Assert.assertTrue(removed.save());

        StrictDeferredCollectionHolder loaded = runway
                .load(StrictDeferredCollectionHolder.class, holder.id());
        Assert.assertNotNull(loaded);
        Assert.assertEquals(2, loaded.targets.size());

        int resolved = 0;
        int refused = 0;
        for (DeferredReference<Target> reference : loaded.targets) {
            try {
                Assert.assertEquals(kept.id(), reference.get().id());
                ++resolved;
            }
            catch (ReferenceNotFoundException e) {
                Assert.assertEquals("targets", e.key());
                Assert.assertEquals(removed.id(), e.target());
                ++refused;
            }
        }
        Assert.assertEquals(1, resolved);
        Assert.assertEquals(1, refused);
    }

    /**
     * <strong>Goal:</strong> Verify that a stale reference resolves the same
     * way whether or not the load pre-selects the referenced record's data.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} that references a
     * saved {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load the {@link Holder} with pre-selection enabled.</li>
     * <li>Disable pre-selection and load the {@link Holder} again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both loads succeed and leave the field
     * {@code null}.
     */
    @Test
    public void testPolicyAppliesRegardlessOfPreSelection() {
        Target target = new Target();
        Holder holder = new Holder();
        holder.target = target;
        Assert.assertTrue(runway.save(target, holder));

        target.deleteOnSave();
        Assert.assertTrue(target.save());

        Holder preSelected = runway.load(Holder.class, holder.id());
        Assert.assertNotNull(preSelected);
        Assert.assertNull(preSelected.target);

        Reflection.set("supportsPreSelectLinkedRecords", false, runway);
        Holder plain = runway.load(Holder.class, holder.id());
        Assert.assertNotNull(plain);
        Assert.assertNull(plain.target);
    }

    /**
     * A {@link Record} that references {@link Target Targets} through a
     * collection that declares no {@link ReferenceNotFound} policy.
     */
    class CollectionHolder extends Record {

        /**
         * The referenced {@link Target Targets}.
         */
        public List<Target> targets;
    }

    /**
     * A {@link Record} that references {@link Target Targets} through a
     * collection that declares {@link ReferenceNotFoundPolicy#REPAIR}, and that
     * a load reaches through an {@link Outer} rather than directly.
     */
    class Inner extends Record {

        /**
         * The referenced {@link Target Targets}.
         */
        @ReferenceNotFound(ReferenceNotFoundPolicy.REPAIR)
        public List<Target> targets;
    }

    /**
     * A {@link Record} that references an {@link Inner}, so a load resolves the
     * {@link Inner Inner's} fields under a navigation prefix.
     */
    class Outer extends Record {

        /**
         * The referenced {@link Inner}.
         */
        public Inner inner;
    }

    /**
     * A {@link Record} that defers a reference to a {@link Target} through a
     * field that declares no {@link ReferenceNotFound} policy.
     */
    class DeferredHolder extends Record {

        /**
         * The deferred {@link Target}.
         */
        public DeferredReference<Target> target;
    }

    /**
     * A {@link Record} that defers a reference to a {@link Target} through a
     * field that declares {@link ReferenceNotFoundPolicy#REPAIR}.
     */
    class RepairingDeferredHolder extends Record {

        /**
         * The deferred {@link Target}.
         */
        @ReferenceNotFound(ReferenceNotFoundPolicy.REPAIR)
        public DeferredReference<Target> target;
    }

    /**
     * A {@link Record} that defers references to {@link Target Targets} through
     * a collection that declares {@link ReferenceNotFoundPolicy#ERROR}.
     */
    class StrictDeferredCollectionHolder extends Record {

        /**
         * The deferred {@link Target Targets}.
         */
        @ReferenceNotFound(ReferenceNotFoundPolicy.ERROR)
        public List<DeferredReference<Target>> targets;
    }

    /**
     * A {@link Record} that defers a reference to a {@link Target} through a
     * field that declares {@link ReferenceNotFoundPolicy#ERROR}.
     */
    class StrictDeferredHolder extends Record {

        /**
         * The deferred {@link Target}.
         */
        @ReferenceNotFound(ReferenceNotFoundPolicy.ERROR)
        public DeferredReference<Target> target;
    }

    /**
     * A {@link Record} that references a {@link Target} through a field that
     * declares no {@link ReferenceNotFound} policy.
     */
    class Holder extends Record {

        /**
         * The referenced {@link Target}.
         */
        public Target target;
    }

    /**
     * A {@link Record} that references a {@link Target} through a field that
     * declares {@link ReferenceNotFoundPolicy#REPAIR}.
     */
    class RepairingHolder extends Record {

        /**
         * The referenced {@link Target}.
         */
        @ReferenceNotFound(ReferenceNotFoundPolicy.REPAIR)
        public Target target;
    }

    /**
     * A {@link Record} that references a {@link Target} through a field that
     * declares {@link ReferenceNotFoundPolicy#ERROR}.
     */
    class StrictHolder extends Record {

        /**
         * The referenced {@link Target}.
         */
        @ReferenceNotFound(ReferenceNotFoundPolicy.ERROR)
        public Target target;
    }

    /**
     * A {@link Record} that other {@link Record Records} reference.
     */
    class Target extends Record {

        /**
         * A stored value, so that the class registers as a {@link Record} type.
         */
        String name = "target";
    }

}
