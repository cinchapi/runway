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
     * <strong>Goal:</strong> Verify that a reference whose target holds no data
     * resolves to nothing and leaves the stored reference in place under the
     * default policy.
     * <p>
     * <strong>Start state:</strong> A saved {@link Holder} that references a
     * saved {@link Target} through an undeclared field.
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
     * resolves the reference to nothing and removes the stored reference.
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
     * fails the load of the record that holds the reference.
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
     * that declares nothing and a saved {@link RepairingHolder} that declares
     * {@link ReferenceNotFoundPolicy#REPAIR}, both referencing one
     * {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Delete the {@link Target}.</li>
     * <li>Load each holder.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The undeclared holder fails its load under the
     * database's policy, and the declared one resolves to nothing and repairs
     * its storage.
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
     * <strong>Goal:</strong> Verify that a reference whose target holds no data
     * is omitted from a collection rather than resolved to a {@code null}
     * element.
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
     * <strong>Goal:</strong> Verify that a reference whose target holds no data
     * resolves the same way whether or not the load pre-selects the referenced
     * record's data.
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
         * A stored value, so the class registers as a {@link Record} type.
         */
        String name = "target";
    }

}
