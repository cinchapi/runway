/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

/**
 * Unit tests for functionality to cascade deletes in {@link Record Records}.
 *
 * @author Jeff Nelson
 */
public class RunwayCascadeDeleteTest extends RunwayBaseClientServerTest {

    @Test
    public void testCascadeDeleteOnPublicField() {
        ParentRecord parent = new ParentRecord();
        parent.child = new ChildRecord();
        parent.deleteOnSave();
        parent.save();
        Map<Class<? extends Record>, Long> records = ImmutableMap.of(
                ParentRecord.class, parent.id(), ChildRecord.class,
                parent.child.id());
        records.forEach((clazz, id) -> {
            try {
                runway.load(clazz, id);
                Assert.fail();
            }
            catch (IllegalStateException e) {
                Assert.assertTrue(true);
            }
        });
    }

    @Test
    public void testNoCascadeDeleteOnCollectionWithoutAnnotation() {
        ParentRecord parent = new ParentRecord();

        // Create multiple ChildRecord instances
        ChildRecord child1 = new ChildRecord();
        ChildRecord child2 = new ChildRecord();

        // Assign ChildRecords to the ParentRecord's children collection
        parent.children = ImmutableList.of(child1, child2);

        // Save the records individually
        parent.save();
        child1.save();
        child2.save();

        // Delete the parent with cascading behavior enabled
        parent.deleteOnSave();
        parent.save();

        // Try to load each ChildRecord; they should still exist since they were
        // not annotated
        try {
            runway.load(ChildRecord.class, child1.id());
            runway.load(ChildRecord.class, child2.id());
        }
        catch (IllegalStateException e) {
            Assert.fail("Child records should not have been deleted.");
        }

        // Confirm the ParentRecord has been deleted
        try {
            runway.load(ParentRecord.class, parent.id());
            Assert.fail("Parent record should have been deleted.");
        }
        catch (IllegalStateException e) {
            Assert.assertTrue("Parent not found as expected", true);
        }
    }

    @Test
    public void testCascadeDeleteOnCollection() {
        ParentWithCascadeCollection parent = new ParentWithCascadeCollection();

        // Create multiple ChildRecord instances
        ChildRecord child1 = new ChildRecord();
        ChildRecord child2 = new ChildRecord();

        // Assign ChildRecords to the collection in ParentWithCascadeCollection
        parent.children = ImmutableList.of(child1, child2);

        // Save the parent record
        parent.save();

        // Verify the children were saved
        Assert.assertNotNull(child1.id());
        Assert.assertNotNull(child2.id());

        // Trigger cascading delete on save
        parent.deleteOnSave();
        parent.save();

        // Verify that all records have been deleted
        Multimap<Class<? extends Record>, Long> records = ArrayListMultimap
                .create();
        records.put(ParentWithCascadeCollection.class, parent.id());
        records.put(ChildRecord.class, child1.id());
        records.put(ChildRecord.class, child2.id());

        records.entries().forEach(entry -> {
            try {
                runway.load(entry.getKey(), entry.getValue());
                Assert.fail("Record should have been deleted " + entry.getKey()
                        + " = " + entry.getValue());
            }
            catch (IllegalStateException e) {
                Assert.assertTrue("Record not found as expected", true);
            }
        });
    }

    @Test
    public void testCascadeDeleteOnPrivateField() {
        ParentWithPrivateCascade parent = new ParentWithPrivateCascade();

        // Create an AdditionalRecord instance and assign it to the private
        // field
        AdditionalRecord additional = new AdditionalRecord();
        parent.setAdditionalRecord(additional);

        // Save both records
        parent.save();
        additional.save();

        // Verify that the AdditionalRecord was saved
        Assert.assertNotNull(additional.id());

        // Delete the parent and verify cascading delete behavior
        parent.deleteOnSave();
        parent.save();

        // Check that both the ParentWithPrivateCascade and AdditionalRecord
        // were deleted
        Map<Class<? extends Record>, Long> records = ImmutableMap.of(
                ParentWithPrivateCascade.class, parent.id(),
                AdditionalRecord.class, additional.id());

        records.forEach((clazz, id) -> {
            try {
                runway.load(clazz, id);
                Assert.fail("Record should have been deleted");
            }
            catch (IllegalStateException e) {
                Assert.assertTrue("Record not found as expected", true);
            }
        });
    }

    /**
     * Represents a parent record that can contain a single child record,
     * a collection of child records, and an inner record. Supports cascade
     * deletion.
     */
    class ParentRecord extends Record {
        @CascadeDelete
        public ChildRecord child;

        public Collection<ChildRecord> children;

        @CascadeDelete
        protected InnerRecord inner;
    }

    /**
     * Represents a child record associated with a parent record and
     * potentially containing inner records. Supports cascade deletion.
     */
    class ChildRecord extends Record {
        @CascadeDelete
        private ParentRecord parent;

        protected Collection<InnerRecord> innerChildren;
    }

    /**
     * Represents an inner record linked to a sibling child record.
     */
    class InnerRecord extends Record {
        public ChildRecord sibling;
    }

    /**
     * Inherits from ParentRecord and adds an additional record
     * field, supporting cascade deletion.
     */
    class InheritedRecord extends ParentRecord {
        @CascadeDelete
        private AdditionalRecord additionalRecord;
    }

    /**
     * Represents an additional record with custom data fields.
     */
    class AdditionalRecord extends Record {
        public String data;
    }

    /**
     * A test class to simulate a parent record with a collection of child
     * records that should be cascade-deleted.
     */
    class ParentWithCascadeCollection extends Record {

        @CascadeDelete
        public List<ChildRecord> children;
    }

    /**
     * A test class to simulate a parent record with a private field that should
     * be cascade-deleted.
     */
    class ParentWithPrivateCascade extends Record {
        @CascadeDelete
        private AdditionalRecord additionalRecord;

        public void setAdditionalRecord(AdditionalRecord additionalRecord) {
            this.additionalRecord = additionalRecord;
        }
    }

    @Test
    public void testCascadeDeleteAtomicityOnTransactionFailure() {
        // Create four parent records, each with a unique child
        ParentWithUniqueField parent1 = new ParentWithUniqueField();
        ParentWithUniqueField parent2 = new ParentWithUniqueField();
        ParentWithUniqueField parent3 = new ParentWithUniqueField();
        ParentWithUniqueField parent4 = new ParentWithUniqueField();

        ChildWithUniqueParent child1 = new ChildWithUniqueParent();
        ChildWithUniqueParent child2 = new ChildWithUniqueParent();
        ChildWithUniqueParent child3 = new ChildWithUniqueParent();
        ChildWithUniqueParent child4 = new ChildWithUniqueParent();

        // Link each child to a unique parent
        parent1.child = child1;
        parent2.child = child2;
        parent3.child = child3;
        parent4.child = child4;

        // Save all records initially and verify they were saved
        Assert.assertTrue(runway.save(parent1, parent2, parent3, parent4,
                child1, child2, child3, child4));

        // Verify that all records are saved and loadable
        Assert.assertNotNull(
                runway.load(ParentWithUniqueField.class, parent1.id()));
        Assert.assertNotNull(
                runway.load(ParentWithUniqueField.class, parent2.id()));
        Assert.assertNotNull(
                runway.load(ParentWithUniqueField.class, parent3.id()));
        Assert.assertNotNull(
                runway.load(ParentWithUniqueField.class, parent4.id()));
        Assert.assertNotNull(
                runway.load(ChildWithUniqueParent.class, child1.id()));
        Assert.assertNotNull(
                runway.load(ChildWithUniqueParent.class, child2.id()));
        Assert.assertNotNull(
                runway.load(ChildWithUniqueParent.class, child3.id()));
        Assert.assertNotNull(
                runway.load(ChildWithUniqueParent.class, child4.id()));

        // Set a conflicting unique value on all records
        parent1.uniqueField = "conflict";
        parent2.uniqueField = "conflict";
        parent3.uniqueField = "conflict";
        parent4.uniqueField = "conflict";
        child1.uniqueField = "conflict";
        child2.uniqueField = "conflict";
        child3.uniqueField = "conflict";
        child4.uniqueField = "conflict";

        // Mark two records for deletion
        parent1.deleteOnSave();

        // Attempt to save all records, expecting a failure due to unique
        // constraint
        Assert.assertFalse(
                "Transaction should have failed due to unique constraint",
                runway.save(parent1, parent2, parent3, parent4, child1, child2,
                        child3, child4));

        // Verify that all records still exist and were unaffected by the failed
        // transaction
        Multimap<Class<? extends Record>, Long> records = ArrayListMultimap
                .create();
        records.put(ParentWithUniqueField.class, parent1.id());
        records.put(ParentWithUniqueField.class, parent2.id());
        records.put(ParentWithUniqueField.class, parent3.id());
        records.put(ParentWithUniqueField.class, parent4.id());
        records.put(ChildWithUniqueParent.class, child1.id());
        records.put(ChildWithUniqueParent.class, child2.id());
        records.put(ChildWithUniqueParent.class, child3.id());
        records.put(ChildWithUniqueParent.class, child4.id());

        records.entries().forEach(entry -> {
            Record loadedRecord = runway.load(entry.getKey(), entry.getValue());
            Assert.assertNotNull(
                    "Record should exist after transaction failure",
                    loadedRecord);

            if(loadedRecord instanceof ParentWithUniqueField) {
                Assert.assertNull(
                        "Unique field should not be set due to transaction failure",
                        ((ParentWithUniqueField) loadedRecord).uniqueField);
            }
            else if(loadedRecord instanceof ChildWithUniqueParent) {
                Assert.assertNull(
                        "Unique field should not be set due to transaction failure",
                        ((ChildWithUniqueParent) loadedRecord).uniqueField);
            }
        });
    }

    /**
     * A parent record with a unique field to cause transaction failures for
     * atomicity testing with cascade deletion.
     */
    class ParentWithUniqueField extends Record {
        @CascadeDelete
        public ChildWithUniqueParent child;

        @Unique
        public String uniqueField;
    }

    /**
     * A child record associated with {@link ParentWithUniqueField} to verify
     * cascade delete atomicity.
     */
    class ChildWithUniqueParent extends Record {
        @CascadeDelete
        private ParentWithUniqueField parent;

        @Unique
        public String uniqueField;
    }

}
