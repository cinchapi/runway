/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for verifying {@link CaptureDelete} functionality in various
 * scenarios. Tests confirm that annotated fields are nullified or removed
 * from collections when the linked record is deleted.
 * 
 * author Jeff Nelson
 */
public class RunwayCaptureDeleteTest extends RunwayBaseClientServerTest {
    
    @Test
    public void testBasicReferenceNullification() {
        // Create a parent and child relationship
        ParentRecord parent = new ParentRecord("Parent1");
        ChildRecord child = new ChildRecord("Child1");
        parent.child = child;

        // Save and delete the child
        Assert.assertTrue(runway.save(parent, child));
        child.deleteOnSave();
        child.save();

        // Verify that the reference in ParentRecord is set to null
        parent = runway.load(ParentRecord.class, parent.id());
        Assert.assertNull("Child reference should be null after deletion", parent.child);
        Assert.assertEquals("Parent1", parent.name);
    }
    
    @Test
    public void testCollectionFieldWithCaptureDelete() {
        // Create a parent record with a collection of children
        ParentWithChildrenCollection parent = new ParentWithChildrenCollection("CollectionParent");
        ChildRecord child1 = new ChildRecord("Child1");
        ChildRecord child2 = new ChildRecord("Child2");
        parent.children = new ArrayList<>(Arrays.asList(child1, child2));

        // Save and delete one of the children
        Assert.assertTrue(runway.save(parent, child1, child2));
        child2.deleteOnSave();
        child2.save();

        // Verify that the deleted child is removed from the collection
        parent = runway.load(ParentWithChildrenCollection.class, parent.id());
        Assert.assertEquals("Collection should contain only one child", 1, parent.children.size());
        Assert.assertEquals("Remaining child should be child1", child1.id(), parent.children.get(0).id());
        Assert.assertEquals("CollectionParent", parent.name);
    }
    
    @Test
    public void testMultipleCaptureDeleteFields() {
        // Create a record with two children, each linked with CaptureDelete
        MultiChildRecord parent = new MultiChildRecord("MultiParent");
        ChildRecord child1 = new ChildRecord("Child1");
        ChildRecord child2 = new ChildRecord("Child2");
        parent.child1 = child1;
        parent.child2 = child2;

        // Save and delete the first child
        Assert.assertTrue(runway.save(parent, child1, child2));
        child1.deleteOnSave();
        child1.save();

        // Verify that only child1 reference is null and child2 is intact
        parent = runway.load(MultiChildRecord.class, parent.id());
        Assert.assertNull("child1 reference should be null", parent.child1);
        Assert.assertNotNull("child2 reference should remain intact", parent.child2);
        Assert.assertEquals("MultiParent", parent.name);
    }
    
    @Test
    public void testMixedDeletionAnnotations() {
        // Create a record with mixed deletion annotations
        MixedDeletionRecord parent = new MixedDeletionRecord("MixedParent");
        ChildRecord child1 = new ChildRecord("Child1");
        DependentRecord dependent = new DependentRecord("Dependent");
        parent.captureChild = child1;
        parent.cascadeChild = dependent;

        // Save and delete the child with CaptureDelete
        Assert.assertTrue(runway.save(parent, child1, dependent));
        child1.deleteOnSave();
        child1.save();

        // Verify that the captureChild reference is null and cascadeChild remains intact
        parent = runway.load(MixedDeletionRecord.class, parent.id());
        Assert.assertNull("captureChild should be null after deletion", parent.captureChild);
        Assert.assertNotNull("cascadeChild should remain intact", parent.cascadeChild);
        Assert.assertEquals("MixedParent", parent.name);
    }
    
    @Test
    public void testCircularReferenceWithCaptureDelete() {
        // Create two records with circular references using CaptureDelete
        CircularRecordA recordA = new CircularRecordA("RecordA");
        CircularRecordB recordB = new CircularRecordB("RecordB");
        recordA.recordB = recordB;
        recordB.recordA = recordA;

        // Save and delete one record in the circular reference
        Assert.assertTrue(runway.save(recordA, recordB));
        recordB.deleteOnSave();
        recordB.save();

        // Verify that recordA's reference to recordB is nullified but recordA is intact
        recordA = runway.load(CircularRecordA.class, recordA.id());
        Assert.assertNull("recordB reference should be null in recordA", recordA.recordB);
    }

    /**
     * Represents a parent record with a single child record linked with
     * {@link CaptureDelete} that is nullified when the child is deleted.
     */
    class ParentRecord extends Record {
        String name;
        
        @CaptureDelete
        public ChildRecord child;

        public ParentRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a record with two child records, both linked with
     * {@link CaptureDelete}, where each child can be independently nullified
     * if deleted.
     */
    class MultiChildRecord extends Record {
        String name;

        @CaptureDelete
        public ChildRecord child1;

        @CaptureDelete
        public ChildRecord child2;

        public MultiChildRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a parent record with a collection of child records linked with
     * {@link CaptureDelete}, which will automatically remove deleted records
     * from the collection.
     */
    class ParentWithChildrenCollection extends Record {
        String name;

        @CaptureDelete
        public List<ChildRecord> children;

        public ParentWithChildrenCollection(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a record with mixed deletion annotations, containing a child 
     * with {@link CaptureDelete} and another with {@link CascadeDelete}, 
     * allowing independent deletion behavior for each linked record.
     */
    class MixedDeletionRecord extends Record {
        String name;

        @CaptureDelete
        public ChildRecord captureChild;

        @CascadeDelete
        public DependentRecord cascadeChild;

        public MixedDeletionRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a child record with a name field, used in tests to check 
     * deletion behavior.
     */
    class ChildRecord extends Record {
        String name;

        public ChildRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a dependent record that is deleted with {@link CascadeDelete}
     * for tests involving mixed annotations.
     */
    class DependentRecord extends Record {
        String data;

        public DependentRecord(String data) {
            this.data = data;
        }
    }
    
    /**
     * Represents a record with a circular reference to another record via 
     * {@link CaptureDelete} for testing circular reference nullification.
     */
    class CircularRecordA extends Record {
        String name;

        @CaptureDelete
        public CircularRecordB recordB;

        public CircularRecordA(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a record with a circular reference back to {@link CircularRecordA} 
     * via {@link CaptureDelete} for testing circular reference nullification.
     */
    class CircularRecordB extends Record {
        String name;

        @CaptureDelete
        public CircularRecordA recordA;

        public CircularRecordB(String name) {
            this.name = name;
        }
    }
}
