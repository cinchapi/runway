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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for objects with bidirectional relationships in Runway.
 * 
 * @author Jeff Nelson
 */
public class RunwayBiDirectionalRelationshipTest
        extends RunwayBaseClientServerTest {

    @Test
    public void testBidirectionalRelationshipSaving() {
        // Create bidirectional relationship between Parent and Child
        ParentRecord parent = new ParentRecord("Parent1");
        ChildRecord child = new ChildRecord("Child1");

        // Establish the bidirectional relationship
        parent.child = child;
        child.parent = parent;

        // Save the parent and verify cascade to the child
        Assert.assertTrue(runway.save(parent));

        // Load both records and verify relationship persistence
        parent = runway.load(ParentRecord.class, parent.id());
        child = runway.load(ChildRecord.class, child.id());

        Assert.assertNotNull(
                "Child should be loaded through bidirectional relationship",
                parent.child);
        Assert.assertNotNull(
                "Parent should be loaded through bidirectional relationship",
                child.parent);
        Assert.assertEquals("Parent1", parent.name);
        Assert.assertEquals("Child1", child.name);
    }

    @Test
    public void testMultipleBidirectionalRelationships() {
        // Create bidirectional relationships among Parent, Child, and Sibling
        ParentRecord parent = new ParentRecord("Parent2");
        ChildRecord child = new ChildRecord("Child2");
        SiblingRecord sibling = new SiblingRecord("Sibling2");

        parent.child = child;
        child.parent = parent;

        sibling.parent = parent;
        parent.sibling = sibling;

        // Save the parent and verify cascade to both child and sibling
        Assert.assertTrue(runway.save(parent));

        // Load all records and verify relationships
        parent = runway.load(ParentRecord.class, parent.id());
        child = runway.load(ChildRecord.class, child.id());
        sibling = runway.load(SiblingRecord.class, sibling.id());

        Assert.assertNotNull("Child should have reference to Parent",
                child.parent);
        Assert.assertNotNull("Sibling should have reference to Parent",
                sibling.parent);
        Assert.assertNotNull("Parent should have reference to Child",
                parent.child);
        Assert.assertNotNull("Parent should have reference to Sibling",
                parent.sibling);

        Assert.assertEquals("Parent2", parent.name);
        Assert.assertEquals("Child2", child.name);
        Assert.assertEquals("Sibling2", sibling.name);
    }

    @Test
    public void testCircularBidirectionalRelationships() {
        // Create circular bidirectional relationship between A and B
        CircularRecordA recordA = new CircularRecordA("RecordA");
        CircularRecordB recordB = new CircularRecordB("RecordB");

        recordA.recordB = recordB;
        recordB.recordA = recordA;

        // Save recordA and verify cascade to recordB
        Assert.assertTrue(runway.save(recordA));

        // Load both records and verify relationships
        recordA = runway.load(CircularRecordA.class, recordA.id());
        recordB = runway.load(CircularRecordB.class, recordB.id());

        Assert.assertNotNull("RecordA should have reference to RecordB",
                recordA.recordB);
        Assert.assertNotNull("RecordB should have reference to RecordA",
                recordB.recordA);

        Assert.assertEquals("RecordA", recordA.name);
        Assert.assertEquals("RecordB", recordB.name);
    }

    @Test
    public void testMultiBranchBidirectionalRelationship() {
        // Create an object graph with multiple branches of relationships from
        // Root
        RootRecord root = new RootRecord("Root");

        // First branch
        LevelOneRecord branchOneLevelOne = new LevelOneRecord(
                "BranchOneLevelOne");
        root.branchOneLevelOne = branchOneLevelOne;
        branchOneLevelOne.root = root;

        // Second branch (deeper with four levels)
        LevelOneRecord branchTwoLevelOne = new LevelOneRecord(
                "BranchTwoLevelOne");
        LevelTwoRecord branchTwoLevelTwo = new LevelTwoRecord(
                "BranchTwoLevelTwo");
        LevelThreeRecord branchTwoLevelThree = new LevelThreeRecord(
                "BranchTwoLevelThree");
        LevelFourRecord branchTwoLevelFour = new LevelFourRecord(
                "BranchTwoLevelFour");

        root.branchTwoLevelOne = branchTwoLevelOne;
        branchTwoLevelOne.root = root;

        branchTwoLevelOne.levelTwo = branchTwoLevelTwo;
        branchTwoLevelTwo.levelOne = branchTwoLevelOne;

        branchTwoLevelTwo.levelThree = branchTwoLevelThree;
        branchTwoLevelThree.levelTwo = branchTwoLevelTwo;

        branchTwoLevelThree.levelFour = branchTwoLevelFour;
        branchTwoLevelFour.levelThree = branchTwoLevelThree;

        // Third branch
        LevelOneRecord branchThreeLevelOne = new LevelOneRecord(
                "BranchThreeLevelOne");
        root.branchThreeLevelOne = branchThreeLevelOne;
        branchThreeLevelOne.root = root;

        // Save the root and verify cascading
        Assert.assertTrue(runway.save(root));

        // Reload the root and verify the entire structure
        root = runway.load(RootRecord.class, root.id());

        // Verify first branch
        branchOneLevelOne = root.branchOneLevelOne;
        Assert.assertNotNull("BranchOneLevelOne should not be null",
                branchOneLevelOne);
        Assert.assertEquals("Root reference in branchOneLevelOne should match",
                root, branchOneLevelOne.root);
        Assert.assertEquals("BranchOneLevelOne", branchOneLevelOne.name);

        // Verify second branch
        branchTwoLevelOne = root.branchTwoLevelOne;
        LevelTwoRecord loadedBranchTwoLevelTwo = branchTwoLevelOne.levelTwo;
        LevelThreeRecord loadedBranchTwoLevelThree = loadedBranchTwoLevelTwo.levelThree;
        LevelFourRecord loadedBranchTwoLevelFour = loadedBranchTwoLevelThree.levelFour;

        Assert.assertNotNull("BranchTwoLevelOne should not be null",
                branchTwoLevelOne);
        Assert.assertNotNull("BranchTwoLevelTwo should not be null",
                loadedBranchTwoLevelTwo);
        Assert.assertNotNull("BranchTwoLevelThree should not be null",
                loadedBranchTwoLevelThree);
        Assert.assertNotNull("BranchTwoLevelFour should not be null",
                loadedBranchTwoLevelFour);
        Assert.assertEquals("BranchTwoLevelOne should reference Root", root,
                branchTwoLevelOne.root);
        Assert.assertEquals(
                "BranchTwoLevelTwo should reference BranchTwoLevelOne",
                branchTwoLevelOne, loadedBranchTwoLevelTwo.levelOne);
        Assert.assertEquals(
                "BranchTwoLevelThree should reference BranchTwoLevelTwo",
                loadedBranchTwoLevelTwo, loadedBranchTwoLevelThree.levelTwo);
        Assert.assertEquals(
                "BranchTwoLevelFour should reference BranchTwoLevelThree",
                loadedBranchTwoLevelThree, loadedBranchTwoLevelFour.levelThree);

        // Verify third branch
        branchThreeLevelOne = root.branchThreeLevelOne;
        Assert.assertNotNull("BranchThreeLevelOne should not be null",
                branchThreeLevelOne);
        Assert.assertEquals(
                "Root reference in branchThreeLevelOne should match", root,
                branchThreeLevelOne.root);
        Assert.assertEquals("BranchThreeLevelOne", branchThreeLevelOne.name);
    }

    /**
     * Represents the root object in the relationship graph.
     */
    class RootRecord extends Record {
        String name;
        public LevelOneRecord branchOneLevelOne;
        public LevelOneRecord branchTwoLevelOne;
        public LevelOneRecord branchThreeLevelOne;

        public RootRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents the first level object with a bidirectional link back to the
     * root.
     */
    class LevelOneRecord extends Record {
        String name;
        public RootRecord root;
        public LevelTwoRecord levelTwo;

        public LevelOneRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents the second level object in the chain, linking back to
     * LevelOne.
     */
    class LevelTwoRecord extends Record {
        String name;
        public LevelOneRecord levelOne;
        public LevelThreeRecord levelThree;

        public LevelTwoRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents the third level object in the chain, linking back to LevelTwo.
     */
    class LevelThreeRecord extends Record {
        String name;
        public LevelTwoRecord levelTwo;
        public LevelFourRecord levelFour;

        public LevelThreeRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents the fourth level object, completing a 4-level deep branch.
     */
    class LevelFourRecord extends Record {
        String name;
        public LevelThreeRecord levelThree;

        public LevelFourRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a parent record with a bidirectional relationship to a child.
     */
    class ParentRecord extends Record {
        String name;
        public ChildRecord child;
        public SiblingRecord sibling;

        public ParentRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a child record with a bidirectional relationship to a parent.
     */
    class ChildRecord extends Record {
        String name;
        public ParentRecord parent;

        public ChildRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a sibling record with a bidirectional relationship to a
     * parent.
     */
    class SiblingRecord extends Record {
        String name;
        public ParentRecord parent;

        public SiblingRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a record in a circular bidirectional relationship with
     * another.
     */
    class CircularRecordA extends Record {
        String name;
        public CircularRecordB recordB;

        public CircularRecordA(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a record in a circular bidirectional relationship with
     * another.
     */
    class CircularRecordB extends Record {
        String name;
        public CircularRecordA recordA;

        public CircularRecordB(String name) {
            this.name = name;
        }
    }
}