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
import java.util.List;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for the functionality of {@link JoinDelete} in {@link Record}
 * classes. These tests verify that records with fields annotated with
 * {@link @JoinDelete} delete their containing records when the linked record is
 * deleted.
 * 
 * author Jeff Nelson
 */
public class RunwayJoinDeleteTest extends RunwayBaseClientServerTest {

    @Test
    public void testAtomicJoinDeleteOnTransactionFailure() {
        ParentWithUniqueField parent1 = new ParentWithUniqueField();
        ParentWithUniqueField parent2 = new ParentWithUniqueField();
        ParentWithUniqueField parent3 = new ParentWithUniqueField();
        ChildWithUniqueParent child1 = new ChildWithUniqueParent();
        ChildWithUniqueParent child2 = new ChildWithUniqueParent();
        ChildWithUniqueParent child3 = new ChildWithUniqueParent();

        child1.name = "child1";
        child2.name = "child2";
        child3.name = "child3";
        parent1.child = child1;
        parent2.child = child2;
        parent3.child = child3;

        Assert.assertTrue(
                runway.save(parent1, parent2, parent3, child1, child2, child3));

        child1.uniqueField = "conflict";
        child2.uniqueField = "conflict";
        child3.uniqueField = "no conflict";

        child3.deleteOnSave();
        Assert.assertFalse(
                runway.save(parent1, parent2, parent3, child1, child2, child3));
        assertExists(parent1);
        assertExists(parent2);
        assertExists(parent3);
        assertExists(child1);
        assertExists(child2);
        assertExists(child3);
    }

    @Test
    public void testBasicJoinDelete() {
        ParentWithJoinDelete parent = new ParentWithJoinDelete();
        ChildRecord child = new ChildRecord("test");
        parent.child = child;

        Assert.assertTrue(runway.save(parent, child));

        child.deleteOnSave();
        child.save();

        List<Record> records = ImmutableList.of(parent, child);
        records.forEach(record -> assertNotExists(record));
    }

    @Test
    public void testBulkJoinDelete() {
        ChildRecord sharedChild = new ChildRecord("sharedChild");
        ParentWithJoinDelete parent1 = new ParentWithJoinDelete();
        ParentWithJoinDelete parent2 = new ParentWithJoinDelete();
        ParentWithJoinDelete parent3 = new ParentWithJoinDelete();

        parent1.child = sharedChild;
        parent2.child = sharedChild;
        parent3.child = sharedChild;

        Assert.assertTrue(runway.save(parent1, parent2, parent3, sharedChild));

        sharedChild.deleteOnSave();
        sharedChild.save();

        List<Record> records = ImmutableList.of(parent1, parent2, parent3,
                sharedChild);
        records.forEach(this::assertNotExists);
    }

    @Test
    public void testCircularJoinDeleteAndCascadeDeleteNoInfiniteLoop() {
        CircularRecordA recordA = new CircularRecordA();
        CircularRecordB recordB = new CircularRecordB();

        recordA.recordB = recordB;
        recordB.recordA = recordA;

        Assert.assertTrue(runway.save(recordA, recordB));

        recordB.deleteOnSave();
        recordB.save();

        assertNotExists(recordA);
        assertNotExists(recordB);
    }

    @Test
    public void testJoinDeleteOnMultipleLinkedRecords() {
        ChildRecord sharedChild = new ChildRecord("sharedChild");
        ParentWithJoinDelete parent1 = new ParentWithJoinDelete();
        ParentWithJoinDelete parent2 = new ParentWithJoinDelete();

        parent1.child = sharedChild;
        parent2.child = sharedChild;

        Assert.assertTrue(runway.save(parent1, parent2, sharedChild));

        sharedChild.deleteOnSave();
        sharedChild.save();

        assertNotExists(parent1);
        assertNotExists(parent2);
        assertNotExists(sharedChild);
    }

    @Test
    public void testJoinDeleteWithCascadeDeleteChain() {
        ParentWithMixedAnnotations parent = new ParentWithMixedAnnotations();
        RelatedRecord related = new RelatedRecord("related");
        ChildRecord child = new ChildRecord("child");
        parent.related = related;
        parent.child = child;

        Assert.assertTrue(runway.save(related, parent, child));

        child.deleteOnSave();
        child.save();

        assertNotExists(related);
        assertNotExists(parent);
        assertNotExists(child);
    }

    @Test
    public void testMultipleJoinDeleteFields() {
        ParentWithJoinDelete parent = new ParentWithJoinDelete();
        ChildRecord child1 = new ChildRecord("child1");
        ChildRecord child2 = new ChildRecord("child2");

        parent.child = child1;
        parent.additionalChild = child2;

        Assert.assertTrue(runway.save(parent, child1, child2));

        child1.deleteOnSave();
        child1.save();

        assertNotExists(parent);
        assertNotExists(child1);
        assertExists(child2);
    }

    @Test
    public void testNestedJoinDeleteChainWithMultipleClasses() {
        // Create the nested hierarchy with distinct classes
        Grandparent grandparent = new Grandparent();
        Parent parent = new Parent();
        Child child = new Child("child");
        Grandchild grandchild = new Grandchild("grandchild");

        // Link the hierarchy together
        grandparent.parent = parent;
        parent.child = child;
        child.grandchild = grandchild;

        // Save the hierarchy to set up the test
        Assert.assertTrue(runway.save(grandparent, parent, child, grandchild));

        // Trigger the deletion chain by deleting the Grandchild
        grandchild.deleteOnSave();
        grandchild.save();

        // Verify that all records in the hierarchy have been deleted
        List<Record> records = ImmutableList.of(grandparent, parent, child, grandchild);
        records.forEach(this::assertNotExists);
    }

    @Test
    public void testNestedJoinDeleteChainWithSelfReferencingClass() {
        // Create a chain of Person instances linked by the "friend" field
        Person person1 = new Person("Person1");
        Person person2 = new Person("Person2");
        Person person3 = new Person("Person3");
        Person person4 = new Person("Person4");

        // Link the chain together
        person1.friend = person2;
        person2.friend = person3;
        person3.friend = person4;

        // Save the chain to set up the test
        Assert.assertTrue(runway.save(person1, person2, person3, person4));

        // Trigger the deletion chain by deleting the last Person in the chain
        person4.deleteOnSave();
        person4.save();

        // Verify that all Person records in the chain have been deleted
        List<Record> people = ImmutableList.of(person1, person2, person3, person4);
        people.forEach(this::assertNotExists);
    }

    @Test
    public void testNullFieldWithJoinDelete() {
        ParentWithJoinDelete parent = new ParentWithJoinDelete();
        parent.child = null;

        Assert.assertTrue(runway.save(parent));
        assertExists(parent);
    }

    /**
     * Asserts that a {@link Record} of {@code clazz} with {@code id} exists in
     * the database.
     * 
     * @param clazz
     * @param id
     */
    private void assertExists(Class<? extends Record> clazz, long id) {
        Assert.assertNotNull(runway.load(clazz, id));
    }

    /**
     * Asserts that {@code record} exists in the database.
     * 
     * @param record
     */
    private void assertExists(Record record) {
        assertExists(record.getClass(), record.id());
    }
    
    /**
     * Asserts that a record of the specified class and ID does not exist in the
     * database. Attempts to load the record and fails the test if the record
     * is found. Additionally verifies that the database has no remaining data
     * associated with the specified record ID.
     *
     * @param clazz the class of the record to check
     * @param id the ID of the record to check
     */
    private void assertNotExists(Class<? extends Record> clazz, long id) {
        try {
            runway.load(clazz, id);
            Assert.fail(clazz.getSimpleName() + " should have been deleted.");
        }
        catch (IllegalStateException e) {
            Assert.assertTrue("Record not found as expected", true);
        }
        Assert.assertTrue("The database still has data for the deleted Record",
                client.select(id).isEmpty());
    }

    /**
     * Asserts that the specified record does not exist in the database by
     * attempting to load it. Fails the test if the record is found, ensuring
     * that deletion was successful and no data remains for the record.
     *
     * @param record the record to check for existence
     */
    private void assertNotExists(Record record) {
        assertNotExists(record.getClass(), record.id());
    }

    /**
     * Represents the child record in a nested hierarchy, which is 
     * deleted when {@link Grandchild} is deleted due to {@link JoinDelete}.
     */
    class Child extends Record {
        
        /** The name of the child record, used for identification in tests. */
        String name;
        
        /** The linked grandchild record, deleted when removed due to JoinDelete. */
        @JoinDelete
        public Grandchild grandchild;

        public Child(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a simple child record with a name field, used for testing
     * deletion chains and annotations.
     */
    class ChildRecord extends Record {

        String name;

        public ChildRecord(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a child record with a unique field, demonstrating unique
     * constraint
     * handling within a parent-child relationship.
     */
    class ChildWithUniqueParent extends Record {

        String name;

        @Unique
        public String uniqueField;
    }

    /**
     * Represents a record that links to {@link CircularRecordB} via
     * {@link JoinDelete},
     * forming part of a circular reference to test cascading deletion.
     */
    class CircularRecordA extends Record {

        @JoinDelete
        public CircularRecordB recordB;
    }

    /**
     * Represents a record that links to {@link CircularRecordA} via
     * {@link CascadeDelete},
     * forming part of a circular reference to test cascading deletion.
     */
    class CircularRecordB extends Record {

        @CascadeDelete
        public CircularRecordA recordA;
    }

    /**
     * Represents the grandchild record in a nested hierarchy, which 
     * triggers deletions up the chain when removed.
     */
    class Grandchild extends Record {
        
        /** The name of the grandchild record, used for identification in tests. */
        String name;

        public Grandchild(String name) {
            this.name = name;
        }
    }

    /**
     * Represents the grandparent record in a nested hierarchy, which 
     * is deleted when {@link Parent} is deleted due to {@link JoinDelete}.
     */
    class Grandparent extends Record {
        
        /** The linked parent record, deleted when removed due to JoinDelete. */
        @JoinDelete
        public Parent parent;
    }

    /**
     * Represents the parent record in a nested hierarchy, which is 
     * deleted when {@link Child} is deleted due to {@link JoinDelete}.
     */
    class Parent extends Record {
        
        /** The linked child record, deleted when removed due to JoinDelete. */
        @JoinDelete
        public Child child;
    }

    /**
     * Represents a parent record with fields that trigger deletions of linked
     * records via {@link JoinDelete}.
     */
    class ParentWithJoinDelete extends Record {

        @JoinDelete
        public ChildRecord child;

        @JoinDelete
        public ChildRecord additionalChild;
    }

    /**
     * Represents a parent record that has a mix of deletion annotations,
     * linking to a child via {@link JoinDelete} and a related record via
     * {@link CascadeDelete}.
     */
    class ParentWithMixedAnnotations extends Record {

        @JoinDelete
        public ChildRecord child;

        @CascadeDelete
        public RelatedRecord related;
    }

    /**
     * Represents a parent record that has a unique field and a linked child
     * record, demonstrating behavior when multiple unique constraints and
     * {@link JoinDelete} are used together.
     */
    class ParentWithUniqueField extends Record {

        @JoinDelete
        public ChildWithUniqueParent child;

        @Unique
        public String uniqueField;
    }

    /**
     * Represents a self-referencing person class, where one person is 
     * linked to another via a "friend" field, enabling a chain of 
     * deletions with {@link JoinDelete}.
     */
    class Person extends Record {
        
        /** The name of the person record, used for identification in tests. */
        String name;
        
        /** The friend record linked with JoinDelete, triggering deletion up the chain. */
        @JoinDelete
        public Person friend;

        public Person(String name) {
            this.name = name;
        }
    }

    /**
     * Represents a related record linked to another record via
     * {@link CascadeDelete}, demonstrating how dependencies are removed in a
     * chain of deletions.
     */
    class RelatedRecord extends Record {

        String value;

        public RelatedRecord(String value) {
            this.value = value;
        }
    }

}
