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
import java.util.Objects;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Test class for the {@link Record#replace(Record, Record)} method.
 *
 * @author Jeff Nelson
 */
public class RecordReplaceTest extends RunwayBaseClientServerTest {

    /**
     * Test that direct field replacement works correctly.
     */
    @Test
    public void testDirectFieldReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create a record that references person1
        Contact contact = new Contact();
        contact.primaryContact = person1;
        contact.alternateContact = person1;

        // Replace person1 with person2
        contact.replace(person1, person2);

        // Verify replacement
        Assert.assertEquals(person2, contact.primaryContact);
        Assert.assertEquals(person2, contact.alternateContact);
        Assert.assertNotSame(person1, contact.primaryContact);
        Assert.assertNotSame(person1, contact.alternateContact);
    }

    /**
     * Test that nested record replacement works correctly.
     */
    @Test
    public void testNestedRecordReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create nested structure
        Department dept = new Department("Engineering");
        dept.manager = person1;
        dept.employees.add(person1);

        Team team = new Team("Backend");
        team.lead = person1;
        team.members.add(person1);

        dept.teams.add(team);

        // Replace person1 with person2
        dept.replace(person1, person2);

        // Verify replacement at all levels
        Assert.assertEquals(person2, dept.manager);
        Assert.assertEquals(person2, dept.employees.get(0));
        Assert.assertEquals(person2, dept.teams.get(0).lead);
        Assert.assertEquals(person2, dept.teams.get(0).members.get(0));
    }

    /**
     * Test that deferred reference replacement works correctly.
     */
    @Test
    public void testDeferredReferenceReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create structure with deferred references
        Organization org = new Organization("TechCorp");
        org.ceo = new DeferredReference<>(person1);
        org.directors.add(new DeferredReference<>(person1));

        // Replace person1 with person2
        org.replace(person1, person2);

        // Verify replacement
        Assert.assertEquals(person2, org.ceo.get());
        Assert.assertEquals(person2, org.directors.get(0).get());
    }

    /**
     * Test that sequence replacement works correctly.
     */
    @Test
    public void testSequenceReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create structure with sequences
        Project project = new Project("WebApp");
        project.contributors.add(person1);
        project.contributors.add(new Person("Charlie", 40));
        project.contributors.add(person1);

        // Replace person1 with person2
        project.replace(person1, person2);

        // Verify replacement in sequence
        Assert.assertEquals(person2, project.contributors.get(0));
        Assert.assertEquals(person2, project.contributors.get(2));
        Assert.assertNotEquals(person1, project.contributors.get(0));
        Assert.assertNotEquals(person1, project.contributors.get(2));
    }

    /**
     * Test that array replacement works correctly.
     */
    @Test
    public void testArrayReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create structure with array
        Company company = new Company("Startup");
        company.founders = new Person[] { person1, new Person("Charlie", 40),
                person1 };

        // Replace person1 with person2
        company.replace(person1, person2);

        // Verify replacement in array
        Assert.assertEquals(person2, company.founders[0]);
        Assert.assertEquals(person2, company.founders[2]);
        Assert.assertNotEquals(person1, company.founders[0]);
        Assert.assertNotEquals(person1, company.founders[2]);
    }

    /**
     * Test that set replacement works correctly.
     */
    @Test
    public void testSetReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create structure with set
        Club club = new Club("Chess");
        club.members.add(person1);
        club.members.add(new Person("Charlie", 40));

        // Replace person1 with person2
        club.replace(person1, person2);

        // Verify replacement in set
        Assert.assertTrue(club.members.contains(person2));
        Assert.assertFalse(club.members.contains(person1));
        Assert.assertEquals(2, club.members.size());
    }

    /**
     * Test that circular reference handling works correctly.
     */
    @Test
    public void testCircularReferenceHandling() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create circular reference
        person1.friend = person1;
        person1.coworkers.add(person1);

        // Replace person1 with person2
        person1.replace(person1, person2);

        // Verify replacement breaks circular reference
        Assert.assertEquals(person2, person1.friend);
        Assert.assertEquals(person2, person1.coworkers.get(0));
    }

    /**
     * Test that null values are handled correctly.
     */
    @Test
    public void testNullValueHandling() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create structure with null values
        Contact contact = new Contact();
        contact.primaryContact = person1;
        contact.alternateContact = null;

        // Replace person1 with person2
        contact.replace(person1, person2);

        // Verify replacement works and nulls are preserved
        Assert.assertEquals(person2, contact.primaryContact);
        Assert.assertNull(contact.alternateContact);
    }

    /**
     * Test that transient fields are ignored during replacement.
     */
    @Test
    public void testTransientFieldIgnored() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create structure with transient field
        Contact contact = new Contact();
        contact.primaryContact = person1;
        contact.setTransientField(person1);

        // Replace person1 with person2
        contact.replace(person1, person2);

        // Verify transient field is not replaced
        Assert.assertEquals(person2, contact.primaryContact);
        Assert.assertEquals(person1, contact.getTransientField());
    }

    /**
     * Test that replacement works with complex nested structures.
     */
    @Test
    public void testComplexNestedStructureReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create complex nested structure
        Enterprise enterprise = new Enterprise("MegaCorp");

        // Department level
        Department dept = new Department("Engineering");
        dept.manager = person1;
        dept.employees.add(person1);

        // Team level within department
        Team team = new Team("Backend");
        team.lead = person1;
        team.members.add(person1);
        dept.teams.add(team);

        // Project level within team
        Project project = new Project("API");
        project.contributors.add(person1);
        team.projects.add(project);

        enterprise.departments.add(dept);

        // Replace person1 with person2
        enterprise.replace(person1, person2);

        // Verify replacement at all levels
        Assert.assertEquals(person2, dept.manager);
        Assert.assertEquals(person2, dept.employees.get(0));
        Assert.assertEquals(person2, dept.teams.get(0).lead);
        Assert.assertEquals(person2, dept.teams.get(0).members.get(0));
        Assert.assertEquals(person2,
                dept.teams.get(0).projects.get(0).contributors.get(0));
    }

    /**
     * Test that replacement works with mixed types in sequences.
     */
    @Test
    public void testMixedTypeSequenceReplacement() {
        Person person1 = new Person("Alice", 25);
        Person person2 = new Person("Bob", 30);

        // Create structure with mixed types
        MixedContainer container = new MixedContainer();
        container.items.add(person1);
        container.items.add("string");
        container.items.add(42);
        container.items.add(person1);

        // Replace person1 with person2
        container.replace(person1, person2);

        // Verify replacement in mixed sequence
        Assert.assertEquals(person2, container.items.get(0));
        Assert.assertEquals("string", container.items.get(1));
        Assert.assertEquals(42, container.items.get(2));
        Assert.assertEquals(person2, container.items.get(3));
    }

    /**
     * Test that replacement with same object does nothing.
     */
    @Test
    public void testReplaceWithSameObject() {
        Person person1 = new Person("Alice", 25);

        // Create structure
        Contact contact = new Contact();
        contact.primaryContact = person1;
        contact.alternateContact = person1;

        // Replace person1 with person1 (same object)
        contact.replace(person1, person1);

        // Verify nothing changed
        Assert.assertSame(person1, contact.primaryContact);
        Assert.assertSame(person1, contact.alternateContact);
    }

    /**
     * Test that replacement with null replacement works (no exception thrown).
     */
    @Test
    public void testReplaceWithNullReplacement() {
        Person person1 = new Person("Alice", 25);
        Contact contact = new Contact();
        contact.primaryContact = person1;

        // This should not throw an exception
        contact.replace(person1, null);

        // Verify the field is now null
        Assert.assertNull(contact.primaryContact);
    }

    /**
     * Test that replacement with null find parameter works (no exception
     * thrown).
     */
    @Test
    public void testReplaceWithNullFind() {
        Person person1 = new Person("Alice", 25);
        Contact contact = new Contact();
        contact.primaryContact = person1;

        // This should not throw an exception
        contact.replace(null, person1);

        // Verify nothing changed since null was not found
        Assert.assertEquals(person1, contact.primaryContact);
    }

    // Test data classes

    class Person extends Record {
        public String name;
        public int age;
        public Person friend;
        public List<Person> coworkers = Lists.newArrayList();

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj)
                return true;
            if(obj == null || getClass() != obj.getClass())
                return false;
            Person person = (Person) obj;
            return age == person.age && name.equals(person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
    }

    class Contact extends Record {
        public Person primaryContact;
        public Person alternateContact;
        private transient Person transientField;

        public void setTransientField(Person person) {
            this.transientField = person;
        }

        public Person getTransientField() {
            return transientField;
        }
    }

    class Department extends Record {
        public String name;
        public Person manager;
        public List<Person> employees = Lists.newArrayList();
        public List<Team> teams = Lists.newArrayList();

        public Department(String name) {
            this.name = name;
        }
    }

    class Team extends Record {
        public String name;
        public Person lead;
        public List<Person> members = Lists.newArrayList();
        public List<Project> projects = Lists.newArrayList();

        public Team(String name) {
            this.name = name;
        }
    }

    class Project extends Record {
        public String name;
        public List<Person> contributors = Lists.newArrayList();

        public Project(String name) {
            this.name = name;
        }
    }

    class Organization extends Record {
        public String name;
        public DeferredReference<Person> ceo;
        public List<DeferredReference<Person>> directors = Lists.newArrayList();

        public Organization(String name) {
            this.name = name;
        }
    }

    class Company extends Record {
        public String name;
        public Person[] founders;

        public Company(String name) {
            this.name = name;
        }
    }

    class Club extends Record {
        public String name;
        public Set<Person> members = Sets.newHashSet();

        public Club(String name) {
            this.name = name;
        }
    }

    class Enterprise extends Record {
        public String name;
        public List<Department> departments = Lists.newArrayList();

        public Enterprise(String name) {
            this.name = name;
        }
    }

    class MixedContainer extends Record {
        public List<Object> items = Lists.newArrayList();
    }
}