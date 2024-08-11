/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
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

import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.collect.Association;
import com.cinchapi.runway.Testing.Company;
import com.cinchapi.runway.Testing.Dock;
import com.cinchapi.runway.Testing.ExtendedDock;
import com.cinchapi.runway.Testing.Nock;
import com.cinchapi.runway.Testing.Node;
import com.cinchapi.runway.Testing.Sock;
import com.cinchapi.runway.Testing.User;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 *
 *
 * @author Jeff Nelson
 */
public class RecordMapFilteringTest extends RunwayBaseClientServerTest {

    @Test
    public void testMapRegularKeysOnly() {
        Nock nock = new Nock();
        nock.name = "John Doe";
        nock.age = 25;
        Association data = Association.ensure(nock.map("name", "age"));
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertEquals("John Doe", data.get("name"));
        Assert.assertEquals(25, data.get("age"));
    }

    @Test
    public void testMapCombinationOfRegularAndNavigationKeys() {
        Company company = new Company("Cinchapi");
        User user = new User("Alice", "alice@example.com", company);
        Association data = Association.ensure(user.map("name", "company.name"));
        Assert.assertEquals("Alice", data.get("name"));
        Assert.assertEquals(ImmutableMap.of("name", "Cinchapi"),
                data.get("company"));
    }

    @Test
    public void testMapPositiveFilteringWithNavigationKeys() {
        Company company = new Company("Cinchapi");
        User user = new User("Alice", "alice@example.com", company);
        Association data = Association.ensure(user.map("name", "company.name"));
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("company"));
        Assert.assertTrue(data.containsKey("company.name"));
        Assert.assertFalse(data.containsKey("email"));
    }

    @Test
    public void testMapCombinationPositiveNegativeFiltersNavigation() {
        Company company = new Company("Cinchapi");
        User user = new User("Alice", "alice@example.com", company);
        Association data = Association
                .ensure(user.map("-name", "company.name"));
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("company"));
        Assert.assertTrue(data.containsKey("company.name"));
        Assert.assertFalse(data.containsKey("company.users"));
        Assert.assertFalse(data.containsKey("email"));
    }

    @Test
    public void testMapWithEmptyInputs() {
        Nock nock = new Nock();
        nock.name = "John Doe";
        nock.age = 25;
        Map<String, Object> data = nock.map();
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertEquals("John Doe", data.get("name"));
        Assert.assertEquals(25, data.get("age"));
    }

    @Test
    public void testMapDeepNavigation() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        Node d = new Node("d");
        a.friends.add(b);
        b.friends.add(c);
        c.friends.add(d);
        Map<String, Object> data = a.map("friends.friends.friends.label");
        Assert.assertEquals(
                ImmutableMap.of("friends", ImmutableList.of(ImmutableMap.of(
                        "friends",
                        ImmutableList.of(ImmutableMap.of("friends",
                                ImmutableList
                                        .of(ImmutableMap.of("label", "d"))))))),
                data);
    }

    @Test
    public void testMapDeepNavigationAndRegularKeys() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        b.friends.add(c);
        Map<String, Object> data = a.map("label", "friends.label",
                "friends.friends.label");
        Association assoc = Association.ensure(data);
        Map<String, Object> expected = ImmutableMap.of("label", "a", "friends",
                ImmutableList.of(ImmutableMap.of("label", "b", "friends",
                        ImmutableList.of(ImmutableMap.of("label", "c")))));
        Assert.assertEquals(expected, data);
        Assert.assertEquals(expected, assoc);
    }

    @Test
    public void testMapDeepNavigationPositiveFiltering() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        b.friends.add(c);
        Map<String, Object> data = a.map("friends.friends.label");
        Assert.assertEquals(
                ImmutableMap.of("friends",
                        ImmutableList.of(ImmutableMap.of("friends",
                                ImmutableList
                                        .of(ImmutableMap.of("label", "c"))))),
                data);
    }

    @Test
    public void testMapDeepNavigationNegativeFiltering() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        b.friends.add(c);
        a.map().forEach((key, value) -> {
            System.out.println(key +" = "+value);
        });
        Map<String, Object> data = a.map("-friends.friends.label");
        // @formatter:off
        List<Map<String, Object>> level3 = ImmutableList.of(
            ImmutableMap.of(
                 // no label for c
                "id", c.id(),
                "friends", ImmutableList.of() 
            )
        );
        List<Map<String, Object>> level2 = ImmutableList.of(
            ImmutableMap.of(
                "label", "b",
                "id", b.id(),
                "friends", level3 
             ),
            ImmutableMap.of(
                "label", "c",
                "id", c.id(),
                "friends", ImmutableList.of()
            )
        );
        Map<String, Object> expected = ImmutableMap.of(
            "label", "a",
            "id", a.id(),
            "friends", level2 
        );
        // @formatter:on
        Assert.assertEquals(expected, data);
    }

    @Test
    public void testMapComplexCombinationWithNestedObjects() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        b.friends.add(c);
        // @formatter:off
        List<Map<String, Object>> level3 = ImmutableList.of(
            ImmutableMap.of(
                "label", "c",
                "id", c.id(),
                "friends", ImmutableList.of() 
            )
        );
        List<Map<String, Object>> level2 = ImmutableList.of(
            ImmutableMap.of(
                "id", b.id(),
                "friends", level3 
             ),
            ImmutableMap.of(
                "id", c.id(),
                "friends", ImmutableList.of()
            )
        );
        Map<String, Object> expected = ImmutableMap.of(
            "label", "a",
            "id", a.id(),
            "friends", level2 
        );
        // @formatter:on
        Map<String, Object> data = a.map("label", "-friends.label",
                "friends.friends.label");
        Assert.assertEquals(expected, data);
    }

    @Test
    public void testMapWithIncorrectKeyStructure() {
        Node a = new Node("a");
        Map<String, Object> data = a.map("friends.friends.label");
        // Expecting an empty map since the key structure doesn't match the
        // object
        Assert.assertTrue(data.isEmpty());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testMapNavigationKeysNegativeFiltersWithMoreFields() {
        Sock sock = new Sock("A", new ExtendedDock("B", "ExtraFieldData"));
        Map<String, Object> data = sock.map("-dock.dock");
        Assert.assertTrue(data.containsKey("sock"));
        Assert.assertTrue(data.containsKey("dock"));
        Assert.assertFalse(((Map) data.get("dock")).containsKey("dock"));
        Assert.assertTrue(((Map) data.get("dock")).containsKey("extension"));
    }

    @Test
    public void testMapNavigationKeysPositiveFilters() {
        Company company = new Company("Cinchapi");
        User a = new User("a", "a@a.com", company);
        User b = new User("b", "b@b.com", company);
        runway.save(company, a, b);
        Assert.assertEquals(
                ImmutableMap.of("users", ImmutableSet.of(
                        ImmutableMap.of("name", "a", "email", "a@a.com"),
                        ImmutableMap.of("name", "b", "email", "b@b.com"))),
                company.map("users.name", "users.email"));
    }

    @Test
    public void testMapDeepNavigationThreeLevels() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        b.friends.add(c);
        Assert.assertEquals(
                ImmutableMap.of("friends", ImmutableList.of(ImmutableMap.of(
                        "friends",
                        ImmutableList.of(ImmutableMap.of("friends",
                                ImmutableList
                                        .of(ImmutableMap.of("label", "c"))))))),
                a.map("friends.friends.friends.label"));
    }

    @Test
    public void testMapWithNavigationKeysAndNestedObjects() {
        Sock sock = new Sock("A", new Dock("B"));
        Assert.assertEquals(
                ImmutableMap.of("dock", ImmutableMap.of("dock", "B")),
                sock.map("dock.dock"));
    }

    @Test
    public void testMapPositiveNegativeFilteringInConflict() {
        Company company = new Company("Cinchapi");
        User user = new User("Alice", "alice@example.com", company);
        Map<String, Object> data = user.map("-company", "company.name");

        // Assert that nothing is returned since positive and negative filters
        // are in conflict
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertFalse(data.containsKey("email"));
        Assert.assertFalse(data.containsKey("company"));
    }
    
    @Test
    public void testMapNoInfiniteRecursion() {
        Company company = new Company("Company");
        User a = new User("A", "A", company);
        User b = new User("B", "B", company);
        runway.save(a,b,company);
        System.out.println(a.map("company.users.-company"));
    }

    // @Test
    // public void testMapNavigationKeysOnly() {
    // Company company = new Company("Cinchapi");
    // User user = new User("Alice", "alice@example.com", company);
    // Map<String, Object> data = user.map("company.name");
    // Assert.assertEquals(ImmutableMap.of("name", "Cinchapi"),
    // data.get("company"));
    // }

    // @Test
    // public void testMapDenylistNavigationKey() {
    // Node a = new Node("A");
    // Node b = new Node("B");
    // Node c = new Node("C");
    // a.friends.add(b);
    // a.friends.add(c);
    // b.friends.add(c);
    // c.friends.add(a);
    // runway.save(a, b, c);
    // Association data = Association.ensure(a.map("-friends"));
    // System.out.println(data);
    // }
    //
    // class Node extends Record {
    //
    // public String label;
    // public List<Node> friends = Lists.newArrayList();
    // public Node bestFriend;
    //
    // public Node(String label) {
    // this.label = label;
    // }
    // }
    //
    //// @Test
    //// public void testContextualMappingDenylistNavigationWithinCollection() {
    //// Candidate a = new Candidate("a");
    //// Candidate b = new Candidate("b");
    //// School schoolA = new School("School");
    //// School schoolB = new School("School B");
    //// a.schools.add(schoolA);
    //// a.schools.add(schoolB);
    //// b.schools.add(schoolA);
    //// a.school = schoolC;
    //// runway.save(a,b,schoolA,schoolB, schoolC);
    //// Association data = Association.ensure(a.map("-school.enabled"));
    //// System.out.println(data);
    //// }
    //
    // class Student extends Record {
    // public Student(String name) {
    // this.name = name;
    // }
    //
    // public School school;
    //
    //
    // public String name;
    // }
    //
    // class Candidate extends Record {
    //
    // public Candidate(String name) {
    // this.name = name;
    // }
    //
    //
    // public String name;
    //
    // public Set<School> schools = new HashSet<>();
    //
    // }
    //
    // class School extends Record {
    //
    // public School(String name) {
    // this.name = name;
    // }
    //
    // public String name;
    //
    // public boolean enabled = true;
    //
    // @Computed
    // public Set<Candidate> candidates() {
    // return db.find(Candidate.class,
    // Criteria.where().key("schools").operator(Operator.LINKS_TO).value(id()));
    // }
    //
    // @Computed
    // public Set<Student> students() {
    // return db.find(Student.class,
    // Criteria.where().key("school").operator(Operator.LINKS_TO).value(id()));
    // }
    // }

}
