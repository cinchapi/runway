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

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link Runway} loading, type resolution, database references,
 * introspection, and sorting.
 *
 * @author Jeff Nelson
 */
public class RunwayLoadResolutionTest extends AbstractRunwayTest {

    @Test
    public void testLoadAny() {
        Manager a = new Manager("A");
        Admin b = new Admin("A", "B");
        SuperAdmin c = new SuperAdmin("C", "C", "C");
        a.save();
        b.save();
        c.save();
        Set<User> users = runway.loadAny(User.class);
        Assert.assertEquals(3, users.size());
        Assert.assertEquals(ImmutableSet.of(a.id(), b.id(), c.id()),
                users.stream().map(User::id).collect(Collectors.toSet()));
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testFindAny() {
        Manager a = new Manager("Jeff");
        Admin b = new Admin("John", "B");
        SuperAdmin c = new SuperAdmin("Jeff", "C", "C");
        a.save();
        b.save();
        c.save();
        Set<User> users = runway.findAny(User.class, Criteria.where()
                .key("name").operator(Operator.EQUALS).value("Jeff"));
        Assert.assertTrue(users.contains(a));
        Assert.assertFalse(users.contains(b));
        Assert.assertTrue(users.contains(c));
        Set<Admin> admins = runway.findAny(Admin.class, Criteria.where()
                .key("name").operator(Operator.EQUALS).value("Jeff"));
        Assert.assertFalse(admins.contains(a));
        Assert.assertFalse(admins.contains(b));
        Assert.assertTrue(admins.contains(c));
    }

    @Test(expected = DuplicateEntryException.class)
    public void testFindAnyUniqueCatchesDuplicateAcrossHierarchy() {
        Manager a = new Manager("Jeff");
        Admin b = new Admin("John", "B");
        SuperAdmin c = new SuperAdmin("Jeff", "C", "C");
        a.save();
        b.save();
        c.save();
        runway.findAnyUnique(User.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("Jeff"));
    }

    @Test
    public void testFindAnyUnique() {
        Manager a = new Manager("Jeff");
        Admin b = new Admin("John", "B");
        SuperAdmin c = new SuperAdmin("Jeff", "C", "C");
        a.save();
        b.save();
        c.save();
        Admin actual = runway.findAnyUnique(Admin.class, Criteria.where()
                .key("name").operator(Operator.EQUALS).value("Jeff"));
        Assert.assertEquals(c, actual);
    }

    @Test
    public void testLoadRecordUsingParentTypeResolvesToActualType() {
        SuperAdmin sa = new SuperAdmin("Jeff Nelson", "foo", "bar");
        sa.save();
        User admin = runway.load(User.class, sa.id());
        Assert.assertEquals(SuperAdmin.class, admin.getClass());
        Assert.assertEquals(admin.name, sa.name);
        Assert.assertEquals(admin.get("foo"), sa.foo);
        Assert.assertEquals(admin.get("bar"), sa.bar);
    }

    @Test
    public void testRecordHasDatabaseInterfaceReferenceWhenConstructedAndOnlyOneRunway() {
        SuperAdmin sa = new SuperAdmin("Jeff Nelson", "foo", "bar");
        sa.db.find(User.class, Criteria.where().key("foo")
                .operator(Operator.EQUALS).value("foo"));
        Assert.assertTrue(true); // lack of Exception means
                                 // the test passes
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordDoesNotHaveDatabaseInterfaceReferenceWhenConstructedAndMultipleRunwaysExist() {
        Runway runway2 = Runway.builder().port(server.getClientPort())
                .environment("" + Time.now()).build();
        try {
            SuperAdmin sa = new SuperAdmin("Jeff Nelson", "foo", "bar");
            sa.db.find(User.class, Criteria.where().key("foo")
                    .operator(Operator.EQUALS).value("foo"));
            Assert.assertTrue(false); // lack of Exception
                                      // means the test
                                      // fails
        }
        finally {
            try {
                runway2.close();
            }
            catch (Exception e) {}
        }
    }

    @Test
    public void testLoadedRecordAlwaysHasDatabaseInstanceReference() {
        SuperAdmin sa = new SuperAdmin("Jeff Nelson", "foo", "bar");
        sa.save();
        Runway runway2 = Runway.builder().port(server.getClientPort())
                .environment("" + Time.now()).build();
        try {
            SuperAdmin loaded = runway.findAnyUnique(SuperAdmin.class,
                    Criteria.where().key("name").operator(Operator.EQUALS)
                            .value("Jeff Nelson"));
            loaded.db.find(User.class, Criteria.where().key("foo")
                    .operator(Operator.EQUALS).value("foo"));
            Assert.assertTrue(true); // lack of Exception
                                     // means the test
                                     // passes
        }
        finally {
            try {
                runway2.close();
            }
            catch (Exception e) {}
        }
    }

    @Test
    public void testIntrospectedData() {
        Organization o1 = new Organization("o1");
        Organization o2 = new Organization("o2");
        Person p1 = new Person("P1", o1);
        runway.save(o1, o2, p1);
        Assert.assertEquals(ImmutableSet.of(p1), o1.members());
        Person p2 = new Person("P2", o1);
        p2.save();
        Assert.assertEquals(ImmutableSet.of(p1, p2), o1.members());
        Person p3 = new Person("P3", o2);
        p3.save();
        Assert.assertEquals(ImmutableSet.of(p1, p2), o1.members());
        Assert.assertEquals(ImmutableSet.of(p3), o2.members());
        p1.organization = o2;
        p1.save();
        Assert.assertEquals(ImmutableSet.of(p2), o1.members());
        Assert.assertEquals(ImmutableSet.of(p1, p3), o2.members());
    }

    @Test
    public void testLoadSort() {
        Manager a = new Manager("Z");
        Manager b = new Manager("S");
        Manager c = new Manager("A");
        Manager d = new Manager("V");
        runway.save(a, b, c, d);
        Set<Manager> managers = runway.load(Manager.class, Order.by("name"));
        Iterator<Manager> expectedIt = ImmutableList.of(c, b, d, a).iterator();
        Iterator<Manager> actualIt = managers.iterator();
        while (expectedIt.hasNext()) {
            Manager expected = expectedIt.next();
            Manager actual = actualIt.next();
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testLoadLinkWithAbstractClassReferenceRepro() {
        Human human = new Human();
        human.name = "Jeff Nelson";
        Team team = new Team();
        team.entity = human;
        team.save();
        team = runway.load(Team.class, team.id());
        Assert.assertEquals(human, team.entity);
    }

    @Test
    public void testLoadRecordWithLinks() {
        Organization org = new Organization("Org");
        Person person = new Person("Jeff Nelson", org);
        Employee employee = new Employee("John Doe", org, person);
        employee.save();
        employee = runway.load(Employee.class, employee.id());
        System.out.println(employee);
        // TODO: finish by asserting some things
    }

}
