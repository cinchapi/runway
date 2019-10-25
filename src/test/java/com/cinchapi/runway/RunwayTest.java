/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.Random;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link Runway}.
 *
 * @author Jeff Nelson
 */
public class RunwayTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return "0.10.2";
    }

    private Runway runway;

    @Override
    public void beforeEachTest() {
        runway = Runway.builder().port(server.getClientPort()).build();
    }

    @Override
    public void afterEachTest() {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

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
        Assert.assertTrue(true); // lack of Exception means the test passes
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordDoesNotHaveDatabaseInterfaceReferenceWhenConstructedAndMultipleRunwaysExist() {
        Runway runway2 = Runway.builder().port(server.getClientPort())
                .environment("" + Time.now()).build();
        try {
            SuperAdmin sa = new SuperAdmin("Jeff Nelson", "foo", "bar");
            sa.db.find(User.class, Criteria.where().key("foo")
                    .operator(Operator.EQUALS).value("foo"));
            Assert.assertTrue(false); // lack of Exception means the test fails
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
            Assert.assertTrue(true); // lack of Exception means the test passes
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
    public void testSearch() {
        User a = new Manager("John Doern");
        User b = new Manager("Jane Doern");
        User c = new Manager("Liz James");
        runway.save(a, b, c);
        Set<Manager> records = runway.search(Manager.class, "n Doe", "name");
        Assert.assertEquals(2, records.size());
    }

    @Test
    public void testSearchMultipleKeys() {
        SuperAdmin a = new SuperAdmin("Jeff", "Goes to the store",
                "with you fuzzugng");
        SuperAdmin b = new SuperAdmin("Ashleah", "With fuzzugng", "Okay cool");
        runway.save(a, b);
        Set<SuperAdmin> records = runway.search(SuperAdmin.class, "zzug", "foo",
                "bar");
        Assert.assertEquals(2, records.size());
    }

    @Test
    public void testSearchSingleClass() {
        SuperAdmin a = new SuperAdmin("Jeff", "Goes to the store",
                "with you fuzzugng");
        Admin b = new Admin("Ashleah", "With fuzzugng");
        runway.save(a, b);
        Set<SuperAdmin> records = runway.search(SuperAdmin.class, "zzug", "foo",
                "bar");
        Assert.assertEquals(1, records.size());
    }

    @Test
    public void testSearchAcrossClassHierarchy() {
        SuperAdmin a = new SuperAdmin("Jeff", "Goes to the store",
                "with you fuzzugng");
        Admin b = new Admin("Ashleah", "With fuzzugng");
        runway.save(a, b);
        Set<User> records = runway.searchAny(User.class, "zzug", "foo", "bar");
        Assert.assertEquals(2, records.size());
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
    public void testLoadFromCache() throws Exception {
        runway.close();
        runway = Runway.builder().host("localhost").port(server.getClientPort())
                .cache(CacheBuilder.newBuilder().build()).build();
        Manager manager = new Manager("Jeff Nelson");
        manager.save();
        Manager a = runway.load(Manager.class, manager.id());
        Manager b = runway.load(Manager.class, manager.id());
        Assert.assertSame(a, b);
        Manager c = new Manager("Ashleah Nelson");
        c.save();
        Manager mgr = runway.load(Manager.class, manager.id());
        Assert.assertSame(mgr, a);
    }

    @Test
    public void testLoadAcrossClassHiearchyPerformsLazyLoad() throws Exception {
        Manager a = new Manager("A");
        Admin b = new Admin("A", "B");
        SuperAdmin c = new SuperAdmin("C", "C", "C");
        a.save();
        b.save();
        c.save();
        runway.close();
        Cache<Long, Record> cache = CacheBuilder.newBuilder().build();
        runway = Runway.builder().host("localhost").port(server.getClientPort())
                .cache(cache).build();
        Set<User> users = runway.loadAny(User.class);
        Assert.assertEquals(0, cache.size());
        Assert.assertEquals(ImmutableSet.of(a, b, c), users);
    }

    @Test
    public void testLoadDeferredReference() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        Assert.assertNull(jock.mentor.$ref());
        Assert.assertEquals("B", jock.mentor.get().name);
        Assert.assertNotNull(jock.mentor.$ref());
    }

    @Test
    public void testLoadDeferredReferenceDynamicGet() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        Jock mentor = jock.get("mentor");
        Assert.assertEquals("B", mentor.name);
    }

    @Test
    public void testLoadDeferredReferenceMap() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.mentor.get().friends.add(new DeferredReference<>(jock));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        jock.map().values().forEach(value -> {
            Assert.assertFalse(value instanceof DeferredReference);
        });
    }

    @Test
    public void testLoadDeferredReferenceExplicitMap() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.mentor.get().friends.add(new DeferredReference<>(jock));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        jock.map("mentor").values().forEach(value -> {
            Assert.assertFalse(value instanceof DeferredReference);
            Assert.assertTrue(value instanceof Jock);
        });
    }

    @Test
    public void testLoadDeferredExplictMapCollection() {
        Jock jock = new Jock("A");
        jock.friends.add(new DeferredReference<>(new Jock("B")));
        jock.friends.add(new DeferredReference<>(new Jock("C")));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        ((List<?>) jock.map("friends").get("friends")).forEach(value -> {
            Assert.assertFalse(value instanceof DeferredReference);
            Assert.assertTrue(value instanceof Jock);
        });
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
    public void testStreaminBulkSelect() {
        runway.bulkSelectTimeoutMillis = 0; // force streaming bulk select
        Set<Manager> expected = Sets.newHashSet();
        Reflection.set("recordsPerSelectBufferSize",
                new java.util.Random().nextInt(10) + 1, runway);
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            Manager manager = new Manager("" + Time.now());
            manager.save();
            expected.add(manager);
        }
        Set<Manager> actual = runway.load(Manager.class);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testStreamingBulkSelectSkipSupport() {
        runway.bulkSelectTimeoutMillis = 0; // force streaming bulk select
        Set<Manager> expected = Sets.newLinkedHashSet();
        Reflection.set("recordsPerSelectBufferSize",
                new java.util.Random().nextInt(10) + 1, runway);
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            Manager manager = new Manager("" + Time.now());
            manager.save();
            expected.add(manager);
        }
        Set<Manager> actual = runway.load(Manager.class);
        Set<Manager> $expected = Sets.newLinkedHashSet();
        int i = 0;
        int skip = expected.size() / 3;
        for (Manager manager : runway.load(Manager.class)) {
            if(i >= skip) {
                $expected.add(manager);
            }
            ++i;
        }
        actual = actual.stream().skip(skip).collect(Collectors.toSet());
        Assert.assertEquals($expected, actual);
    }

    @Test
    public void testFindAnyAndInstantiateBaseClass() {
        Manager user = new Manager("Jeff Nelson");
        user.save();
        User actual = runway.findAnyUnique(User.class, Criteria.where()
                .key("name").operator(Operator.LIKE).value("%Jeff%"));
        Assert.assertEquals(user, actual);
    }

    @Test
    public void testInherentCacheRefresh() throws Exception {
        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .cache(CacheBuilder.newBuilder().build()).build();
        Admin a1 = new Admin("A", "A");
        a1.save();
        a1 = runway.findUnique(Admin.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("A"));
        Runway runway2 = Runway.builder().port(server.getClientPort())
                .cache(CacheBuilder.newBuilder().build()).build();
        Admin a2 = runway2.findUnique(Admin.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("A"));
        Assert.assertEquals(a1, a2);
        a2.set("foo", "B");
        runway2.save(a2);
        a1 = runway.findUnique(Admin.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("A"));
        Assert.assertEquals("B", a1.foo);
    }

    @Test
    public void testInherentCacheRefreshAny() throws Exception {

        runway.close();
        runway = Runway.builder().port(server.getClientPort())
                .cache(CacheBuilder.newBuilder().build()).build();
        User a1 = new Admin("A", "A");
        a1.save();
        a1 = runway.findAnyUnique(User.class, Criteria.where().key("name")
                .operator(Operator.EQUALS).value("A"));
        Runway runway2 = Runway.builder().port(server.getClientPort())
                .cache(CacheBuilder.newBuilder().build()).build();
        try {
            User a2 = runway2.findAnyUnique(User.class, Criteria.where()
                    .key("name").operator(Operator.EQUALS).value("A"));
            Assert.assertEquals(a1, a2);
            a2.set("foo", "B");
            runway2.save(a2);
            a1 = runway.findAnyUnique(User.class, Criteria.where().key("name")
                    .operator(Operator.EQUALS).value("A"));
            Assert.assertEquals("B", a1.get("foo"));
        }
        finally {
            runway2.close();
        }
    }

    class Jock extends Record {

        public String name;
        public DeferredReference<Jock> mentor;
        public List<DeferredReference<Jock>> friends = Lists.newArrayList();

        public Jock(String name) {
            this.name = name;
        }

    }

    abstract class User extends Record {

        public final String name;

        public User(String name) {
            this.name = name;
        }

    }

    class Manager extends User {

        public Manager(String name) {
            super(name);
        }
    }

    class Admin extends User {

        public final String foo;

        /**
         * Construct a new instance.
         * 
         * @param name
         */
        public Admin(String name, String foo) {
            super(name);
            this.foo = foo;
        }

    }

    class SuperAdmin extends Admin {

        public final String bar;

        /**
         * Construct a new instance.
         * 
         * @param name
         * @param foo
         */
        public SuperAdmin(String name, String foo, String bar) {
            super(name, foo);
            this.bar = bar;
        }

    }

    class Person extends Record {

        public final String name;
        public Organization organization;

        public Person(String name, Organization organization) {
            this.name = name;
            this.organization = organization;
        }
    }

    class Organization extends Record {

        public final String name;

        public Organization(String name) {
            this.name = name;
        }

        public Set<Person> members() {
            return db.find(Person.class, Criteria.where().key("organization")
                    .operator(Operator.LINKS_TO).value(id()));
        }
    }

    abstract class Entity extends Record {
        String name;
    }

    class Human extends Entity {

    }

    class Team extends Record {

        Entity entity;
    }

}
