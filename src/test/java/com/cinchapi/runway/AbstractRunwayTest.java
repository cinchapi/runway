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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Base test class for {@link Runway} tests. Contains all inner record types
 * shared across the split test files.
 * <p>
 * NOTE: The {@link Player} and {@link PointGuard} inner types are inherited
 * from {@link RunwayBaseClientServerTest}.
 *
 * @author Jeff Nelson
 */
public abstract class AbstractRunwayTest extends RunwayBaseClientServerTest {

    class Slayer extends Record {

        String name;

        @Override
        protected Map<String, Object> derived() {
            Map<String, Object> derived = new HashMap<>();
            derived.put("isAllStar", null);
            return derived;
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

    class Employee extends Person {

        public final Person boss;

        public Employee(String name, Organization organization, Person boss) {
            super(name, organization);
            this.boss = boss;
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

        @Override
        protected Map<String, Object> derived() {
            return ImmutableMap.of("members", members());
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

    class Adult extends Human {

        transient String firstName;
        transient String lastName;
        String email;

        public Adult(String name, String email) {
            this.name = name;
            this.email = email;
        }

        @Override
        protected void onLoad() {
            super.onLoad();

            String[] toks = name.split("\\s");
            firstName = toks[0];
            if(toks.length > 1) {
                lastName = toks[toks.length - 1];
            }
        }

    }

    class ScoreReport extends Record {
        public final String name;
        public final float score;

        public ScoreReport(String name, float score) {
            this.name = name;
            this.score = score;
        }
    }

    class Student extends Record {

        @Nullable
        /* package */ Float ccat;

        public Set<ScoreReport> scores = Sets.newLinkedHashSet();

        @Override
        public void onLoad() {
            if(ccat != null && scores.isEmpty()) {
                scores.add(new ScoreReport("ccat", ccat));
            }
        }

    }

    class Parent extends Human {

        Human child;
    }

    class NonParent extends Human {

    }

    class NonNonParent extends NonParent {

    }

    class Toddler extends Human {

        @Required
        int age;
    }

}
