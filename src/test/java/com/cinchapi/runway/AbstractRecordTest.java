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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.cinchapi.common.collect.Continuation;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Base test class for {@link Record} tests. Contains all inner record types
 * shared across the split test files.
 *
 * @author Jeff Nelson
 */
public abstract class AbstractRecordTest extends RunwayBaseClientServerTest {

    class Node extends Record {

        public String label;
        public List<Node> friends = Lists.newArrayList();

        public Node(String label) {
            this.label = label;
        }
    }

    class Mock extends Record {

        @Unique
        @Required
        public String name;

        public Integer age;

        public boolean alive = true;

        @SuppressWarnings("unused")
        private boolean foo = false;

        @Readable
        private boolean bar = false;

    }

    class Flock extends Record {

        public final String name;

        public Flock(String name) {
            this.name = name;
        }
    }

    class Sock extends Record {

        public final String sock;
        public final Dock dock;

        public Sock(String sock, Dock dock) {
            this.sock = sock;
            this.dock = dock;
        }

        @Override
        public Map<Class<?>, TypeAdapter<?>> typeAdapters() {
            return ImmutableMap.of(Dock.class, new TypeAdapter<Dock>() {

                @Override
                public void write(JsonWriter out, Dock value)
                        throws IOException {
                    out.value("foo");
                }

                @Override
                public Dock read(JsonReader in) throws IOException {
                    return null;
                }

            });
        }
    }

    class Lock extends Record {
        public final List<Dock> docks;

        public Lock(List<Dock> docks) {
            this.docks = docks;
        }
    }

    class Dock extends Record {

        public final String dock;

        public Dock(String dock) {
            this.dock = dock;
        }
    }

    class Tock extends Record {
        public List<Stock> stocks = Lists.newArrayList();
        public boolean zombie = false;

        public Tock() {

        }
    }

    class Stock extends Record {
        public Tock tock;
    }

    class Pock extends Record {
        public Tag tag;

        public Pock(String tag) {
            this.tag = Tag.create(tag);
        }

        @Override
        public Map<Class<?>, TypeAdapter<?>> typeAdapters() {
            return ImmutableMap.of(Tag.class, new TypeAdapter<Tag>() {

                @Override
                public void write(JsonWriter out, Tag value)
                        throws IOException {
                    out.value(tag.toString());
                }

                @Override
                public Tag read(JsonReader in) throws IOException {
                    return null;
                }

            });
        }

    }

    enum SampleEnum {
        FOO
    }

    class HasEnumCollection extends Record {
        Set<SampleEnum> enumCollection = Sets.newHashSet();

        public HasEnumCollection() {
            enumCollection.add(SampleEnum.FOO);
        }
    }

    class Shoe extends Record {

        public Shoe(List<String> shoes) {
            this.shoes = shoes;
        }

        List<String> shoes;
        boolean ignore = false;
    }

    class Nock extends Mock {

        @Derived
        public String zipcode() {
            return "30327";
        }

        @Derived("area")
        public String city() {
            return "Atlanta";
        }

        @Override
        public Map<String, Object> derived() {
            return ImmutableMap.of("city", "Atlanta");
        }

    }

    class Rock extends Nock {

        @Computed("county")
        public String county() {
            long stop = System.currentTimeMillis() + 1000;
            while (System.currentTimeMillis() < stop) {
                continue;
            }
            return "Fulton";
        }

        @Override
        public Map<String, Supplier<Object>> computed() {
            return ImmutableMap.of("state", () -> {
                long stop = System.currentTimeMillis() + 1000;
                while (System.currentTimeMillis() < stop) {
                    continue;
                }
                return "Georgia";
            });
        }
    }

    class Bock extends Nock {
        @Override
        public Map<String, Supplier<Object>> computed() {
            return ImmutableMap.of("state",
                    () -> Continuation.of(UUID::randomUUID));
        }
    }

    class Jock extends Record {

        public Gock testy;
        public String name;
        public DeferredReference<Jock> mentor;
        public List<DeferredReference<Jock>> friends = Lists.newArrayList();

        public Jock(String name) {
            this.name = name;
        }

    }

    class Gock extends Jock {

        public Stock stock;
        public Node node;
        public User user;
        public Sock sock;
        public Gock gock;
        public Jock jock;
        public Jock jock2;
        public Hock hock;
        public Qock qock;

        public Gock(String name) {
            super(name);
        }

    }

    class Hock extends Record {

        public String a;

    }

    class Oock extends Hock {

    }

    class Qock extends Record {

        public String a;

    }

    class Vock extends Qock {

        public String b;

    }

    class User extends Record {
        String name;
        String email;
        Company company;

        public User(String name, String email, Company company) {
            this.name = name;
            this.email = email;
            this.company = company;
        }
    }

    class Company extends Record {

        String name;

        public Company(String name) {
            this.name = name;
        }

        public Set<User> users() {
            return db.find(User.class, Criteria.where().key("company")
                    .operator(Operator.LINKS_TO).value(id()));
        }

        @Override
        public Map<String, Supplier<Object>> computed() {
            return ImmutableMap.of("users", () -> users());
        }
    }

    class HasBeforeSaveHook extends Record {

        String value;
        HasBeforeSaveHook child;
        transient int saves = 0;

        public HasBeforeSaveHook(String value) {
            this.value = value;
        }

        @Override
        public void beforeSave() {
            ++saves;
        }
    }

}
