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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.collect.Association;
import com.cinchapi.common.collect.Collections;
import com.cinchapi.common.collect.Continuation;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class RecordTest extends ClientServerTest {

    private Runway runway;

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

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
    public void testCannotAddDuplicateValuesForUniqueVariable() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        Assert.assertTrue(runway.save(person));

        Mock person2 = new Mock();
        person2.name = "Jeff Nelson";
        Assert.assertFalse(runway.save(person2));

        person2.name = "Jeffery Nelson";
        Assert.assertTrue(runway.save(person2));
    }

    @Test
    public void testCannotSaveNullValueForRequiredVariable() {
        Mock person = new Mock();
        person.age = 23;
        Assert.assertFalse(runway.save(person));
    }

    @Test
    public void testNoPartialSaveWhenRequiredVariableIsNull() {
        Mock person = new Mock();
        person.age = 23;
        runway.save(person);
        Assert.assertTrue(client.describe(person.id()).isEmpty());
    }

    @Test
    public void testBooleanIsNotStoredAsBase64() {
        Mock person = new Mock();
        person.name = "John Doe";
        person.age = 100;
        runway.save(person);
        person = runway.load(Mock.class, person.id());
        Assert.assertTrue(person.alive);
    }

    @Test
    public void testSetDynamicAttribute() {
        Mock person = new Mock();
        person.set("0_2_0", "foo");
        System.out.println(person);
    }

    @Test
    public void testLoadPopulatesFields() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 100;
        runway.save(person);
        person = runway.load(Mock.class, person.id());
        Assert.assertEquals("Jeff Nelson", person.name);
        Assert.assertEquals((int) 100, (int) person.age);
    }

    @Test
    public void testLoadAllRecordsFromClass() {
        int count = Random.getScaleCount();
        for (int i = 0; i < count; ++i) {
            Mock mock = new Mock();
            mock.name = Random.getSimpleString();
            mock.age = Random.getInt();
            runway.save(mock);
        }
        Assert.assertEquals(count, runway.load(Mock.class).size());
    }

    @Test
    public void testCanGetReadablePrivateField() {
        Mock mock = new Mock();
        Assert.assertTrue(mock.map().containsKey("bar"));
        Assert.assertNotNull(mock.get("bar"));
    }

    @Test
    public void testCannotGetNonReadablePrivateField() {
        Mock mock = new Mock();
        Assert.assertFalse(mock.map().containsKey("foo"));
        Assert.assertNull(mock.get("foo"));
    }

    @Test(expected = Exception.class)
    public void testLoadNonExistingRecord() {
        System.out.println(runway.load(Mock.class, -2));
    }

    @Test
    public void testNoNoArgConstructor() {
        Flock flock = new Flock("Jeff Nelson");
        runway.save(flock); // TODO: change
        System.out.println(runway.load(Flock.class, flock.id()));
    }

    @Test
    public void testCustomTypeAdapter() {
        Sock sock = new Sock("sock", new Dock("dock"));
        Assert.assertTrue(sock.json().contains("foo"));
    }

    @Test
    public void testLoadRecordWithCollectionOfLinks() {
        Lock lock = new Lock(ImmutableList.of(new Dock("dock")));
        lock.save();
        Assert.assertEquals(lock, runway.load(Lock.class, lock.id()));
    }

    @Test
    public void testCircularLinks() {
        Tock tock = new Tock();
        Stock stock = new Stock();
        tock.stocks.add(stock);
        stock.tock = tock;
        tock.save();
        Assert.assertTrue(true);
    }

    @Test
    public void testConcourseTypesJsonRepresentation() {
        Pock pock = new Pock("test");
        Assert.assertEquals(
                new GsonBuilder().setPrettyPrinting().create().toJson(
                        ImmutableMap.of("tag", "test", "id", pock.id())),
                pock.json());
    }

    @Test
    public void testLoadEnumWithinCollection() {
        HasEnumCollection hec = new HasEnumCollection();
        hec.save();
        hec = runway.load(HasEnumCollection.class, hec.id());
        hec.enumCollection
                .forEach(se -> Assert.assertTrue(se instanceof SampleEnum));
    }

    @Test
    public void testLoadTag() {
        Pock pock = new Pock("foo");
        pock.save();
        runway.load(Pock.class, pock.id()); // lack of Exception means we pass
    }

    @Test
    public void testLoadEmptyCollection() {
        Shoe shoe = new Shoe(ImmutableList.of());
        Assert.assertTrue(shoe.save());
        runway.load(Shoe.class, shoe.id()); // lack of Exception means we pass
    }

    @Test
    public void testJsonWithLink() {
        Stock stock = new Stock();
        stock.tock = new Tock();
        Assert.assertTrue(true); // lack of Exception means we pass
    }

    @Test
    public void testJsonSingleValueCollectionDefault() {
        Shoe shoe = new Shoe(ImmutableList.of("Nike"));
        String json = shoe.json();
        JsonElement elt = new JsonParser().parse(json);
        Assert.assertTrue(elt.getAsJsonObject().get("shoes").isJsonArray());
    }

    @Test
    public void testJsonSingleValueCollectionFlatten() {
        Shoe shoe = new Shoe(ImmutableList.of("Nike"));
        String json = shoe.json(SerializationOptions.builder()
                .flattenSingleElementCollections(true).build());
        JsonElement elt = new JsonParser().parse(json);
        Assert.assertTrue(elt.getAsJsonObject().get("shoes").isJsonPrimitive());
    }

    @Test
    public void testGetNoKeysReturnsAllData() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map();
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertTrue(data.containsKey("bar"));
        Assert.assertTrue(data.containsKey("city"));
    }

    @Test
    public void testGetNegativeFiltering() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map("-age", "-city");
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertTrue(data.containsKey("bar"));
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testGetNegativeAndPositiveFiltering() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map("-age", "alive", "-city");
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertFalse(data.containsKey("bar"));
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testGetPositiveFiltering() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 31;
        Map<String, Object> data = nock.map("age", "alive", "city");
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertFalse(data.containsKey("foo"));
        Assert.assertFalse(data.containsKey("bar"));
        Assert.assertTrue(data.containsKey("city"));
    }

    @Test
    public void testGetComputedValue() {
        Rock rock = new Rock();
        long start = System.currentTimeMillis();
        String state = rock.get("state");
        long end = System.currentTimeMillis();
        Assert.assertEquals("Georgia", state);
        Assert.assertTrue(end - start >= 1000);
    }

    @Test
    public void testComputedValueIncludedInGetAll() {
        Rock rock = new Rock();
        Map<String, Object> data = rock.map();
        Assert.assertTrue(data.containsKey("state"));
    }

    @Test
    public void testComputedValueNotComputedIfNotNecessary() {
        Bock bock = new Bock();
        Map<String, Object> data = bock.map("-state");
        System.out.println(data);
        Assert.assertFalse(data.containsKey("state"));
    }

    @Test
    public void testNonCollectionLinkFieldErroneousUpdateRepro() {
        Tock t1 = new Tock();
        Tock t2 = new Tock();
        Stock s1 = new Stock();
        s1.tock = t1;
        runway.save(t1, t2, s1);
        s1.tock = t2;
        s1.save();
        Set<Stock> stocks = runway.find(Stock.class, Criteria.where()
                .key("tock").operator(Operator.LINKS_TO).value(t1.id()));
        Assert.assertTrue(stocks.isEmpty());
        stocks = runway.find(Stock.class, Criteria.where().key("tock")
                .operator(Operator.LINKS_TO).value(t2.id()));
        Assert.assertEquals(1, stocks.size());
    }

    @Test
    public void testCollectionLinkFieldOverwriteRegression() {
        Tock tock = new Tock();
        tock.stocks.add(new Stock());
        tock.stocks.add(new Stock());
        tock.stocks.forEach(stock -> stock.tock = tock);
        tock.save();
        Tock t1 = runway.load(Tock.class, tock.id());
        Assert.assertEquals(2, t1.stocks.size());
    }

    @Test
    public void testIntrinsicMapDoesNotReturnComputedData() {
        Bock bock = new Bock();
        Map<String, Object> data = bock.intrinsic();
        Assert.assertFalse(data.containsKey("state"));
    }

    @Test
    public void testIntrinsicMapDoesNotReturnComputedDataEvenIfRequested() {
        Bock bock = new Bock();
        Map<String, Object> data = bock.intrinsic("state");
        Assert.assertFalse(data.containsKey("state"));
    }

    @Test
    public void testIntrinsicMapDoesNotReturnDerivedData() {
        Nock nock = new Nock();
        Map<String, Object> data = nock.intrinsic();
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testIntrinsicMapDoesNotReturnDerivedDataEvenIfRequested() {
        Nock nock = new Nock();
        Map<String, Object> data = nock.intrinsic("city");
        Assert.assertFalse(data.containsKey("city"));
    }

    @Test
    public void testInstrinsicMapAllNegativeFilters() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 100;
        Map<String, Object> data = nock.intrinsic("-age", "-name");
        Assert.assertFalse(data.containsKey("state"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertFalse(data.containsKey("name"));
        Assert.assertTrue(data.containsKey("alive"));
        Assert.assertTrue(data.containsKey("bar"));
    }

    @Test
    public void testIntrinsicMapPositiveAndNegativeFilters() {
        Nock nock = new Nock();
        nock.name = "Jeff Nelson";
        nock.age = 100;
        Map<String, Object> data = nock.intrinsic("-age", "name", "-bar");
        Assert.assertFalse(data.containsKey("state"));
        Assert.assertFalse(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertFalse(data.containsKey("bar"));
        Assert.assertFalse(data.containsKey("alive"));
    }

    @Test
    public void testJsonSerializationOptionsSerializeNulls() {
        Nock nock = new Nock();
        nock.age = 100;
        nock.name = null;
        String json = nock.json(SerializationOptions.builder()
                .flattenSingleElementCollections(true).serializeNullValues(true)
                .build(), "age", "name");
        Map<String, Object> data = new Gson().fromJson(json,
                new TypeToken<Map<String, Object>>() {}.getType());
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertTrue(data.containsKey("name"));
        Assert.assertEquals(nock.age.doubleValue(), data.get("age"));
        Assert.assertEquals(nock.name, data.get("name"));
    }

    @Test
    public void testJsonSerializationOptionsWithoutSerializeNulls() {
        Nock nock = new Nock();
        nock.age = 100;
        nock.name = null;
        String json = nock.json(
                SerializationOptions.builder()
                        .flattenSingleElementCollections(true).build(),
                "age", "name");
        Map<String, Object> data = new Gson().fromJson(json,
                new TypeToken<Map<String, Object>>() {}.getType());
        Assert.assertTrue(data.containsKey("age"));
        Assert.assertEquals(nock.age.doubleValue(), data.get("age"));
    }

    @Test
    public void testGetIdUseGetMethod() {
        Nock nock = new Nock();
        Assert.assertEquals((long) nock.id(), (long) nock.get("id"));
    }

    @Test
    public void testJsonAllDataWithNullValues() {
        Mock mock = new Mock();
        mock.name = "Mock";
        mock.age = null;
        String json = mock.json(SerializationOptions.builder()
                .serializeNullValues(true).build());
        System.out.println(json);
        Assert.assertTrue(json.contains("null"));
    }

    @Test
    public void testDefaultCompareToUsesId() {
        Nock a = new Nock();
        Nock b = new Nock();
        Assert.assertTrue(a.compareTo(b) < 0);
    }

    @Test
    public void testCompareToSingleKeyDefault() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 38;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "name") > 0);
        Assert.assertTrue(b.compareTo(c, "name") < 0);
        Assert.assertTrue(c.compareTo(d, "name") > 0);
        Assert.assertTrue(a.compareTo(c, "name") < 0); // When equal, the record
                                                       // id is used as a tie
                                                       // breaker
    }

    @Test
    public void testCompareToSingleKeyDescending() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 38;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "<name") < 0);
        Assert.assertTrue(b.compareTo(c, "<name") > 0);
        Assert.assertTrue(c.compareTo(d, "<name") < 0);
        Assert.assertTrue(a.compareTo(c, "<name") < 0); // When equal, the
                                                        // record
                                                        // id is used as a tie
                                                        // breaker
    }

    @Test
    public void testCompareToSingleKeyAscending() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 38;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "name") > 0);
        Assert.assertTrue(b.compareTo(c, "name") < 0);
        Assert.assertTrue(c.compareTo(d, "name") > 0);
        Assert.assertTrue(a.compareTo(c, "name") < 0); // When equal, the record
                                                       // id is used as a tie
                                                       // breaker
    }

    @Test
    public void testCompareToMultiKeys() {
        Mock a = new Mock();
        a.name = "Mary";
        a.age = 40;
        Mock b = new Mock();
        b.name = "Barb";
        b.age = 20;
        Mock c = new Mock();
        c.name = "Mary";
        c.age = 41;
        Mock d = new Mock();
        d.name = "Alice";
        d.age = 10;
        Assert.assertTrue(a.compareTo(b, "name age") > 0);
        Assert.assertTrue(b.compareTo(c, "name age") < 0);
        Assert.assertTrue(c.compareTo(d, "name age") > 0);
        Assert.assertTrue(a.compareTo(c, "name <age") > 0);
    }

    @Test
    public void testThrowSupressedExceptions() {
        Mock a = new Mock();
        a.name = "Bob";
        Mock b = new Mock();
        b.name = "Bob";
        a.save();
        if(b.save()) {
            Assert.fail();
        }
        else {
            try {
                b.throwSupressedExceptions();
                Assert.fail();
            }
            catch (RuntimeException e) {
                Assert.assertTrue(
                        e.getMessage().startsWith("name must be unique"));
            }

        }
    }

    @Test
    public void testCompareToUsingNullValuesDoesNotNPE() {
        Mock a = new Mock();
        Mock b = new Mock();
        a.compareTo(b, "name");
        Assert.assertTrue(true); // lack of Exception means that the test passes
    }

    @Test
    public void testSavingRecordWithLinksDoesNotCreateExtraneousRevisions() {
        Sock a = new Sock("a", new Dock("a"));
        a.save();
        Concourse concourse = Concourse.connect("localhost",
                server.getClientPort(), "admin", "admin");
        int expected = concourse.review(a.id()).size();
        a.save();
        int actual = concourse.review(a.id()).size();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSavingRecordWithChangeToLink() {
        Sock a = new Sock("a", new Dock("a"));
        a.save();
        Dock d = new Dock("b");
        Reflection.set("dock", d, a);
        a.save();
        a = runway.load(Sock.class, a.id());
        Assert.assertEquals(d, a.dock);
    }

    @Test
    public void testSetDynamicValue() {
        Flock flock = new Flock("flock");
        String key = Random.getSimpleString();
        flock.set(key, 1);
        Assert.assertEquals(1, (int) flock.get(key));
        Assert.assertTrue(flock.map().containsKey(key));
    }

    @Test
    public void testJsonCycleDetection() {

        Gson gson = new GsonBuilder().setPrettyPrinting()
                .registerTypeHierarchyAdapter(Record.class,
                        new TypeAdapter<Record>() {

                            @Override
                            public void write(JsonWriter out, Record value)
                                    throws IOException {
                                out.jsonValue(value.json(SerializationOptions
                                        .builder().serializeNullValues(true)
                                        .build()));
                            }

                            @Override
                            public Record read(JsonReader in)
                                    throws IOException {
                                throw new UnsupportedOperationException();
                            }

                        })
                .create();
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        b.friends.add(a);
        b.friends.add(c);
        c.friends.add(a);
        c.friends.add(b);
        String json = gson.toJson(ImmutableList.of(c, a, b));
        System.out.println(json);
        Assert.assertTrue(true); // lack of StackOverflowExceptions means we
                                 // pass

    }

    @Test
    public void testSaveDeferredReference() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        long id = jock.mentor.get().id();
        jock.save();
        Assert.assertEquals(Link.to(id), client.get("mentor", jock.id()));
    }

    @Test
    public void testSaveDeferredReferenceCollection() {
        Jock jock = new Jock("A");
        jock.friends.add(new DeferredReference<>(new Jock("B")));
        jock.friends.add(new DeferredReference<>(new Jock("C")));
        jock.friends.add(new DeferredReference<>(new Jock("D")));
        Set<Link> expected = jock.friends.stream()
                .map(ref -> Link.to(ref.get().id()))
                .collect(Collectors.toSet());
        jock.save();
        Assert.assertEquals(expected, client.select("friends", jock.id()));
    }

    @Test
    public void testGetNavigation() {
        Sock sock = new Sock("A", new Dock("B"));
        Assert.assertEquals("B", sock.get("dock.dock"));
    }

    @Test
    public void testGetNavigationCollection() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        Assert.assertEquals(ImmutableList.of("b", "c"), a.get("friends.label"));
    }

    @Test
    public void testMapNavigationCollection() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        Assert.assertEquals(
                ImmutableMap.of("friends",
                        ImmutableList.of(ImmutableMap.of("label", "b"),
                                ImmutableMap.of("label", "c"))),
                a.map("friends.label"));
    }

    @Test
    public void testGetNavigationCollectionNested() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        Node d = new Node("d");
        Node e = new Node("e");
        b.friends.add(d);
        c.friends.add(e);
        c.friends.add(a);
        Assert.assertEquals(
                ImmutableList.of(ImmutableList.of("d"),
                        ImmutableList.of("e", "a")),
                a.get("friends.friends.label"));
    }

    @Test
    public void testMapNavigationCollectionNested() {
        Node a = new Node("a");
        Node b = new Node("b");
        Node c = new Node("c");
        a.friends.add(b);
        a.friends.add(c);
        Node d = new Node("d");
        Node e = new Node("e");
        b.friends.add(d);
        c.friends.add(e);
        c.friends.add(a);
        Assert.assertEquals(
                ImmutableMap
                        .of("friends",
                                ImmutableList.of(
                                        ImmutableMap.of("friends",
                                                ImmutableList
                                                        .of(ImmutableMap.of(
                                                                "label", "d"))),
                                        ImmutableMap.of("friends",
                                                ImmutableList.of(
                                                        ImmutableMap.of("label",
                                                                "e"),
                                                        ImmutableMap.of("label",
                                                                "a"))))),
                a.map("friends.friends.label"));
    }

    @Test
    public void testMapNavigation() {
        Sock sock = new Sock("A", new Dock("B"));
        Assert.assertEquals(ImmutableSet.of("dock.dock"),
                Association.of(sock.map("dock.dock")).flatten().keySet());
        Assert.assertEquals(
                ImmutableMap.of("dock", ImmutableMap.of("dock", "B")),
                sock.map("dock.dock"));
    }

    @Test
    public void testMapNavigationComplex() {
        Company company = new Company("Cinchapi");
        User a = new User("a", "a@a.com", company);
        User b = new User("b", "b@b.com", company);
        runway.save(company, a, b);
        Map<String, Object> data = company.map("users.name", "users.email");
        Set<?> expected = ImmutableSet.of(
                ImmutableMap.of("name", "a", "email", "a@a.com"),
                ImmutableMap.of("name", "b", "email", "b@b.com"));
        Set<?> actual = Collections
                .ensureSet((Collection<?>) data.get("users"));
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSetValueToNullRemovesFromDatabase() {
        Mock mock = new Mock();
        mock.alive = true;
        mock.age = 10;
        mock.name = "Mock";
        mock.bar = false;
        mock.save();
        mock.age = null;
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        Assert.assertNull(mock.age);
    }

    @Test
    public void testCannotDynamicallySetIntrinsicAttributeWithInvalidType() {
        Mock mock = new Mock();
        try {
            mock.set("age", "10");
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertTrue(true);
        }
        Assert.assertNotEquals("10", mock.age);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testRequiredConstraintEnforcedOnExplicitLoad() {
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.save();
        Concourse concourse = Concourse.at().port(server.getClientPort()).connect();
        try {
            concourse.clear("name", mock.id());
        }
        finally {
            concourse.close();
        }
        mock = runway.load(Mock.class, mock.id());     
    }
    
    @Test(expected = IllegalStateException.class)
    public void testRequiredConstraintEnforcedOnImplicitLoad() {
        for(int i = 0; i < Random.getScaleCount(); ++i) {
            Mock m = new Mock();
            m.name = Random.getSimpleString();
            m.age = i;
            m.save();
        }
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.save();
        Concourse concourse = Concourse.at().port(server.getClientPort()).connect();
        try {
            concourse.clear("name", mock.id());
        }
        finally {
            concourse.close();
        }    
        Set<Mock> mocks = runway.find(Mock.class, Criteria.where().key("age").operator(Operator.LESS_THAN_OR_EQUALS).value(32));
        for(Mock m : mocks) {
            System.out.println(m.name);
        }
    }
    
    @Test
    public void testDefaultRealms() {
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        Assert.assertEquals(ImmutableSet.of(), mock.realms());
    }
    
    @Test
    public void testAddRealm() {
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.addRealm("test");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        Assert.assertEquals(ImmutableSet.of("test"), mock.realms());
    }
    
    @Test
    public void testAddMultiRealms() {
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.addRealm("test");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        mock.addRealm("prod");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        Assert.assertEquals(ImmutableSet.of("test", "prod"), mock.realms());
    }
    
    @Test
    public void testRemoveRealm() {
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.addRealm("test");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        mock.addRealm("prod");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        mock.removeRealm("test");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        Assert.assertEquals(ImmutableSet.of("prod"), mock.realms());
    }
    
    @Test
    public void testRemoveAllRealms() {
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.addRealm("test");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        mock.addRealm("prod");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        mock.removeRealm("test");
        mock.removeRealm("prod");
        mock.save();
        mock = runway.load(Mock.class, mock.id());
        Assert.assertEquals(ImmutableSet.of(), mock.realms());
    }
    
    @Test
    public void testGetPaths() {
        Set<String> paths = Record.getPaths(Gock.class);
        long count = 0;
        
        /*
         * Detect a path that would by cyclic and terminate it 
         */
        count = paths.stream().filter(path -> path.startsWith("gock")).count();
        Assert.assertEquals(1, count);
        count = paths.stream().filter(path -> path.startsWith("jock.testy")).count();
        Assert.assertEquals(1, count);
        count = paths.stream().filter(path -> path.startsWith("testy")).count();
        Assert.assertEquals(1, count);
        
        /*
         * Collection of Links is terminated (e.g. no numeric expansion paths)
         */
        count = paths.stream().filter(path -> path.startsWith("stock.tock.stocks")).count();
        Assert.assertEquals(1, count);
        count = paths.stream().filter(path -> path.startsWith("node.friends")).count();
        Assert.assertEquals(1, count);
        count = paths.stream().filter(path -> path.startsWith("jock.friends")).count();
        Assert.assertEquals(1, count);
        count = paths.stream().filter(path -> path.startsWith("friends")).count();
        Assert.assertEquals(1, count);
        
        /*
         * Expected Paths
         */
        Assert.assertTrue(paths.contains("stock.tock.zombie"));
        Assert.assertTrue(paths.contains("node.label"));
        Assert.assertTrue(paths.contains("user.name"));
        Assert.assertTrue(paths.contains("user.email"));
        Assert.assertTrue(paths.contains("user.company.name"));
        Assert.assertTrue(paths.contains("sock.sock"));
        Assert.assertTrue(paths.contains("sock.dock.dock"));
        Assert.assertTrue(paths.contains("jock.name"));
        Assert.assertTrue(paths.contains("jock2.name"));
        Assert.assertTrue(paths.contains("name"));
        
        /*
         * Deferred Reference Isn't Expanded
         */
        count = paths.stream().filter(path -> path.startsWith("jock.mentor")).count();
        Assert.assertEquals(1, count);
        count = paths.stream().filter(path -> path.startsWith("mentor")).count();
        Assert.assertEquals(1, count);
        
        System.out.println(paths);       
    }
    
//    @Test
//    public void testReconcileCollectionPrimitiveValues() {
//        Shoe shoe = new Shoe(Lists.newArrayList("A", "B", "C"));
//        shoe.save();
//        shoe.shoes = Lists.newArrayList("B", "D", "A");
//        shoe.save();
//        shoe = runway.load(Shoe.class, shoe.id());
//        System.out.println(shoe.shoes.getClass());
//        Assert.assertEquals(Lists.newArrayList("B", "D", "A"), shoe.shoes);
//    }

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

        @Override
        public Map<String, Object> derived() {
            return ImmutableMap.of("city", "Atlanta");
        }

    }

    class Rock extends Nock {

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

        public Gock(String name) {
            super(name);
        }
        
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

}
