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
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Tests for {@link Record} JSON serialization, type adapters, and serialization
 * options.
 *
 * @author Jeff Nelson
 */
public class RecordJsonTest extends AbstractRecordTest {

    @Test
    public void testJsonWithLink() {
        Stock stock = new Stock();
        stock.tock = new Tock();
        Assert.assertTrue(true); // lack of Exception means
                                 // we pass
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
        Assert.assertTrue(true); // lack of
                                 // StackOverflowExceptions
                                 // means we pass

    }

    @Test
    public void testConcourseTypesJsonRepresentation() {
        Pock pock = new Pock("test");
        JsonElement expected = new JsonParser()
                .parse(new GsonBuilder().setPrettyPrinting().create().toJson(
                        ImmutableMap.of("tag", "test", "id", pock.id())));
        JsonElement actual = new JsonParser().parse(pock.json());
        Assert.assertEquals(expected, actual);
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
        runway.load(Pock.class, pock.id()); // lack of
                                            // Exception
                                            // means we pass
    }

    @Test
    public void testLoadEmptyCollection() {
        Shoe shoe = new Shoe(ImmutableList.of());
        Assert.assertTrue(shoe.save());
        runway.load(Shoe.class, shoe.id()); // lack of
                                            // Exception
                                            // means we pass
    }

    @Test
    public void testCustomTypeAdapter() {
        Sock sock = new Sock("sock", new Dock("dock"));
        Assert.assertTrue(sock.json().contains("foo"));
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

}
