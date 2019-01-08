package com.cinchapi.runway;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.collect.Continuation;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Required;
import com.cinchapi.runway.Unique;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
        return "latest";
    }

    @Override
    public void beforeEachTest() {
        runway = Runway.connect("localhost", server.getClientPort(), "admin",
                "admin");
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
        String json = shoe.json(true);
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

}
