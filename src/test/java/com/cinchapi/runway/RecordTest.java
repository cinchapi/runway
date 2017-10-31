package com.cinchapi.runway;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.util.Random;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Required;
import com.cinchapi.runway.Unique;

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
        Assert.assertTrue(mock.getData().containsKey("bar"));
        Assert.assertTrue(mock.getData("bar").containsKey("bar"));
    }
    
    @Test
    public void testCannotGetNonReadablePrivateField() {
        Mock mock = new Mock();
        Assert.assertFalse(mock.getData().containsKey("foo"));
        Assert.assertFalse(mock.getData("foo").containsKey("foo"));
    }
    
    @Test(expected = Exception.class)
    public void testLoadNonExistingRecord() {
        System.out.println(runway.load(Mock.class, -2));
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

}
