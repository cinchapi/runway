package com.cinchapi.runway;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Required;
import com.cinchapi.runway.Unique;

public class RecordTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return "latest";
    }

    @Override
    public void beforeEachTest() {
        Record.setConnectionInformation("localhost",
                this.server.getClientPort(), "admin", "admin");
    }
    
    @Test
    public void testCannotAddDuplicateValuesForUniqueVariable(){
        Mock person = Record.create(Mock.class);
        person.name = "Jeff Nelson";
        Assert.assertTrue(person.save());
        
        Mock person2 = Record.create(Mock.class);
        person2.name = "Jeff Nelson";
        Assert.assertFalse(person2.save());
        
        person2.name = "Jeffery Nelson";
        Assert.assertTrue(person2.save());      
    }
    
    @Test
    public void testCannotSaveNullValueForRequiredVariable(){
        Mock person = Mock.create(Mock.class);
        person.age = 23;
        Assert.assertFalse(person.save());
    }
    
    @Test
    public void testNoPartialSaveWhenRequiredVariableIsNull(){
        Mock person = Mock.create(Mock.class);
        person.age = 23;
        person.save();
        Assert.assertTrue(client.describe(person.getId()).isEmpty());
    }
    
    @Test
    public void testBooleanIsNotStoredAsBase64(){
        Mock person = Mock.create(Mock.class);
        person.name = "John Doe";
        person.age = 100;
        person.save();
        long id = person.getId();
        person = Mock.load(Mock.class, id);
        Assert.assertTrue(person.alive);
    }

    class Mock extends Record {

        @Unique
        @Required
        public String name;
        
        public Integer age;
        
        public boolean alive = true;

    }

}
