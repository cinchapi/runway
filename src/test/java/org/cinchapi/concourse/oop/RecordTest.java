package org.cinchapi.concourse.oop;

import java.util.Set;

import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.test.ClientServerTest;
import org.cinchapi.concourse.thrift.Operator;
import org.junit.Assert;
import org.junit.Test;

public class RecordTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return "0.4.0";
    }

    @Override
    public void beforeEachTest() {
        Record.setConnectionInformation("localhost",
                this.server.getClientPort(), "admin", "admin");
    }

    @Test
    public void testFoo() {
        Mock person = Record.create(Mock.class);
        person.name = "Jeffery Nelson";
        person.save();
        System.out.println(person);
    }
    
    class Mock extends Record {
        
        public String name;
        
    }

}
