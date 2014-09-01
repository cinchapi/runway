package org.cinchapi.concourse.orm;

import java.util.Set;

import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.orm.Record;
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
        person.age = 29;
        person.save();
              
        Mock person2 = Record.load(Mock.class, person.getId());
        person2.age = 45;
        
        Mock person3 = Record.load(Mock.class, person.getId());
        System.out.println(person2);
        System.out.println(person3);

//        Mock person2 = Record.create(Mock.class);
//        person2.name = "Jeffery Nelson";
//        
//        System.out.println(Record.find(Mock.class, Criteria.where().key("name")
//                .operator(Operator.EQUALS).value("Jeffery Nelson").build()));
////        System.out.println(client.browse(person.getId()));
//        Assert.assertFalse(person2.save());
    }

    class Mock extends Record {

        @Unique
        public String name;
        
        public Integer age;

    }

}
