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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link Record} validation constraints, realms, and type checking.
 *
 * @author Jeff Nelson
 */
public class RecordConstraintTest extends AbstractRecordTest {

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

    @Test(expected = IllegalStateException.class)
    public void testRequiredConstraintEnforcedOnExplicitLoad() {
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.save();
        Concourse concourse = Concourse.at().port(server.getClientPort())
                .connect();
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
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            Mock m = new Mock();
            m.name = Random.getSimpleString();
            m.age = i;
            m.save();
        }
        Mock mock = new Mock();
        mock.name = "Jeff Nelson";
        mock.age = 32;
        mock.save();
        Concourse concourse = Concourse.at().port(server.getClientPort())
                .connect();
        try {
            concourse.clear("name", mock.id());
        }
        finally {
            concourse.close();
        }
        Set<Mock> mocks = runway.find(Mock.class, Criteria.where().key("age")
                .operator(Operator.LESS_THAN_OR_EQUALS).value(32));
        for (Mock m : mocks) {
            System.out.println(m.name);
        }
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

}
