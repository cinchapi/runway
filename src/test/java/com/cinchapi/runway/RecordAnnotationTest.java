/*
 * Copyright (c) 2013-2020 Cinchapi Inc.
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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.time.Time;
import com.google.common.collect.ImmutableMap;

/**
 * Unit tests for {@link Record} annotations.
 *
 * @author Jeff Nelson
 */
public class RecordAnnotationTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

    Runway db;

    @Override
    public void beforeEachTest() {
        db = Runway.builder().port(server.getClientPort()).build();
    }

    @Override
    public void afterEachTest() {
        try {
            db.close();
        }
        catch (Exception e) {
            CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    @Test
    public void testMultiUniqueConstraint() {
        Student student = new Student();
        student.name = "Jeff Nelson";

        Job job = new Job();
        job.title = "Software Engineer";

        Invitation invitation = new Invitation();
        invitation.student = student;
        invitation.job = job;
        invitation.timestamp = Timestamp.now();
        invitation.save();

        invitation = new Invitation();
        invitation.student = student;
        invitation.job = job;
        invitation.timestamp = Timestamp.now();
        Assert.assertFalse(invitation.save());
        try {
            invitation.throwSupressedExceptions();
            Assert.fail();
        }
        catch (Exception e) {
            Assert.assertEquals("foo must be unique", e.getMessage());
        }
    }

    @Test
    public void testUniqueConstraintPrimitiveLong() {
        Model a = new Model();
        a.age = 10L;
        Model b = new Model();
        b.age = 10L;
        Model c = new Model();
        c.age = 11L;
        ImmutableMap.of(a, true, b, false, c, true)
                .forEach((model, expected) -> {
                    boolean actual = model.save();
                    if(expected.equals(actual)) {
                        System.out.println("PASS: " + model.id());
                    }
                    else {
                        System.out.println("FAIL: " + model.id());
                        model.throwSupressedExceptions();
                        Assert.fail();
                    }
                });
    }

    @Test
    public void testUniqueConstraintPrimitiveString() {
        Model a = new Model();
        a.name = "Jeff";
        Model b = new Model();
        b.name = "Jeff";
        Model c = new Model();
        c.name = "Jeffery";
        ImmutableMap.of(a, true, b, false, c, true)
                .forEach((model, expected) -> {
                    boolean actual = model.save();
                    if(expected.equals(actual)) {
                        System.out.println("PASS: " + model.id());
                    }
                    else {
                        System.out.println("FAIL: " + model.id());
                        model.throwSupressedExceptions();
                        Assert.fail();
                    }
                });
    }

    @Test
    public void testUniqueConstraintPrimitiveTag() {
        Model a = new Model();
        a.description = Tag.create("Jeff");
        Model b = new Model();
        b.description = Tag.create("Jeff");
        Model c = new Model();
        c.description = Tag.create("Jeffery");
        ImmutableMap.of(a, true, b, false, c, true)
                .forEach((model, expected) -> {
                    boolean actual = model.save();
                    if(expected.equals(actual)) {
                        System.out.println("PASS: " + model.id());
                    }
                    else {
                        System.out.println("FAIL: " + model.id());
                        model.throwSupressedExceptions();
                        Assert.fail();
                    }
                });
    }

    @Test
    public void testUniqueConstraintPrimitiveSerializable() {
        Model a = new Model();
        a.dict = ImmutableMap.of("foo", "foo");
        Model b = new Model();
        b.dict = ImmutableMap.of("foo", "foo");
        Model c = new Model();
        c.dict = ImmutableMap.of("foo", "bar");
        ImmutableMap.of(a, true, b, false, c, true)
                .forEach((model, expected) -> {
                    boolean actual = model.save();
                    if(expected.equals(actual)) {
                        System.out.println("PASS: " + model.id());
                    }
                    else {
                        System.out.println("FAIL: " + model.id());
                        model.throwSupressedExceptions();
                        Assert.fail();
                    }
                });
    }

    class Invitation extends Record {

        @Unique(name = "foo")
        Student student;

        @Unique(name = "foo")
        Job job;

        Timestamp timestamp;
    }

    class Job extends Record {

        String title;
    }

    class Student extends Record {

        String name;
    }

    class Model extends Record {

        @Unique
        long age = Time.now();

        @Unique
        String name;

        @Unique
        Tag description;

        @Unique
        Map<String, Object> dict;

        @Unique
        Job job;

    }

}
