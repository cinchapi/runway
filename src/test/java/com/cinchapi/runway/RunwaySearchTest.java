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

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link Runway} search and deferred reference loading.
 *
 * @author Jeff Nelson
 */
public class RunwaySearchTest extends AbstractRunwayTest {

    @Test
    public void testSearch() {
        User a = new Manager("John Doern");
        User b = new Manager("Jane Doern");
        User c = new Manager("Liz James");
        runway.save(a, b, c);
        Set<Manager> records = runway.search(Manager.class, "n Doe", "name");
        Assert.assertEquals(2, records.size());
    }

    @Test
    public void testSearchMultipleKeys() {
        SuperAdmin a = new SuperAdmin("Jeff", "Goes to the store",
                "with you fuzzugng");
        SuperAdmin b = new SuperAdmin("Ashleah", "With fuzzugng", "Okay cool");
        runway.save(a, b);
        Set<SuperAdmin> records = runway.search(SuperAdmin.class, "zzug", "foo",
                "bar");
        Assert.assertEquals(2, records.size());
    }

    @Test
    public void testSearchSingleClass() {
        SuperAdmin a = new SuperAdmin("Jeff", "Goes to the store",
                "with you fuzzugng");
        Admin b = new Admin("Ashleah", "With fuzzugng");
        runway.save(a, b);
        Set<SuperAdmin> records = runway.search(SuperAdmin.class, "zzug", "foo",
                "bar");
        Assert.assertEquals(1, records.size());
    }

    @Test
    public void testSearchAcrossClassHierarchy() {
        SuperAdmin a = new SuperAdmin("Jeff", "Goes to the store",
                "with you fuzzugng");
        Admin b = new Admin("Ashleah", "With fuzzugng");
        runway.save(a, b);
        Set<User> records = runway.searchAny(User.class, "zzug", "foo", "bar");
        Assert.assertEquals(2, records.size());
    }

    @Test
    public void testLoadDeferredReference() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        Assert.assertNull(jock.mentor.$ref());
        Assert.assertEquals("B", jock.mentor.get().name);
        Assert.assertNotNull(jock.mentor.$ref());
    }

    @Test
    public void testLoadDeferredReferenceDynamicGet() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        Jock mentor = jock.get("mentor");
        Assert.assertEquals("B", mentor.name);
    }

    @Test
    public void testLoadDeferredReferenceMap() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.mentor.get().friends.add(new DeferredReference<>(jock));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        jock.map().values().forEach(value -> {
            Assert.assertFalse(value instanceof DeferredReference);
        });
    }

    @Test
    public void testLoadDeferredReferenceExplicitMap() {
        Jock jock = new Jock("A");
        jock.mentor = new DeferredReference<>(new Jock("B"));
        jock.mentor.get().friends.add(new DeferredReference<>(jock));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        jock.map("mentor").values().forEach(value -> {
            Assert.assertFalse(value instanceof DeferredReference);
            Assert.assertTrue(value instanceof Jock);
        });
    }

    @Test
    public void testLoadDeferredExplictMapCollection() {
        Jock jock = new Jock("A");
        jock.friends.add(new DeferredReference<>(new Jock("B")));
        jock.friends.add(new DeferredReference<>(new Jock("C")));
        jock.save();
        jock = runway.load(Jock.class, jock.id());
        ((List<?>) jock.map("friends").get("friends")).forEach(value -> {
            Assert.assertFalse(value instanceof DeferredReference);
            Assert.assertTrue(value instanceof Jock);
        });
    }

}
