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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.collect.Association;
import com.cinchapi.common.collect.Collections;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for {@link Record} links, navigation, circular references, and deferred
 * references.
 *
 * @author Jeff Nelson
 */
public class RecordLinkTest extends AbstractRecordTest {

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

}
