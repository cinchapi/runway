/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.ClientServerTest;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;

/**
 * Unit tests for filtering functionality in {@link Runway}.
 *
 * @author Jeff Nelson
 */
public class RunwayFilterTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return ClientServerTest.LATEST_SNAPSHOT_VERSION;
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
    public void testCountFilter() {
        Writer a = new Writer("a");
        Writer b = new Writer("b");
        Editor c = new Editor("c");
        db.save(a, b);

        int acount = Random.getScaleCount();
        for (int i = 0; i < acount; ++i) {
            Article article = new Article("a" + i, a);
            article.save();
        }

        int bcount = Random.getScaleCount();
        for (int i = 0; i < bcount; ++i) {
            Article article = new Article("b" + i, b);
            article.save();
        }

        Assert.assertEquals(acount,
                db.count(Article.class, article -> article.isVisibleTo(a)));
        Assert.assertEquals(bcount,
                db.count(Article.class, article -> article.isVisibleTo(b)));
        Assert.assertEquals(acount + bcount,
                db.count(Article.class, article -> article.isVisibleTo(c)));
    }

    @Test
    public void testCountAnyFilter() {
        int writers = Random.getScaleCount();
        int editors = Random.getScaleCount();
        int admins = Random.getScaleCount();
        for (int i = 0; i < writers; ++i) {
            Writer writer = new Writer("writer" + i);
            writer.save();
        }
        for (int i = 0; i < editors; ++i) {
            Editor editor = new Editor("writer" + i);
            editor.save();
        }
        for (int i = 0; i < admins; ++i) {
            Admin admin = new Admin("writer" + i);
            admin.save();
        }
        Writer w = new Writer("writer");
        Editor e = new Editor("editor");
        Admin a = new Admin("a");
        db.save(w, e, a);
        Assert.assertEquals(1, db.countAny(User.class, u -> u.isVisibleTo(w)));
        Assert.assertEquals(2 + editors + writers,
                db.countAny(User.class, u -> u.isVisibleTo(e)));
        Assert.assertEquals(3 + admins + editors + writers,
                db.countAny(User.class, u -> u.isVisibleTo(a)));
    }

    @Test
    public void testCountCriteriaFilter() {
        Writer a = new Writer("a");
        Writer b = new Writer("b");
        db.save(a, b);
        AtomicInteger acount = new AtomicInteger(0);
        AtomicInteger bcount = new AtomicInteger(0);
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            String title;
            if(Random.getScaleCount() % 3 == 0) {
                title = "Expected";
                acount.getAndIncrement();
            }
            else {
                title = Random.getSimpleString();
            }
            Article article = new Article(title, a);
            article.save();
        }
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            String title;
            if(Random.getScaleCount() % 3 == 0) {
                title = "Expected";
                bcount.getAndIncrement();
            }
            else {
                title = Random.getSimpleString();
            }
            Article article = new Article(title, b);
            article.save();
        }
        Assert.assertEquals(acount.get(),
                db.count(Article.class,
                        Criteria.where().key("title").operator(Operator.EQUALS)
                                .value("Expected"),
                        article -> article.isVisibleTo(a)));
        Assert.assertEquals(bcount.get(),
                db.count(Article.class,
                        Criteria.where().key("title").operator(Operator.EQUALS)
                                .value("Expected"),
                        article -> article.isVisibleTo(b)));

    }

    abstract class Base extends Record {

        protected abstract boolean isVisibleTo(User user);
    }

    abstract class User extends Base {

        String name;

        public User(String name) {
            this.name = name;
        }

    }

    class Admin extends User {

        /**
         * Construct a new instance.
         * 
         * @param name
         */
        public Admin(String name) {
            super(name);
        }

        @Override
        protected boolean isVisibleTo(User user) {
            return this.equals(user) || user instanceof Admin;
        }
    }

    class Editor extends User {

        /**
         * Construct a new instance.
         * 
         * @param name
         */
        public Editor(String name) {
            super(name);
        }

        @Override
        protected boolean isVisibleTo(User user) {
            return this.equals(user) || user instanceof Admin
                    || user instanceof Editor;
        }
    }

    class Writer extends User {

        /**
         * Construct a new instance.
         * 
         * @param name
         */
        public Writer(String name) {
            super(name);
        }

        @Override
        protected boolean isVisibleTo(User user) {
            return this.equals(user) || user instanceof Admin
                    || user instanceof Editor;
        }
    }

    class Article extends Base {

        public Article(String title, Writer author) {
            this.title = title;
            this.author = author;
        }

        Writer author;
        String title;

        @Override
        protected boolean isVisibleTo(User user) {
            return user instanceof Admin || user instanceof Editor
                    || author.equals(user);
        }
    }

}
