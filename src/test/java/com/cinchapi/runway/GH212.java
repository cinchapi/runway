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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/212">GH-212</a>: a dynamic
 * write must leave a field holding its declared {@link Collection} type, which
 * is the type every other write path builds.
 *
 * @author Jeff Nelson
 */
public class GH212 extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a dynamic write to a field declared by
     * a concrete {@link Collection} type stores that type instead of the
     * implementation the caller supplied.
     * <p>
     * <strong>Start state:</strong> An unsaved {@link Container} whose
     * {@code concurrent} field is declared as a {@link CopyOnWriteArrayList}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build an {@link java.util.ArrayList ArrayList} that holds one
     * element.</li>
     * <li>Call {@link Record#set(String, Object)} with the key
     * {@code concurrent} and that list.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The field holds a {@link CopyOnWriteArrayList}
     * with the same element. Before the fix, the write fails with an
     * {@link IllegalArgumentException}.
     */
    @Test
    public void testSetConformsCollectionToDeclaredConcreteType() {
        Container container = new Container();
        List<String> values = Lists.newArrayList("a");
        container.set("concurrent", values);
        Assert.assertEquals(CopyOnWriteArrayList.class,
                container.concurrent.getClass());
        Assert.assertEquals(values, container.concurrent);
    }

    /**
     * <strong>Goal:</strong> Verify that a dynamic write stores the instance
     * the caller supplied when that instance already satisfies the declared
     * type, so the caller and the {@link Record} continue to share it.
     * <p>
     * <strong>Start state:</strong> An unsaved {@link Container} whose
     * {@code plain} field is declared as a {@link List}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build an {@link java.util.ArrayList ArrayList} that holds one
     * element.</li>
     * <li>Call {@link Record#set(String, Object)} with the key {@code plain}
     * and that list.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The field holds the identical instance the
     * caller supplied, which means no copy occurred.
     */
    @Test
    public void testSetKeepsCallerInstanceWhenItSatisfiesDeclaredType() {
        Container container = new Container();
        List<String> values = Lists.newArrayList("a");
        container.set("plain", values);
        Assert.assertSame(values, container.plain);
    }

    /**
     * <strong>Goal:</strong> Verify that a field holds the same
     * {@link Collection} type whether its value arrived from a dynamic write or
     * from a load.
     * <p>
     * <strong>Start state:</strong> An unsaved {@link Container} whose
     * {@code concurrent} field is declared as a {@link CopyOnWriteArrayList}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Write the {@code concurrent} field through
     * {@link Record#set(String, Object)} and save the {@link Container}.</li>
     * <li>Load the saved {@link Container} by its id.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The written copy and the loaded copy both hold
     * a {@link CopyOnWriteArrayList}.
     */
    @Test
    public void testDynamicWriteAndLoadProduceTheSameCollectionType() {
        Container container = new Container();
        container.set("concurrent", Lists.newArrayList("a"));
        Assert.assertTrue(runway.save(container));
        Container loaded = runway.load(Container.class, container.id());
        Assert.assertEquals(CopyOnWriteArrayList.class,
                container.concurrent.getClass());
        Assert.assertEquals(CopyOnWriteArrayList.class,
                loaded.concurrent.getClass());
    }

    /**
     * A {@link Record} that declares one {@link Collection} field by a concrete
     * type and another by an interface.
     *
     * @author Jeff Nelson
     */
    public static class Container extends Record {

        /**
         * A field declared by a concrete type, which a caller chooses so that
         * every path holds a thread-safe instance.
         */
        public CopyOnWriteArrayList<String> concurrent = new CopyOnWriteArrayList<>();

        /**
         * A field declared by an interface, which any implementation satisfies.
         */
        public List<String> plain = Lists.newArrayList();
    }

}
