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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;

/**
 * End-to-end routing tests for the {@link Selection} fluent builder API,
 * verifying that every parameter combination supported by {@link Gateway} is
 * correctly routed through
 * {@link DatabaseInterface#select(Selection, Selection...)}.
 * <p>
 * Each test saves data, builds a {@link Selection}, executes it via
 * {@code select()}, and asserts the result matches what the equivalent direct
 * {@link DatabaseInterface} call would produce.
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("deprecation")
public class SelectionRoutingTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.of(clazz)} with no
     * criteria routes to {@code load(clazz)}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Widget Widgets}.</li>
     * <li>Execute {@code Selection.of(Widget.class)} via {@code select()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains both {@link Widget
     * Widgets}, matching {@code runway.load(Widget.class)}.
     */
    @Test
    public void testNoCriteriaRoutesToLoad() {
        runway.save(new Widget("a"), new Widget("b"));
        Set<Widget> expected = runway.load(Widget.class);
        Selections results = runway.select(Selection.of(Widget.class));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(criteria)} routes to
     * {@code find(clazz, criteria)}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets} with
     * different names.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Widget Widgets} "alpha" and "beta".</li>
     * <li>Execute a {@link Selection} with criteria matching "alpha".</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains only the "alpha"
     * {@link Widget}, matching {@code runway.find(Widget.class, criteria)}.
     */
    @Test
    public void testCriteriaRoutesToFind() {
        runway.save(new Widget("alpha"), new Widget("beta"));
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("alpha");
        Set<Widget> expected = runway.find(Widget.class, criteria);
        Selections results = runway
                .select(Selection.of(Widget.class).where(criteria));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
        Assert.assertEquals(1, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(criteria).order(order)} routes to
     * {@code find(clazz, criteria, order)}.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with criteria and order.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is non-empty and matches the direct
     * find call with order.
     */
    @Test
    public void testCriteriaWithOrderRoutesToFindWithOrder() {
        runway.save(new Widget("c", 3), new Widget("a", 1), new Widget("b", 2));
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Order order = Order.by("name");
        Set<Widget> expected = runway.find(Widget.class, criteria, order);
        Selections results = runway.select(
                Selection.of(Widget.class).where(criteria).order(order));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(criteria).order(order).page(page)}
     * routes to {@code find(clazz, criteria, order, page)}.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with criteria, order, and page.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains exactly one {@link Widget}
     * (page size 1).
     */
    @Test
    public void testCriteriaWithOrderAndPageRoutesToFindWithOrderAndPage() {
        runway.save(new Widget("c", 3), new Widget("a", 1), new Widget("b", 2));
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Order order = Order.by("name");
        Page page = Page.sized(1);
        Set<Widget> expected = runway.find(Widget.class, criteria, order, page);
        Selections results = runway.select(Selection.of(Widget.class)
                .where(criteria).order(order).page(page));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
        Assert.assertEquals(1, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(criteria).page(page)} routes to
     * {@code find(clazz, criteria, page)} (no order).
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with criteria and page but no order.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains at most one {@link Widget}
     * (page size 1).
     */
    @Test
    public void testCriteriaWithPageRoutesToFindWithPage() {
        runway.save(new Widget("x", 10), new Widget("y", 20),
                new Widget("z", 30));
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Page page = Page.sized(1);
        Selections results = runway
                .select(Selection.of(Widget.class).where(criteria).page(page));
        Set<Widget> actual = results.next();
        Assert.assertEquals(1, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).order(order)} with no criteria routes to
     * {@code load(clazz, order)}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with order but no criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains both {@link Widget
     * Widgets}, matching {@code runway.load(Widget.class, order)}.
     */
    @Test
    public void testOrderWithoutCriteriaRoutesToLoad() {
        runway.save(new Widget("b"), new Widget("a"));
        Order order = Order.by("name");
        Set<Widget> expected = runway.load(Widget.class, order);
        Selections results = runway
                .select(Selection.of(Widget.class).order(order));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).order(order).page(page)} with no criteria
     * routes to {@code load(clazz, order, page)}.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with order and page but no criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains exactly one {@link Widget}
     * (page size 1).
     */
    @Test
    public void testOrderAndPageWithoutCriteriaRoutesToLoad() {
        runway.save(new Widget("c"), new Widget("a"), new Widget("b"));
        Order order = Order.by("name");
        Page page = Page.sized(1);
        Set<Widget> expected = runway.load(Widget.class, order, page);
        Selections results = runway
                .select(Selection.of(Widget.class).order(order).page(page));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
        Assert.assertEquals(1, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.of(clazz).page(page)}
     * with no criteria and no order routes to {@code load(clazz, page)}.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with only page.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains exactly two {@link Widget
     * Widgets} (page size 2).
     */
    @Test
    public void testPageOnlyRoutesToLoad() {
        runway.save(new Widget("a"), new Widget("b"), new Widget("c"));
        Page page = Page.sized(2);
        Selections results = runway
                .select(Selection.of(Widget.class).page(page));
        Set<Widget> actual = results.next();
        Assert.assertEquals(2, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.of(clazz).id(id)}
     * routes to {@code load(clazz, id)}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Widget} and capture its ID.</li>
     * <li>Execute a {@link Selection} with that ID.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is the same {@link Widget}.
     */
    @Test
    public void testIdRoutesToLoadById() {
        Widget w = new Widget("target");
        runway.save(w);
        long id = w.id();
        Selections results = runway.select(Selection.of(Widget.class).id(id));
        Widget actual = results.next();
        Assert.assertNotNull(actual);
        Assert.assertEquals("target", actual.name);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.of(clazz).count()}
     * routes to {@code count(clazz)} and returns the correct count.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a counting {@link Selection}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code 3}.
     */
    @Test
    public void testCountRoutesToCount() {
        runway.save(new Widget("a"), new Widget("b"), new Widget("c"));
        Selections results = runway.select(Selection.of(Widget.class).count());
        int actual = results.next();
        Assert.assertEquals(3, actual);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.of(clazz).where(criteria).count()} routes to
     * {@code count(clazz, criteria)}.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets} with
     * different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets} with scores 1, 2, 3.</li>
     * <li>Execute a counting {@link Selection} with criteria matching score
     * &gt; 1.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code 2}.
     */
    @Test
    public void testCountWithCriteriaRoutesToCountWithCriteria() {
        runway.save(new Widget("a", 1), new Widget("b", 2), new Widget("c", 3));
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(1);
        Selections results = runway
                .select(Selection.of(Widget.class).where(criteria).count());
        int actual = results.next();
        Assert.assertEquals(2, actual);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.ofAny(clazz)} routes
     * to {@code loadAny(clazz)} and includes descendants.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget} and a saved
     * {@link SpecialWidget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Widget} and a {@link SpecialWidget}.</li>
     * <li>Execute {@code Selection.ofAny(Widget.class)} via
     * {@code select()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result includes both the {@link Widget}
     * and the {@link SpecialWidget}.
     */
    @Test
    public void testOfAnyRoutesToLoadAnyIncludingDescendants() {
        runway.save(new Widget("plain"), new SpecialWidget("special"));
        Selections results = runway.select(Selection.ofAny(Widget.class));
        Set<Widget> actual = results.next();
        Assert.assertEquals(2, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.of(clazz)} excludes
     * descendants.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget} and a saved
     * {@link SpecialWidget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Widget} and a {@link SpecialWidget}.</li>
     * <li>Execute {@code Selection.of(Widget.class)} via {@code select()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result includes only the {@link Widget},
     * not the {@link SpecialWidget}.
     */
    @Test
    public void testOfExcludesDescendants() {
        runway.save(new Widget("plain"), new SpecialWidget("special"));
        Selections results = runway.select(Selection.of(Widget.class));
        Set<Widget> actual = results.next();
        Assert.assertEquals(1, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@code Selection.ofAny(clazz).where(criteria)} routes to
     * {@code findAny(clazz, criteria)}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget} and a saved
     * {@link SpecialWidget} with the same name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save both with name "shared".</li>
     * <li>Execute {@code Selection.ofAny(Widget.class).where(criteria)}
     * matching name "shared".</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result includes both.
     */
    @Test
    public void testOfAnyWithCriteriaRoutesToFindAny() {
        runway.save(new Widget("shared"), new SpecialWidget("shared"));
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("shared");
        Selections results = runway
                .select(Selection.ofAny(Widget.class).where(criteria));
        Set<Widget> actual = results.next();
        Assert.assertEquals(2, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.ofAny(clazz).count()}
     * routes to {@code countAny(clazz)}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget} and a saved
     * {@link SpecialWidget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save both.</li>
     * <li>Execute a counting {@code ofAny} selection.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count is {@code 2}.
     */
    @Test
    public void testOfAnyCountRoutesToCountAny() {
        runway.save(new Widget("a"), new SpecialWidget("b"));
        Selections results = runway
                .select(Selection.ofAny(Widget.class).count());
        int actual = results.next();
        Assert.assertEquals(2, actual);
    }

    /**
     * <strong>Goal:</strong> Verify that a client-side {@code filter} is
     * applied when loading.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets} with
     * scores 1, 2, 3.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with a filter that only passes score &gt;
     * 1.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains two {@link Widget
     * Widgets}.
     */
    @Test
    public void testFilterAppliedOnLoad() {
        runway.save(new Widget("a", 1), new Widget("b", 2), new Widget("c", 3));
        Selections results = runway
                .select(Selection.of(Widget.class).filter(w -> w.score > 1));
        Set<Widget> actual = results.next();
        Assert.assertEquals(2, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a client-side {@code filter} is
     * applied when finding with criteria.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets} with
     * scores 1, 2, 3.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with criteria (score &gt; 0) and filter
     * (score &gt; 1).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Criteria matches all three, filter narrows to
     * two.
     */
    @Test
    public void testFilterAppliedOnFind() {
        runway.save(new Widget("a", 1), new Widget("b", 2), new Widget("c", 3));
        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(0);
        Selections results = runway.select(Selection.of(Widget.class)
                .where(criteria).filter(w -> w.score > 1));
        Set<Widget> actual = results.next();
        Assert.assertEquals(2, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a client-side {@code filter} is
     * applied to a count operation.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Widget Widgets} with
     * scores 1, 2, 3.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Widget Widgets}.</li>
     * <li>Execute a counting {@link Selection} with a filter that only passes
     * score &gt; 1.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The count is {@code 2}.
     */
    @Test
    public void testFilterAppliedOnCount() {
        runway.save(new Widget("a", 1), new Widget("b", 2), new Widget("c", 3));
        Selections results = runway.select(
                Selection.of(Widget.class).filter(w -> w.score > 1).count());
        int actual = results.next();
        Assert.assertEquals(2, actual);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code realms} is passed through
     * correctly.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets} in
     * different realms.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Widget} in realm "east" and another in realm
     * "west".</li>
     * <li>Execute a {@link Selection} constrained to realm "east".</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains only the "east"
     * {@link Widget}.
     */
    @Test
    public void testRealmsConstrainsResults() {
        Widget east = new Widget("east-widget");
        east.addRealm("east");
        Widget west = new Widget("west-widget");
        west.addRealm("west");
        runway.save(east, west);
        Selections results = runway
                .select(Selection.of(Widget.class).realms(Realms.only("east")));
        Set<Widget> actual = results.next();
        Assert.assertEquals(1, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code Selection.of(clazz).any()} is
     * equivalent to {@code Selection.ofAny(clazz)}.
     * <p>
     * <strong>Start state:</strong> A saved {@link Widget} and a saved
     * {@link SpecialWidget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save both.</li>
     * <li>Execute {@code Selection.of(Widget.class).any()} via
     * {@code select()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result includes both, same as
     * {@code ofAny}.
     */
    @Test
    public void testAnyFluentMethodEquivalentToOfAny() {
        runway.save(new Widget("plain"), new SpecialWidget("special"));
        Selections results = runway.select(Selection.of(Widget.class).any());
        Set<Widget> actual = results.next();
        Assert.assertEquals(2, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a builder can be passed directly to
     * {@code select()} without calling {@code build()}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Widget Widgets}.</li>
     * <li>Pass an {@link Selection.OpenBuilder} directly to {@code select()}
     * without calling {@code build()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains both {@link Widget
     * Widgets}.
     */
    @Test
    public void testBuilderPassedDirectlyWithoutBuild() {
        runway.save(new Widget("a"), new Widget("b"));
        Selections results = runway.select(Selection.of(Widget.class));
        Set<Widget> actual = results.next();
        Assert.assertEquals(2, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a builder with criteria can be passed
     * directly to {@code select()} without calling {@code build()}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets} with
     * different names.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Widget Widgets} "alpha" and "beta".</li>
     * <li>Pass a {@link Selection.QueryBuilder} directly to {@code select()}
     * without calling {@code build()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result contains only "alpha".
     */
    @Test
    public void testQueryBuilderPassedDirectlyWithoutBuild() {
        runway.save(new Widget("alpha"), new Widget("beta"));
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("alpha");
        Selections results = runway
                .select(Selection.of(Widget.class).where(criteria));
        Set<Widget> actual = results.next();
        Assert.assertEquals(1, actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that absent criteria with order routes the
     * same as {@link Gateway#fetch(Class, Criteria, Order, Page)} with
     * {@code null} criteria.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with order but no criteria.</li>
     * <li>Compare against {@link Gateway} with null criteria and same
     * order.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both return the same {@link Widget Widgets}.
     */
    @Test
    public void testAbsentCriteriaNeverRoutesToFind() {
        runway.save(new Widget("a"), new Widget("b"));
        Order order = Order.by("name");
        Set<Widget> expected = runway.gateway().fetch(Widget.class, null, order,
                null);
        Selections results = runway
                .select(Selection.of(Widget.class).order(order));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that null order and null page produce the
     * same result as {@link Gateway#fetch(Class, Criteria, Order, Page)} with
     * all nulls.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with null order and null page.</li>
     * <li>Compare against {@link Gateway} with all null optional params.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both return the same {@link Widget Widgets}.
     */
    @Test
    public void testNullOrderAndNullPageMatchesGateway() {
        runway.save(new Widget("a"), new Widget("b"));
        Set<Widget> expected = runway.gateway().fetch(Widget.class, null,
                (Order) null, null);
        Selections results = runway.select(Selection.of(Widget.class)
                .criteria(null).order(null).page(null));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that criteria with null order and null page
     * produces the same result as
     * {@link Gateway#fetch(Class, Criteria, Order, Page)} with the same
     * criteria and null order/page.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with criteria and null order and null
     * page.</li>
     * <li>Compare against {@link Gateway} with same criteria and null
     * order/page.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both return the same {@link Widget Widgets}.
     */
    @Test
    public void testCriteriaWithNullOrderAndNullPageMatchesGateway() {
        runway.save(new Widget("alpha"), new Widget("beta"));
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("alpha").build();
        Set<Widget> expected = runway.gateway().fetch(Widget.class, criteria,
                (Order) null, null);
        Selections results = runway.select(Selection.of(Widget.class)
                .where(criteria).order(null).page(null));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
    }

    /**
     * <strong>Goal:</strong> Verify that criteria with null filter produces the
     * same result as {@link Gateway#fetch(Class, Criteria, Order, Page)} with
     * the same criteria.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Widget Widgets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Widget Widgets}.</li>
     * <li>Execute a {@link Selection} with criteria and null filter.</li>
     * <li>Compare against {@link Gateway} with same criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both return the same {@link Widget Widgets}.
     */
    @Test
    public void testCriteriaWithNullFilterMatchesGateway() {
        runway.save(new Widget("alpha"), new Widget("beta"));
        Criteria criteria = Criteria.where().key("name")
                .operator(Operator.EQUALS).value("alpha").build();
        Set<Widget> expected = runway.gateway().fetch(Widget.class, criteria,
                (Order) null, null);
        Selections results = runway.select(
                Selection.of(Widget.class).where(criteria).filter(null));
        Set<Widget> actual = results.next();
        Assert.assertEquals(expected.size(), actual.size());
    }

    /**
     * A simple test {@link Record} with a name and score.
     */
    class Widget extends Record {

        /**
         * The widget name.
         */
        String name;

        /**
         * The widget score.
         */
        int score;

        /**
         * Construct a new {@link Widget}.
         *
         * @param name the name
         */
        Widget(String name) {
            this(name, 0);
        }

        /**
         * Construct a new {@link Widget}.
         *
         * @param name the name
         * @param score the score
         */
        Widget(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }

    /**
     * A descendant of {@link Widget} for testing {@code any}/{@code ofAny}
     * routing.
     */
    class SpecialWidget extends Widget {

        /**
         * Construct a new {@link SpecialWidget}.
         *
         * @param name the name
         */
        SpecialWidget(String name) {
            super(name);
        }
    }

}
