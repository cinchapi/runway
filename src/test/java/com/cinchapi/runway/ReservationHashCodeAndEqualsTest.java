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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for {@link Reservation} equality and hashing.
 *
 * @author Jeff Nelson
 */
public class ReservationHashCodeAndEqualsTest {

    /**
     * <strong>Goal:</strong> Verify that two {@link Reservation Reservations}
     * built with logically identical but separately constructed components are
     * equal.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} with independently
     * constructed {@link Criteria}, {@link Order}, {@link Page}, and
     * {@link Realms} that represent the same logical query.</li>
     * <li>Compare them with {@code equals}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The two {@link Reservation Reservations} are
     * equal and have the same hash code.
     */
    @Test
    public void testEqualWhenAllComponentsMatch() {
        Reservation a = Reservation.builder(Record.class).id(42L)
                .criteria(Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("Alice"))
                .order(Order.by("name").ascending()).page(Page.sized(10).go(1))
                .realms(Realms.only("production")).any(true).counting(false)
                .build();
        Reservation b = Reservation.builder(Record.class).id(42L)
                .criteria(Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("Alice"))
                .order(Order.by("name").ascending()).page(Page.sized(10).go(1))
                .realms(Realms.only("production")).any(true).counting(false)
                .build();
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * <strong>Goal:</strong> Verify that two {@link Reservation Reservations}
     * with only the class specified are equal.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} with only the class
     * set.</li>
     * <li>Compare them with {@code equals}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The two {@link Reservation Reservations} are
     * equal and have the same hash code.
     */
    @Test
    public void testEqualWhenOnlyClassSet() {
        Reservation a = Reservation.builder(Record.class).build();
        Reservation b = Reservation.builder(Record.class).build();
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * <strong>Goal:</strong> Verify that two {@link Reservation Reservations}
     * with separately constructed but logically identical {@link Criteria} are
     * equal.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} each with an independently
     * constructed {@link Criteria} using the same key, operator, and
     * value.</li>
     * <li>Compare them with {@code equals}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The two {@link Reservation Reservations} are
     * equal and have the same hash code.
     */
    @Test
    public void testEqualWhenCriteriaMatch() {
        Reservation a = Reservation.builder(Record.class).criteria(Criteria
                .where().key("name").operator(Operator.EQUALS).value("Alice"))
                .build();
        Reservation b = Reservation.builder(Record.class).criteria(Criteria
                .where().key("name").operator(Operator.EQUALS).value("Alice"))
                .build();
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * <strong>Goal:</strong> Verify that two {@link Reservation Reservations}
     * with separately constructed but logically identical {@link Order Orders}
     * are equal.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} each with an independently
     * constructed {@link Order} on the same key.</li>
     * <li>Compare them with {@code equals}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The two {@link Reservation Reservations} are
     * equal and have the same hash code.
     */
    @Test
    public void testEqualWhenOrderMatch() {
        Reservation a = Reservation.builder(Record.class)
                .order(Order.by("name")).build();
        Reservation b = Reservation.builder(Record.class)
                .order(Order.by("name")).build();
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * <strong>Goal:</strong> Verify that two {@link Reservation Reservations}
     * with separately constructed but logically identical {@link Page Pages}
     * are equal.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} each with an independently
     * constructed {@link Page} of the same size.</li>
     * <li>Compare them with {@code equals}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The two {@link Reservation Reservations} are
     * equal and have the same hash code.
     */
    @Test
    public void testEqualWhenPageMatch() {
        Reservation a = Reservation.builder(Record.class).page(Page.sized(10))
                .build();
        Reservation b = Reservation.builder(Record.class).page(Page.sized(10))
                .build();
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Reservation} is equal to
     * itself.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Reservation}.</li>
     * <li>Compare it to itself.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code equals} returns {@code true}.
     */
    @Test
    public void testEqualToSelf() {
        Reservation a = Reservation.builder(Record.class).id(1L).build();
        Assert.assertEquals(a, a);
    }

    /**
     * <strong>Goal:</strong> Verify that a unique {@link Reservation} is not
     * equal to a non-unique {@link Reservation} with the same criteria.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} with identical criteria,
     * one with {@code unique(true)} and one without.</li>
     * <li>Compare them with {@code equals}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The two {@link Reservation Reservations} are
     * not equal.
     */
    @Test
    public void testNotEqualWhenUniqueDiffers() {
        Reservation a = Reservation
                .builder(Record.class).criteria(Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("Alice"))
                .unique(true).build();
        Reservation b = Reservation.builder(Record.class).criteria(Criteria
                .where().key("name").operator(Operator.EQUALS).value("Alice"))
                .build();
        Assert.assertNotEquals(a, b);
    }

    /**
     * <strong>Goal:</strong> Verify that two unique {@link Reservation
     * Reservations} with identical components are equal.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} with the same criteria and
     * both set to {@code unique(true)}.</li>
     * <li>Compare them with {@code equals}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The two {@link Reservation Reservations} are
     * equal and have the same hash code.
     */
    @Test
    public void testEqualWhenBothUnique() {
        Reservation a = Reservation
                .builder(Record.class).criteria(Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("Alice"))
                .unique(true).build();
        Reservation b = Reservation
                .builder(Record.class).criteria(Criteria.where().key("name")
                        .operator(Operator.EQUALS).value("Alice"))
                .unique(true).build();
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    /**
     * <strong>Goal:</strong> Verify that two logically identical
     * {@link Reservation Reservations} with separately constructed
     * {@link Criteria}, {@link Order}, and {@link Page} produce the same hash
     * code.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build two {@link Reservation Reservations} with independently
     * constructed but logically identical components.</li>
     * <li>Compare their hash codes.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The hash codes are equal.
     */
    @Test
    public void testHashCodeMatchesForEqualReservations() {
        Reservation a = Reservation.builder(Record.class).id(7L)
                .criteria(Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("Alice"))
                .order(Order.by("name")).page(Page.sized(10)).any(true).build();
        Reservation b = Reservation.builder(Record.class).id(7L)
                .criteria(Criteria.where().key("name").operator(Operator.EQUALS)
                        .value("Alice"))
                .order(Order.by("name")).page(Page.sized(10)).any(true).build();
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

}
