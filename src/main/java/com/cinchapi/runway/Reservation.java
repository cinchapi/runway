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

import java.util.Objects;

import javax.annotation.Nullable;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link Reservation} captures the signature of a query for use as a lookup
 * key in the thread-local reserve. Two queries with the same signature are
 * considered equivalent and can share results.
 *
 * @author Jeff Nelson
 */
final class Reservation {

    /**
     * Return a new {@link Builder}.
     *
     * @param clazz the target {@link Record} class
     * @return a new {@link Builder}
     */
    static Builder builder(Class<?> clazz) {
        return new Builder(clazz);
    }

    /**
     * The target {@link Record} class.
     */
    private final Class<?> clazz;

    /**
     * The record ID for single-record lookups, or {@code null}.
     */
    @Nullable
    private final Long id;

    /**
     * The query criteria, or {@code null}.
     */
    @Nullable
    private final Criteria criteria;

    /**
     * The sort order, or {@code null}.
     */
    @Nullable
    private final Order order;

    /**
     * The pagination, or {@code null}.
     */
    @Nullable
    private final Page page;

    /**
     * The {@link Realms} filter.
     */
    private final Realms realms;

    /**
     * Whether to include descendants of {@link #clazz}.
     */
    private final boolean any;

    /**
     * Whether this is a counting reservation.
     */
    private final boolean counting;

    /**
     * Construct a new {@link Reservation}.
     *
     * @param builder the {@link Builder}
     */
    private Reservation(Builder builder) {
        this.clazz = builder.clazz;
        this.id = builder.id;
        this.criteria = builder.criteria;
        this.order = builder.order;
        this.page = builder.page;
        this.realms = builder.realms;
        this.any = builder.any;
        this.counting = builder.counting;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        else if(obj instanceof Reservation) {
            Reservation other = (Reservation) obj;
            return Objects.equals(clazz, other.clazz)
                    && Objects.equals(id, other.id)
                    && Objects.equals(criteria, other.criteria)
                    && Objects.equals(order, other.order)
                    && Objects.equals(page, other.page)
                    && Objects.equals(realms, other.realms) && any == other.any
                    && counting == other.counting;
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, id, criteria, order, page, realms, any,
                counting);
    }

    /**
     * A {@link Builder} for constructing {@link Reservation Reservations}.
     *
     * @author Jeff Nelson
     */
    static final class Builder {

        /**
         * The target {@link Record} class.
         */
        private final Class<?> clazz;

        /**
         * The record ID.
         */
        @Nullable
        private Long id;

        /**
         * The query criteria.
         */
        @Nullable
        private Criteria criteria;

        /**
         * The sort order.
         */
        @Nullable
        private Order order;

        /**
         * The pagination.
         */
        @Nullable
        private Page page;

        /**
         * The {@link Realms} filter.
         */
        private Realms realms;

        /**
         * Whether to include descendants.
         */
        private boolean any;

        /**
         * Whether this is a counting reservation.
         */
        private boolean counting;

        /**
         * Construct a new {@link Builder}.
         *
         * @param clazz the target {@link Record} class
         */
        private Builder(Class<?> clazz) {
            this.clazz = clazz;
        }

        /**
         * Set the record ID.
         *
         * @param id the record ID, or {@code null}
         * @return this {@link Builder}
         */
        Builder id(@Nullable Long id) {
            this.id = id;
            return this;
        }

        /**
         * Set the query criteria.
         *
         * @param criteria the {@link Criteria}
         * @return this {@link Builder}
         */
        Builder criteria(Criteria criteria) {
            this.criteria = criteria;
            return this;
        }

        /**
         * Set the sort order.
         *
         * @param order the {@link Order}
         * @return this {@link Builder}
         */
        Builder order(Order order) {
            this.order = order;
            return this;
        }

        /**
         * Set the pagination.
         *
         * @param page the {@link Page}
         * @return this {@link Builder}
         */
        Builder page(Page page) {
            this.page = page;
            return this;
        }

        /**
         * Set the {@link Realms} filter.
         *
         * @param realms the {@link Realms}
         * @return this {@link Builder}
         */
        Builder realms(Realms realms) {
            this.realms = realms;
            return this;
        }

        /**
         * Set whether to include descendants of the target class.
         *
         * @param any {@code true} to include descendants
         * @return this {@link Builder}
         */
        Builder any(boolean any) {
            this.any = any;
            return this;
        }

        /**
         * Set whether this is a counting reservation.
         *
         * @param counting {@code true} for counting
         * @return this {@link Builder}
         */
        Builder counting(boolean counting) {
            this.counting = counting;
            return this;
        }

        /**
         * Build the {@link Reservation}.
         *
         * @return a new {@link Reservation}
         */
        Reservation build() {
            return new Reservation(this);
        }

    }

}
