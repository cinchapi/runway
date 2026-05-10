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

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link ReadHandle} records read operations against a database and returns
 * their results upon {@link #materialize() materialization}.
 *
 * @author Jeff Nelson
 */
interface ReadHandle {

    /**
     * Record a select for every record matching the {@code criteria}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     */
    void select(Criteria criteria);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     */
    void select(Set<String> keys, Criteria criteria);

    /**
     * Record a select for every record matching the {@code criteria}, sorted by
     * {@code order}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     */
    void select(Criteria criteria, Order order);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, sorted by {@code order}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     */
    void select(Set<String> keys, Criteria criteria, Order order);

    /**
     * Record a select for every record matching the {@code criteria}, limited
     * to the requested {@code page} of the result set.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     */
    void select(Criteria criteria, Page page);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, limited to the requested {@code page} of the result
     * set.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     */
    void select(Set<String> keys, Criteria criteria, Page page);

    /**
     * Record a select for every record matching the {@code criteria}, sorted by
     * {@code order} and limited to the requested {@code page}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @param page the {@link Page} that limits the result set
     */
    void select(Criteria criteria, Order order, Page page);

    /**
     * Record a select for the {@code keys} on every record matching the
     * {@code criteria}, sorted by {@code order} and limited to the requested
     * {@code page}.
     *
     * @param keys the field names whose values should be returned
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @param page the {@link Page} that limits the result set
     */
    void select(Set<String> keys, Criteria criteria, Order order, Page page);

    /**
     * Record a find for the records matching the {@code criteria}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     */
    void find(Criteria criteria);

    /**
     * Record a find for the records matching the {@code criteria}, sorted by
     * {@code order}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     */
    void find(Criteria criteria, Order order);

    /**
     * Record a find for the records matching the {@code criteria}, limited to
     * the requested {@code page}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param page the {@link Page} that limits the result set
     */
    void find(Criteria criteria, Page page);

    /**
     * Record a find for the records matching the {@code criteria}, sorted by
     * {@code order} and limited to the requested {@code page}.
     *
     * @param criteria the {@link Criteria} that identifies the records
     * @param order the {@link Order} that determines the sort
     * @param page the {@link Page} that limits the result set
     */
    void find(Criteria criteria, Order order, Page page);

    /**
     * Return the results of every read recorded on this {@link ReadHandle}.
     * <p>
     * The returned {@link List} contains one entry per recorded read. Entry
     * {@code i} is the value produced by the {@code i}th recorded read &mdash;
     * a {@link java.util.Map Map&lt;Long, Map&lt;String,
     * Set&lt;Object&gt;&gt;&gt;} for a select, a {@link java.util.Set
     * Set&lt;Long&gt;} for a find. Callers cast each entry to the type of the
     * read they recorded at that index.
     *
     * @return the recorded reads' results
     */
    List<Object> materialize();

}
