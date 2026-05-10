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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.google.common.base.Preconditions;

/**
 * A {@link ReadHandle} for {@link Concourse}.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
class ConcourseReadHandle implements ReadHandle {

    /**
     * The {@link Concourse} connection against which reads are issued.
     */
    protected final Concourse concourse;

    /**
     * The values produced by reads recorded on this
     * {@link ConcourseReadHandle}.
     */
    private List<Object> results;

    /**
     * Construct a new {@link ConcourseReadHandle}.
     *
     * @param concourse the {@link Concourse} connection against which reads are
     *            issued; must not be {@code null}
     */
    ConcourseReadHandle(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
        this.results = null;
    }

    @Override
    public void select(Criteria criteria) {
        results().add(concourse.select(criteria));
    }

    @Override
    public void select(Set<String> keys, Criteria criteria) {
        results().add(concourse.select(keys, criteria));
    }

    @Override
    public void select(Criteria criteria, Order order) {
        results().add(concourse.select(criteria, order));
    }

    @Override
    public void select(Set<String> keys, Criteria criteria, Order order) {
        results().add(concourse.select(keys, criteria, order));
    }

    @Override
    public void select(Criteria criteria, Page page) {
        results().add(concourse.select(criteria, page));
    }

    @Override
    public void select(Set<String> keys, Criteria criteria, Page page) {
        results().add(concourse.select(keys, criteria, page));
    }

    @Override
    public void select(Criteria criteria, Order order, Page page) {
        results().add(concourse.select(criteria, order, page));
    }

    @Override
    public void select(Set<String> keys, Criteria criteria, Order order,
            Page page) {
        results().add(concourse.select(keys, criteria, order, page));
    }

    @Override
    public void find(Criteria criteria) {
        results().add(concourse.find(criteria));
    }

    @Override
    public void find(Criteria criteria, Order order) {
        results().add(concourse.find(criteria, order));
    }

    @Override
    public void find(Criteria criteria, Page page) {
        results().add(concourse.find(criteria, page));
    }

    @Override
    public void find(Criteria criteria, Order order, Page page) {
        results().add(concourse.find(criteria, order, page));
    }

    @Override
    public List<Object> materialize() {
        return results();
    }

    private List<Object> results() {
        if(results == null) {
            results = new ArrayList<>();
        }
        return results;
    }

}
