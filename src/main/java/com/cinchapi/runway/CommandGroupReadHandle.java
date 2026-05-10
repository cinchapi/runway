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

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.lang.CommandGroup;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.google.common.collect.ImmutableList;

/**
 * A {@link ConcourseReadHandle} that uses a {@link CommandGroup} to
 * {@link Concourse#prepare() batch} reads and
 * {@link Concourse#submit(CommandGroup) submit} them in a single round trip
 * once upon {@link #materialize() materialization}.
 * 
 * @author Jeff Nelson
 */
final class CommandGroupReadHandle extends ConcourseReadHandle {

    /**
     * The {@link CommandGroup} that accumulates reads recorded on this
     * {@link ReadHandle}.
     */
    private CommandGroup group;

    /**
     * Construct a new {@link CommandGroupReadHandle}.
     *
     * @param concourse the {@link Concourse} connection against which reads are
     *            submitted; must not be {@code null}
     */
    CommandGroupReadHandle(Concourse concourse) {
        super(concourse);
        this.group = concourse.prepare();
    }

    @Override
    public void select(Criteria criteria) {
        group.select(criteria);
    }

    @Override
    public void select(Set<String> keys, Criteria criteria) {
        group.select(ImmutableList.copyOf(keys), criteria);
    }

    @Override
    public void select(Criteria criteria, Order order) {
        group.select(criteria, order);
    }

    @Override
    public void select(Set<String> keys, Criteria criteria, Order order) {
        group.select(ImmutableList.copyOf(keys), criteria, order);
    }

    @Override
    public void select(Criteria criteria, Page page) {
        group.select(criteria, page);
    }

    @Override
    public void select(Set<String> keys, Criteria criteria, Page page) {
        group.select(ImmutableList.copyOf(keys), criteria, page);
    }

    @Override
    public void select(Criteria criteria, Order order, Page page) {
        group.select(criteria, order, page);
    }

    @Override
    public void select(Set<String> keys, Criteria criteria, Order order,
            Page page) {
        group.select(ImmutableList.copyOf(keys), criteria, order, page);
    }

    @Override
    public void find(Criteria criteria) {
        group.find(criteria);
    }

    @Override
    public void find(Criteria criteria, Order order) {
        group.find(criteria, order);
    }

    @Override
    public void find(Criteria criteria, Page page) {
        group.find(criteria, page);
    }

    @Override
    public void find(Criteria criteria, Order order, Page page) {
        group.find(criteria, order, page);
    }

    @Override
    public List<Object> materialize() {
        return concourse.submit(group);
    }

}
