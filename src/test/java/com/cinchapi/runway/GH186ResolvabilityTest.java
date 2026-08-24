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
import com.cinchapi.concourse.thrift.Operator;

/**
 * Unit tests for the GH-186 database-resolvability boundary.
 *
 * @author ptt
 */
public class GH186ResolvabilityTest {

    /**
     * A base type with no descendant key.
     *
     * @author ptt
     */
    abstract class Base extends Record {

        /**
         * A key shared by the hierarchy.
         */
        public String shared;

    }

    /**
     * A descendant that declares its own key.
     *
     * @author ptt
     */
    class Descendant extends Base {

        /**
         * A key declared only by this descendant.
         */
        public String descendant;

    }

    /**
     * <strong>Goal:</strong> Verify a hierarchy-aware resolvability check
     * accepts a key declared by a descendant.
     * <p>
     * <strong>Start state:</strong> A base class with one descendant-defined
     * key.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a criteria that names the descendant-defined key.</li>
     * <li>Invoke the hierarchy-aware public resolvability form.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The criteria is database-resolvable across the
     * hierarchy.
     *
     */
    @Test
    public void testHierarchyResolvabilityIncludesDescendantKey() {
        Criteria criteria = Criteria.where().key("descendant")
                .operator(Operator.EQUALS).value("value").build();

        Assert.assertTrue(Record.isDatabaseResolvableCondition(Base.class,
                criteria, true));
    }

    /**
     * <strong>Goal:</strong> Verify the exact-class resolvability check keeps
     * its existing key scope.
     * <p>
     * <strong>Start state:</strong> A base class with one descendant-defined
     * key.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a criteria that names the descendant-defined key.</li>
     * <li>Check the criteria against the base class and the descendant.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The base class rejects the key, while the
     * descendant accepts it.
     */
    @Test
    public void testExactClassResolvabilityRemainsUnchanged() {
        Criteria criteria = Criteria.where().key("descendant")
                .operator(Operator.EQUALS).value("value").build();

        Assert.assertFalse(
                Record.isDatabaseResolvableCondition(Base.class, criteria));
        Assert.assertTrue(Record.isDatabaseResolvableCondition(Descendant.class,
                criteria));
    }

    /**
     * <strong>Goal:</strong> Verify a hierarchy-aware resolvability check
     * rejects a key that no class declares.
     * <p>
     * <strong>Start state:</strong> A base class with one descendant-defined
     * key.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a criteria that names an unknown key.</li>
     * <li>Check the criteria across the hierarchy.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The criteria is not database-resolvable.
     */
    @Test
    public void testHierarchyResolvabilityRejectsUnknownKey() {
        Criteria criteria = Criteria.where().key("unknown")
                .operator(Operator.EQUALS).value("value").build();

        Assert.assertFalse(Record.isDatabaseResolvableCondition(Base.class,
                criteria, true));
    }

}
