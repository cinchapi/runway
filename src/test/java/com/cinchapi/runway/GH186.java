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
import com.cinchapi.concourse.thrift.Operator;
import com.google.common.collect.Sets;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/186">GH-186</a>.
 *
 * @author ptt
 */
public class GH186 extends RunwayBaseClientServerTest {

    /**
     * A tenant that owns {@link Seat Seats}.
     *
     * @author ptt
     */
    class Tenant extends Record {

        /**
         * The tenant's seats.
         */
        public Set<Seat> seats = Sets.newLinkedHashSet();

    }

    /**
     * A seat assigned to a {@link User}.
     *
     * @author ptt
     */
    class Seat extends Record {

        /**
         * The assigned user.
         */
        public final User user;

        /**
         * Construct a new instance.
         *
         * @param tenant the {@link Tenant} that owns the seat
         * @param user the assigned {@link User}
         */
        public Seat(Tenant tenant, User user) {
            this.user = user;
            tenant.seats.add(this);
        }

    }

    /**
     * A user identified by a stable value.
     *
     * @author ptt
     */
    class User extends Record {

        /**
         * The stable identifier.
         */
        public final String userId;

        /**
         * Construct a new instance.
         *
         * @param userId the stable identifier
         */
        public User(String userId) {
            this.userId = userId;
        }

    }

    /**
     * A pool of credits.
     *
     * @author ptt
     */
    abstract class CreditPool extends Record {

        /**
         * The pool label.
         */
        public final String label;

        /**
         * Construct a new instance.
         *
         * @param label the pool label
         */
        public CreditPool(String label) {
            this.label = label;
        }

    }

    /**
     * A credit pool with no tenant.
     *
     * @author ptt
     */
    class CreditGrant extends CreditPool {

        /**
         * Construct a new instance.
         *
         * @param label the pool label
         */
        public CreditGrant(String label) {
            super(label);
        }

    }

    /**
     * A credit pool attached to a {@link Tenant}.
     *
     * @author ptt
     */
    class ExtraCreditPool extends CreditPool {

        /**
         * The attached tenant.
         */
        public final Tenant tenant;

        /**
         * Construct a new instance.
         *
         * @param label the pool label
         * @param tenant the attached {@link Tenant}
         */
        public ExtraCreditPool(String label, Tenant tenant) {
            super(label);
            this.tenant = tenant;
        }

    }

    /**
     * <strong>Goal:</strong> Verify a hierarchy query pushes criteria that
     * names a key declared by a descendant.
     * <p>
     * <strong>Start state:</strong> A hierarchy with one matching
     * {@link ExtraCreditPool} and one {@link CreditGrant} that has no tenant.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a criteria that names the descendant key and a navigation key
     * rooted at it.</li>
     * <li>Find exact {@link ExtraCreditPool ExtraCreditPools} and then find any
     * {@link CreditPool CreditPools} with the criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both queries return the same matching pool.
     */
    @Test
    public void testFindAnyResolvesSubclassKeyInDatabase() {
        Tenant tenant = new Tenant();
        User user = new User("u1");
        new Seat(tenant, user);
        ExtraCreditPool purchase = new ExtraCreditPool("purchase", tenant);
        CreditGrant grant = new CreditGrant("grant");
        runway.save(tenant, user, purchase, grant);

        Criteria criteria = Criteria.where().key("tenant")
                .operator(Operator.LINKS_TO).value(tenant.id()).and()
                .key("tenant.seats.user.userId").operator(Operator.EQUALS)
                .value("u1").build();

        Assert.assertEquals(runway.find(ExtraCreditPool.class, criteria),
                runway.findAny(CreditPool.class, criteria));
    }

    /**
     * <strong>Goal:</strong> Verify a hierarchy count accepts criteria that
     * names a key declared by a descendant.
     * <p>
     * <strong>Start state:</strong> A hierarchy with one matching
     * {@link ExtraCreditPool} and one {@link CreditGrant} that has no tenant.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a criteria that names the descendant key.</li>
     * <li>Count exact {@link ExtraCreditPool ExtraCreditPools} and then count
     * any {@link CreditPool CreditPools} with the criteria.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both queries count the same matching pool.
     */
    @Test
    public void testCountAnyResolvesSubclassKeyInDatabase() {
        Tenant tenant = new Tenant();
        User user = new User("u1");
        new Seat(tenant, user);
        ExtraCreditPool purchase = new ExtraCreditPool("purchase", tenant);
        CreditGrant grant = new CreditGrant("grant");
        runway.save(tenant, user, purchase, grant);

        Criteria criteria = Criteria.where().key("tenant")
                .operator(Operator.LINKS_TO).value(tenant.id()).build();

        Assert.assertEquals(runway.count(ExtraCreditPool.class, criteria),
                runway.countAny(CreditPool.class, criteria));
    }

}
