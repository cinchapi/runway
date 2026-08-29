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
 * Tests for a hierarchy query whose criteria joins a navigation key to another
 * condition with {@code AND} while some records in the hierarchy do not store
 * the navigated key.
 *
 * @author Jeff Nelson
 */
public class HierarchyNavigationConjunctionTest
        extends RunwayBaseClientServerTest {

    /**
     * A {@link Record} that owns {@link PoolSeat PoolSeats}.
     */
    class PoolTenant extends Record {

        /**
         * The {@link PoolSeat PoolSeats} of this {@link PoolTenant}.
         */
        public Set<PoolSeat> seats = Sets.newLinkedHashSet();

    }

    /**
     * A {@link Record} that connects a {@link PoolTenant} to a
     * {@link PoolUser}.
     */
    class PoolSeat extends Record {

        /**
         * The {@link PoolUser} that occupies this {@link PoolSeat}.
         */
        public final PoolUser user;

        /**
         * Construct a new instance.
         *
         * @param tenant the {@link PoolTenant}
         * @param user the {@link PoolUser}
         */
        public PoolSeat(PoolTenant tenant, PoolUser user) {
            this.user = user;
            tenant.seats.add(this);
        }

    }

    /**
     * A {@link Record} with an external identifier.
     */
    class PoolUser extends Record {

        /**
         * The external identifier.
         */
        public final String userId;

        /**
         * Construct a new instance.
         *
         * @param userId the external identifier
         */
        public PoolUser(String userId) {
            this.userId = userId;
        }

    }

    /**
     * The root of the hierarchy that the tests query.
     */
    abstract class CreditPool extends Record {

        /**
         * The name of this {@link CreditPool}.
         */
        public final String label;

        /**
         * Construct a new instance.
         *
         * @param label the name
         */
        public CreditPool(String label) {
            this.label = label;
        }

    }

    /**
     * A {@link CreditPool} that stores no {@code tenant}.
     */
    class CreditGrant extends CreditPool {

        /**
         * Construct a new instance.
         *
         * @param label the name
         */
        public CreditGrant(String label) {
            super(label);
        }

    }

    /**
     * A {@link CreditPool} that stores a {@code tenant}.
     */
    class ExtraCreditPool extends CreditPool {

        /**
         * The {@link PoolTenant} that owns this {@link ExtraCreditPool}.
         */
        public final PoolTenant tenant;

        /**
         * Construct a new instance.
         *
         * @param label the name
         * @param tenant the {@link PoolTenant}
         */
        public ExtraCreditPool(String label, PoolTenant tenant) {
            super(label);
            this.tenant = tenant;
        }

    }

    /**
     * The {@link PoolTenant} that the tests query through.
     */
    private PoolTenant tenant;

    /**
     * The {@link ExtraCreditPool} that every query must match.
     */
    private ExtraCreditPool purchase;

    /**
     * Store one {@link ExtraCreditPool} that links to a {@link PoolTenant} and
     * one {@link CreditGrant} that stores no {@code tenant}.
     */
    private void setupData() {
        tenant = new PoolTenant();
        PoolUser user = new PoolUser("u1");
        new PoolSeat(tenant, user);
        purchase = new ExtraCreditPool("purchase", tenant);
        CreditGrant grant = new CreditGrant("grant");
        runway.save(tenant, user, purchase, grant);
    }

    /**
     * <strong>Goal:</strong> Verify a hierarchy find matches on a
     * {@link Operator#LINKS_TO} condition alone.
     * <p>
     * <strong>Start state:</strong> One {@link ExtraCreditPool} that links to a
     * {@link PoolTenant} and one {@link CreditGrant} that stores no
     * {@code tenant}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findAny} on {@link CreditPool} with
     * {@code tenant LINKS_TO} the tenant.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result holds the {@link ExtraCreditPool}.
     */
    @Test
    public void testFindAnyMatchesLinksToAlone() {
        setupData();
        Criteria criteria = Criteria.where().key("tenant")
                .operator(Operator.LINKS_TO).value(tenant.id()).build();
        Set<CreditPool> results = runway.findAny(CreditPool.class, criteria);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(purchase.id(), results.iterator().next().id());
    }

    /**
     * <strong>Goal:</strong> Verify a hierarchy find matches on a navigation
     * key alone.
     * <p>
     * <strong>Start state:</strong> One {@link ExtraCreditPool} that links to a
     * {@link PoolTenant} and one {@link CreditGrant} that stores no
     * {@code tenant}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findAny} on {@link CreditPool} with
     * {@code tenant.seats.user.userId} equals {@code "u1"}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result holds the {@link ExtraCreditPool}.
     */
    @Test
    public void testFindAnyMatchesNavigationKeyAlone() {
        setupData();
        Criteria criteria = Criteria.where().key("tenant.seats.user.userId")
                .operator(Operator.EQUALS).value("u1").build();
        Set<CreditPool> results = runway.findAny(CreditPool.class, criteria);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(purchase.id(), results.iterator().next().id());
    }

    /**
     * <strong>Goal:</strong> Verify a hierarchy find matches a conjunction of a
     * {@link Operator#LINKS_TO} condition and a navigation key while another
     * class in the hierarchy stores no value for the navigated key.
     * <p>
     * <strong>Start state:</strong> One {@link ExtraCreditPool} that links to a
     * {@link PoolTenant} and one {@link CreditGrant} that stores no
     * {@code tenant}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code findAny} on {@link CreditPool} with
     * {@code tenant LINKS_TO} the tenant AND {@code tenant.seats.user.userId}
     * equals {@code "u1"}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result holds the {@link ExtraCreditPool}.
     */
    @Test
    public void testFindAnyMatchesConjunctionWithNavigationKey() {
        setupData();
        Criteria criteria = Criteria.where().key("tenant")
                .operator(Operator.LINKS_TO).value(tenant.id()).and()
                .key("tenant.seats.user.userId").operator(Operator.EQUALS)
                .value("u1").build();
        Set<CreditPool> results = runway.findAny(CreditPool.class, criteria);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(purchase.id(), results.iterator().next().id());
    }

    /**
     * <strong>Goal:</strong> Verify an exact class find matches the same
     * conjunction.
     * <p>
     * <strong>Start state:</strong> One {@link ExtraCreditPool} that links to a
     * {@link PoolTenant} and one {@link CreditGrant} that stores no
     * {@code tenant}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code find} on {@link ExtraCreditPool} with
     * {@code tenant LINKS_TO} the tenant AND {@code tenant.seats.user.userId}
     * equals {@code "u1"}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result holds the {@link ExtraCreditPool}.
     */
    @Test
    public void testFindExactClassMatchesConjunctionWithNavigationKey() {
        setupData();
        Criteria criteria = Criteria.where().key("tenant")
                .operator(Operator.LINKS_TO).value(tenant.id()).and()
                .key("tenant.seats.user.userId").operator(Operator.EQUALS)
                .value("u1").build();
        Set<ExtraCreditPool> results = runway.find(ExtraCreditPool.class,
                criteria);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(purchase.id(), results.iterator().next().id());
    }

}
