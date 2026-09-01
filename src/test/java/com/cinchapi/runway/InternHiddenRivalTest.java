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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.ForwardingConcourse;
import com.cinchapi.concourse.lang.CommandGroup;
import com.cinchapi.runway.InternTest.Account;
import com.cinchapi.runway.InternTest.RacingUser;
import com.cinchapi.runway.db.ConcourseProvider;

/**
 * Race reproduction tests for {@link Runway#intern(Record) intern} when a
 * rival's committed identity claim is invisible to intern's lookup but visible
 * to the save's deferred {@link Unique} enforcement read, without any commit
 * conflict in between &mdash; the sequence observed in production where the
 * rival's commit lands inside the save's bulk submit window.
 *
 * @author Jeff Nelson
 */
public class InternHiddenRivalTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that {@code intern} adopts a rival that
     * claimed the identity when the lookup misses the rival but the save's
     * {@link Unique} enforcement read observes it with no commit conflict.
     * <p>
     * <strong>Start state:</strong> One saved {@link RacingUser} (the rival),
     * and a {@link HidingConcourseConnectionPool} installed on the
     * {@link Runway}, armed to hide the rival from exactly one read.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the rival {@link RacingUser}.</li>
     * <li>Arm the pool to hide the rival's id from the next read result that
     * contains it, so intern's lookup reports the identity as unclaimed.</li>
     * <li>Call {@code intern} with a new {@link RacingUser} that has the same
     * email.</li>
     * <li>Count every {@link RacingUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns the rival (same id and state)
     * and exactly one {@link RacingUser} exists.
     */
    @Test
    public void testInternAdoptsRivalTheLookupCouldNotSee() {
        HidingConcourseConnectionPool pool = new HidingConcourseConnectionPool(
                Concourse.connect("localhost", server.getClientPort(), "admin",
                        "admin", environment));
        Reflection.set("connections", pool, runway); // (authorized)
        RacingUser winner = new RacingUser("race@example.com", "Winner");
        Assert.assertTrue(runway.save(winner));
        pool.hideOnce(winner.id());
        RacingUser result = runway
                .intern(new RacingUser("race@example.com", "Loser"));
        Assert.assertTrue(pool.fired());
        Assert.assertEquals(winner.id(), result.id());
        Assert.assertEquals("Winner", result.name);
        Assert.assertEquals(1, runway.count(RacingUser.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} adopts a rival that
     * claims every part of a multi-constraint identity when one constraint's
     * lookup misses the rival and another observes it.
     * <p>
     * <strong>Start state:</strong> One saved {@link Account} (the rival), and
     * a {@link HidingConcourseConnectionPool} installed on the {@link Runway},
     * armed to hide the rival from exactly one read.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save the rival {@link Account}.</li>
     * <li>Arm the pool to hide the rival's id from the next read result that
     * contains it, so one constraint's lookup reports its part of the identity
     * as unclaimed while another observes the rival.</li>
     * <li>Call {@code intern} with a new {@link Account} that has the same
     * email and handle.</li>
     * <li>Count every {@link Account}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns the rival (same id and state)
     * and exactly one {@link Account} exists.
     */
    @Test
    public void testInternAdoptsRivalWhenOneConstraintLookupMissedIt() {
        HidingConcourseConnectionPool pool = new HidingConcourseConnectionPool(
                Concourse.connect("localhost", server.getClientPort(), "admin",
                        "admin", environment));
        Reflection.set("connections", pool, runway); // (authorized)
        Account winner = new Account("race@example.com", "racer", "Winner");
        Assert.assertTrue(runway.save(winner));
        pool.hideOnce(winner.id());
        Account result = runway
                .intern(new Account("race@example.com", "racer", "Loser"));
        Assert.assertTrue(pool.fired());
        Assert.assertEquals(winner.id(), result.id());
        Assert.assertEquals("Winner", result.bio);
        Assert.assertEquals(1, runway.count(Account.class));
    }

    /**
     * A {@link ConnectionPool} whose connections are {@link HidingConcourse}
     * instances, so a test can make one read miss a designated record the way a
     * mid-submit rival commit is missed in production.
     *
     * @author Jeff Nelson
     */
    private static final class HidingConcourseConnectionPool
            extends ConnectionPool implements
            ConcourseProvider {

        /**
         * The id every pooled connection hides from the next read result that
         * contains it, or {@code 0} when no hiding is armed.
         */
        private final AtomicLong hidden;

        /**
         * Whether a pooled connection hid the armed id from a read.
         */
        private final AtomicBoolean fired;

        /**
         * Construct a new instance whose pooled {@link HidingConcourse}
         * connections each forward to a copy of {@code concourse}.
         *
         * @param concourse the {@link Concourse} whose connection is copied
         */
        HidingConcourseConnectionPool(Concourse concourse) {
            this(concourse, new AtomicLong(), new AtomicBoolean());
        }

        /**
         * Construct a new instance.
         *
         * @param concourse the {@link Concourse} whose connection is copied
         * @param hidden the hidden-id state shared by all pooled connections
         * @param fired the hid-a-read state shared by all pooled connections
         */
        private HidingConcourseConnectionPool(Concourse concourse,
                AtomicLong hidden, AtomicBoolean fired) {
            super(() -> new HidingConcourse(
                    Concourse.copyExistingConnection(concourse), hidden, fired),
                    1);
            this.hidden = hidden;
            this.fired = fired;
        }

        /**
         * Arm the pool to hide {@code id} from the next read result that
         * contains it, after which reads pass through untouched.
         *
         * @param id the record id to hide once
         */
        void hideOnce(long id) {
            hidden.set(id);
            fired.set(false);
        }

        /**
         * Return whether the armed id was hidden from a read, so a test can
         * assert that it exercised the race instead of passing on reads that
         * observed the record all along.
         *
         * @return {@code true} if a read result was stripped
         */
        boolean fired() {
            return fired.get();
        }

        @Override
        protected Queue<Concourse> buildQueue(int size) {
            return new ConcurrentLinkedQueue<>();
        }

        @Override
        protected Concourse getConnection() {
            return supplier.get();
        }
    }

    /**
     * A {@link ForwardingConcourse} that strips a designated record id from the
     * first read result that contains it, then forwards everything untouched,
     * replaying the production sequence where a rival's committed claim is
     * invisible to one read and visible to the next.
     *
     * @author Jeff Nelson
     */
    private static final class HidingConcourse extends ForwardingConcourse {

        /**
         * The id to hide from the next read result that contains it, or
         * {@code 0} when no hiding is armed.
         */
        private final AtomicLong hidden;

        /**
         * Whether this connection, or another in the pool, hid the armed id
         * from a read.
         */
        private final AtomicBoolean fired;

        /**
         * Construct a new instance.
         *
         * @param concourse the delegate {@link Concourse}
         * @param hidden the hidden-id state shared across the pool
         * @param fired the hid-a-read state shared across the pool
         */
        HidingConcourse(Concourse concourse, AtomicLong hidden,
                AtomicBoolean fired) {
            super(concourse);
            this.hidden = hidden;
            this.fired = fired;
        }

        @Override
        public List<Object> submit(CommandGroup group) {
            List<Object> results = super.submit(group);
            long id = hidden.get();
            if(id != 0) {
                List<Object> sanitized = new ArrayList<>(results.size());
                boolean stripped = false;
                for (Object result : results) {
                    if(result instanceof Set
                            && ((Set<?>) result).contains(id)) {
                        Set<Object> copy = new LinkedHashSet<>((Set<?>) result);
                        copy.remove(id);
                        sanitized.add(copy);
                        stripped = true;
                    }
                    else if(result instanceof Map
                            && ((Map<?, ?>) result).containsKey(id)) {
                        Map<Object, Object> copy = new LinkedHashMap<>(
                                (Map<?, ?>) result);
                        copy.remove(id);
                        sanitized.add(copy);
                        stripped = true;
                    }
                    else {
                        sanitized.add(result);
                    }
                }
                if(stripped) {
                    hidden.set(0);
                    fired.set(true);
                    return sanitized;
                }
                else {
                    return results;
                }
            }
            else {
                return results;
            }
        }

        @Override
        protected ForwardingConcourse $this(Concourse concourse) {
            return new HidingConcourse(concourse, hidden, fired);
        }
    }

}
