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
package com.cinchapi.runway.access;

import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.Record;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests for {@link Audience#$checkIfVisible()} that verify an anonymous
 * {@link Audience} uses the same implicit discoverability logic as a
 * non-anonymous {@link Audience}.
 * <p>
 * The implicit discoverability rule states that a {@link Record} that can be
 * read or written by an {@link Audience} is implicitly discoverable by that
 * {@link Audience}, even when explicit discoverability is not granted.
 * </p>
 *
 * @author Jeff Nelson
 */
public class AudienceAnonymousImplicitDiscoverabilityTest
        extends AudienceAccessControlBaseTest {

    /**
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} can
     * discover an {@link AccessControl} record that is readable by anonymous,
     * even when {@link AccessControl#$isDiscoverableByAnonymous()} returns
     * {@code false}.
     * <p>
     * <strong>Start state:</strong> A {@link Candidate} whose
     * {@code $isDiscoverableByAnonymous()} returns {@code false} and whose
     * {@code $readableByAnonymous()} returns a
     * non-{@link AccessControl#NO_KEYS} value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Candidate} with required fields set.</li>
     * <li>Obtain the anonymous {@link Audience} via
     * {@link Audience#anonymous()}.</li>
     * <li>Evaluate the {@code $checkIfVisible()} predicate against the
     * {@link Candidate}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The predicate returns {@code true} because
     * readability implies implicit discoverability.
     */
    @Test
    public void testAnonymousCanDiscoverRecordReadableByAnonymous() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Doe";
        candidate.email = "jane@example.com";
        Audience anonymous = Audience.anonymous();
        Assert.assertTrue(anonymous.$checkIfVisible().test(candidate));
    }

    /**
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} can
     * discover an {@link AccessControl} record that is writable by anonymous,
     * even when {@link AccessControl#$isDiscoverableByAnonymous()} returns
     * {@code false} and {@link AccessControl#$readableByAnonymous()} returns
     * {@link AccessControl#NO_KEYS}.
     * <p>
     * <strong>Start state:</strong> A {@link WritableByAnonymousEntity} whose
     * {@code $isDiscoverableByAnonymous()} returns {@code false}, whose
     * {@code $readableByAnonymous()} returns {@link AccessControl#NO_KEYS}, and
     * whose {@code $writableByAnonymous()} returns a
     * non-{@link AccessControl#NO_KEYS} value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link WritableByAnonymousEntity}.</li>
     * <li>Obtain the anonymous {@link Audience} via
     * {@link Audience#anonymous()}.</li>
     * <li>Evaluate the {@code $checkIfVisible()} predicate against the
     * entity.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The predicate returns {@code true} because
     * writability implies implicit discoverability.
     */
    @Test
    public void testAnonymousCanDiscoverRecordWritableByAnonymous() {
        WritableByAnonymousEntity entity = new WritableByAnonymousEntity();
        Audience anonymous = Audience.anonymous();
        Assert.assertTrue(anonymous.$checkIfVisible().test(entity));
    }

    /**
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} cannot
     * discover an {@link AccessControl} record that has no anonymous access of
     * any kind.
     * <p>
     * <strong>Start state:</strong> An {@link Application} whose
     * {@code $isDiscoverableByAnonymous()} returns {@code false}, whose
     * {@code $readableByAnonymous()} returns {@link AccessControl#NO_KEYS}, and
     * whose {@code $writableByAnonymous()} returns
     * {@link AccessControl#NO_KEYS}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create an {@link Application}.</li>
     * <li>Obtain the anonymous {@link Audience} via
     * {@link Audience#anonymous()}.</li>
     * <li>Evaluate the {@code $checkIfVisible()} predicate against the
     * {@link Application}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The predicate returns {@code false} because no
     * form of anonymous access is granted.
     */
    @Test
    public void testAnonymousCannotDiscoverRecordWithNoAnonymousAccess() {
        Application application = new Application();
        Audience anonymous = Audience.anonymous();
        Assert.assertFalse(anonymous.$checkIfVisible().test(application));
    }

    /**
     * <strong>Goal:</strong> Verify that the anonymous {@link Audience
     * Audience's} implicit discoverability logic is consistent with the
     * non-anonymous path &mdash; both should treat readability as implying
     * discoverability.
     * <p>
     * <strong>Start state:</strong> A {@link Candidate} that is not explicitly
     * discoverable by another {@link Candidate} or by anonymous, but is
     * readable by both.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create two {@link Candidate Candidates}: a viewer and a subject.</li>
     * <li>Evaluate the viewer's {@code $checkIfVisible()} predicate against the
     * subject (non-anonymous path).</li>
     * <li>Evaluate the anonymous {@code $checkIfVisible()} predicate against
     * the same subject.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both predicates return {@code true},
     * confirming that the anonymous path applies the same implicit
     * discoverability rule as the non-anonymous path.
     */
    @Test
    public void testAnonymousImplicitDiscoverabilityMatchesNonAnonymous() {
        Candidate viewer = new Candidate();
        viewer.name = "Alice";
        viewer.email = "alice@example.com";
        Candidate subject = new Candidate();
        subject.name = "Bob";
        subject.email = "bob@example.com";
        Audience anonymous = Audience.anonymous();
        boolean nonAnonymousVisible = viewer.$checkIfVisible().test(subject);
        boolean anonymousVisible = anonymous.$checkIfVisible().test(subject);
        Assert.assertTrue(nonAnonymousVisible);
        Assert.assertTrue(anonymousVisible);
    }

    /**
     * A {@link Record} that is not explicitly discoverable by any
     * {@link Audience}, but is writable by all {@link Audience audiences}
     * including anonymous. This type exercises the implicit discoverability
     * fallback through writability alone.
     */
    protected static class WritableByAnonymousEntity extends Record implements
            AccessControl {

        /**
         * A writable status field.
         */
        public String status;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ImmutableSet.of("status");
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return ImmutableSet.of("status");
        }
    }

}
