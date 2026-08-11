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

/**
 * Tests for {@link AccessControl#authorize(Audience) authorize}, covering how a
 * {@link Record Record's} creation rules admit or refuse anonymous and
 * identified {@link Audience audiences}.
 *
 * @author Jeff Nelson
 */
public class AccessControlAuthorizeTest extends AudienceAccessControlBaseTest {

    /**
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} is
     * authorized when the {@link Record} is creatable by anonymous users, even
     * when {@code $isCreatableBy} does not admit the anonymous audience
     * instance.
     * <p>
     * <strong>Start state:</strong> No database state is needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct an {@link Inquiry}, which anonymous users may create but
     * only an {@link Admin} may create among identified audiences.</li>
     * <li>Call {@code authorize} with {@link Audience#anonymous()}.</li>
     * <li>Call {@code authorize} with {@code null}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Neither call throws a
     * {@link RestrictedAccessException}.
     */
    @Test
    public void testAuthorizePermitsAnonymousWhenCreatableByAnonymous() {
        Inquiry inquiry = new Inquiry();
        inquiry.message = "Is the posted role still open?";
        boolean threw = false;
        try {
            inquiry.authorize(Audience.anonymous());
            inquiry.authorize(null);
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertFalse(threw);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code authorize} agrees with the
     * create gate of an {@link Audience} for an anonymous audience.
     * <p>
     * <strong>Start state:</strong> No database state is needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code create} on {@link Audience#anonymous()} for the
     * {@link Inquiry} class.</li>
     * <li>Call {@code authorize} on the created {@link Inquiry} with
     * {@link Audience#anonymous()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code create} call returns an
     * {@link Inquiry} and the {@code authorize} call does not throw a
     * {@link RestrictedAccessException}.
     */
    @Test
    public void testAuthorizeAgreesWithAudienceCreateGateForAnonymous() {
        Inquiry inquiry = Audience.anonymous().create(Inquiry.class);
        Assert.assertNotNull(inquiry);
        boolean threw = false;
        try {
            inquiry.authorize(Audience.anonymous());
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertFalse(threw);
    }

    /**
     * <strong>Goal:</strong> Verify that an anonymous {@link Audience} is
     * refused when the {@link Record} is not creatable by anonymous users, even
     * when {@code $isCreatableBy} admits every identified audience.
     * <p>
     * <strong>Start state:</strong> No database state is needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Feedback}, which every identified {@link Audience}
     * may create but anonymous users may not.</li>
     * <li>Call {@code authorize} with {@link Audience#anonymous()} and catch
     * the expected exception.</li>
     * <li>Call {@code authorize} with {@code null} and catch the expected
     * exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls throw a
     * {@link RestrictedAccessException}.
     */
    @Test
    public void testAuthorizeRefusesAnonymousWhenNotCreatableByAnonymous() {
        Feedback feedback = new Feedback();
        feedback.comments = "The interview process was smooth";
        boolean threw = false;
        try {
            feedback.authorize(Audience.anonymous());
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        threw = false;
        try {
            feedback.authorize(null);
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
    }

    /**
     * <strong>Goal:</strong> Verify that an identified {@link Audience} is
     * authorized when {@code $isCreatableBy} admits it, even when the
     * {@link Record} is not creatable by anonymous users.
     * <p>
     * <strong>Start state:</strong> No database state is needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Candidate}.</li>
     * <li>Construct an {@link Application}, which a {@link Candidate} may
     * create but anonymous users may not.</li>
     * <li>Call {@code authorize} on the {@link Application} with the
     * {@link Candidate}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call does not throw a
     * {@link RestrictedAccessException}.
     */
    @Test
    public void testAuthorizePermitsAudienceWhenCreatableByAudience() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        Application application = new Application();
        boolean threw = false;
        try {
            application.authorize(candidate);
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertFalse(threw);
    }

    /**
     * <strong>Goal:</strong> Verify that an identified {@link Audience} is
     * refused when {@code $isCreatableBy} does not admit it, even when the
     * {@link Record} is creatable by anonymous users.
     * <p>
     * <strong>Start state:</strong> No database state is needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Candidate}.</li>
     * <li>Construct an {@link Inquiry}, which anonymous users may create but
     * only an {@link Admin} may create among identified audiences.</li>
     * <li>Call {@code authorize} on the {@link Inquiry} with the
     * {@link Candidate} and catch the expected exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws a
     * {@link RestrictedAccessException}.
     */
    @Test
    public void testAuthorizeRefusesAudienceWhenNotCreatableByAudience() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        Inquiry inquiry = new Inquiry();
        boolean threw = false;
        try {
            inquiry.authorize(candidate);
        }
        catch (RestrictedAccessException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
    }

    /**
     * An access controlled {@link Record} that anonymous users may create but
     * that only an {@link Admin} may create among identified audiences.
     *
     * @author Jeff Nelson
     */
    public static class Inquiry extends Record implements AccessControl {

        /**
         * The message submitted with the inquiry.
         */
        public String message;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return true;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return audience instanceof Admin ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return audience instanceof Admin ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }
    }

    /**
     * An access controlled {@link Record} that every identified
     * {@link Audience} may create but that anonymous users may not.
     *
     * @author Jeff Nelson
     */
    public static class Feedback extends Record implements AccessControl {

        /**
         * The comments submitted with the feedback.
         */
        public String comments;

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return audience instanceof Admin;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return audience instanceof Admin ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return audience instanceof Admin ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }
    }

}
