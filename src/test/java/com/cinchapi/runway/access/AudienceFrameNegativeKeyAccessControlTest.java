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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

/**
 * Regression test verifying that {@link Audience#frame} honors a
 * {@code -}-prefixed key against an audience whose readable set is restricted
 * (not {@link AccessControl#ALL_KEYS}). The result is the intersection of the
 * audience's allowlist with the caller's exclusions &mdash; never an empty map.
 *
 * @author Jeff Nelson
 */
public class AudienceFrameNegativeKeyAccessControlTest
        extends AudienceAccessControlBaseTest {

    /**
     * <strong>Goal:</strong> Verify that a {@code -}-prefixed key passed to
     * {@link Audience#frame} is honored as an exclusion against the
     * intersection of the caller's request and the audience's readable set,
     * even when the readable set is restricted to a specific subset of fields.
     * <p>
     * <strong>Start state:</strong> A {@link Candidate} populated with
     * {@code name}, {@code email}, {@code skills}, {@code yearsExperience}, and
     * {@code location}, plus the non-readable {@code resume} field. An
     * {@link EmployerUser} audience whose {@code $readableBy} rule for
     * {@link Candidate} returns {@code {name, email, skills, yearsExperience,
     * location}}, which is not {@link AccessControl#ALL_KEYS}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code employerUser.frame(ImmutableSet.of("-name"),
     *       candidate)}.</li>
     * <li>Inspect the returned map for the presence of expected readable keys
     * and the absence of {@code name}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is non-null and contains
     * {@code email}, {@code skills}, {@code yearsExperience}, and
     * {@code location} (the readable keys other than {@code name}). It does
     * <strong>not</strong> contain {@code name} (excluded by {@code -name}) and
     * does <strong>not</strong> contain {@code resume} (not in the readable
     * set).
     */
    @Test
    public void testFrameHonorsNegativeKeyAgainstRestrictedReadable() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.skills = "Java, Python";
        candidate.yearsExperience = 5;
        candidate.location = "San Francisco";
        candidate.resume = "Sensitive resume content";

        EmployerUser employerUser = new EmployerUser();
        employerUser.name = "HR Manager";
        employerUser.email = "hr@techcorp.com";

        Map<String, Object> result = employerUser
                .frame(ImmutableSet.of("-name"), candidate);

        Assert.assertNotNull("subject must be discoverable", result);
        Assert.assertFalse("-name should exclude name",
                result.containsKey("name"));
        Assert.assertTrue("email is readable", result.containsKey("email"));
        Assert.assertTrue("skills is readable", result.containsKey("skills"));
        Assert.assertTrue("yearsExperience is readable",
                result.containsKey("yearsExperience"));
        Assert.assertTrue("location is readable",
                result.containsKey("location"));
        Assert.assertFalse("resume is not readable",
                result.containsKey("resume"));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code -}-prefixed key passed to
     * {@link Audience#frame} against an {@link AccessControl#ALL_KEYS} readable
     * returns the subject's defaults minus the excluded key. This exercises the
     * {@code readable == ALL_KEYS} forwarding path with a pure negative call
     * shape, complementing the restricted-readable case in
     * {@link #testFrameHonorsNegativeKeyAgainstRestrictedReadable}.
     * <p>
     * <strong>Start state:</strong> A {@link Candidate} populated with
     * {@code name}, {@code email}, {@code skills}, {@code yearsExperience},
     * {@code location}, and {@code resume}. An {@link Admin} audience whose
     * {@code $readableBy} rule for {@link Candidate} returns
     * {@link AccessControl#ALL_KEYS}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code admin.frame(ImmutableSet.of("-name"), candidate)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result does not contain {@code name} but
     * contains every other intrinsic key, including {@code resume} (which an
     * unrestricted audience may see).
     */
    @Test
    public void testFrameHonorsNegativeKeyAgainstAllKeysReadable() {
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.skills = "Java, Python";
        candidate.yearsExperience = 5;
        candidate.location = "San Francisco";
        candidate.resume = "Sensitive resume content";

        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@company.com";

        Map<String, Object> result = admin.frame(ImmutableSet.of("-name"),
                candidate);

        Assert.assertNotNull("subject must be discoverable", result);
        Assert.assertFalse("-name should exclude name",
                result.containsKey("name"));
        Assert.assertTrue("email is readable", result.containsKey("email"));
        Assert.assertTrue("skills is readable", result.containsKey("skills"));
        Assert.assertTrue("yearsExperience is readable",
                result.containsKey("yearsExperience"));
        Assert.assertTrue("location is readable",
                result.containsKey("location"));
        Assert.assertTrue("resume is readable under ALL_KEYS",
                result.containsKey("resume"));
    }

}
