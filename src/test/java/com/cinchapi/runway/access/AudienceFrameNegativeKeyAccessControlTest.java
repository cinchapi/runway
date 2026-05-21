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
 * Regression test demonstrating that {@link Audience#frame} silently drops
 * {@code -}-prefixed keys when the audience's readable set is restricted (not
 * {@link AccessControl#ALL_KEYS}).
 * <p>
 * The intersection check at the heart of {@code frame} compares each requested
 * root against the audience's bare-key allowlist. A requested key like
 * {@code "-name"} has the literal {@code -} as part of its root and never
 * matches the bare {@code name} in the allowlist, so the exclusion is dropped
 * and the call resolves to an empty result.
 * </p>
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
     * <p>
     * <strong>Current behavior:</strong> {@code -name}'s root is the literal
     * string {@code "-name"}, which fails the intersection check against the
     * bare-key allowlist. The visible array is empty and the method returns an
     * empty map &mdash; the assertions on {@code email}, {@code skills},
     * {@code yearsExperience}, and {@code location} all fail.
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

}
