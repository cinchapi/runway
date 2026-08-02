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

/**
 * Unit tests for {@link AtomicRetryPolicy}.
 *
 * @author Jeff Nelson
 */
public class AtomicRetryPolicyTest {

    /**
     * <strong>Goal:</strong> Verify that a negative retry limit is rejected.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link AtomicRetryPolicy#create(int, long)} with a limit of
     * {@code -1}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateRejectsNegativeLimit() {
        AtomicRetryPolicy.create(-1, 10);
    }

    /**
     * <strong>Goal:</strong> Verify that a negative backoff interval is
     * rejected.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link AtomicRetryPolicy#create(int, long)} with a backoff of
     * {@code -1} milliseconds.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateRejectsNegativeBackoff() {
        AtomicRetryPolicy.create(5, -1);
    }

    /**
     * <strong>Goal:</strong> Verify that the configured retry limit is reported
     * back by the {@link AtomicRetryPolicy#limit() limit} accessor.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create an {@link AtomicRetryPolicy} with a limit of {@code 7}.</li>
     * <li>Call {@link AtomicRetryPolicy#limit() limit}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The accessor returns {@code 7}.
     */
    @Test
    public void testLimitReturnsConfiguredValue() {
        AtomicRetryPolicy policy = AtomicRetryPolicy.create(7, 10);
        Assert.assertEquals(7, policy.limit());
    }

    /**
     * <strong>Goal:</strong> Verify that a zero retry limit is permitted so a
     * caller can disable retries entirely.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create an {@link AtomicRetryPolicy} with a limit of {@code 0}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The policy is created and reports a limit of
     * {@code 0}.
     */
    @Test
    public void testZeroLimitIsPermitted() {
        Assert.assertEquals(0, AtomicRetryPolicy.create(0, 10).limit());
    }

    /**
     * <strong>Goal:</strong> Verify that a zero backoff interval never sleeps
     * and never throws, so tests and latency-sensitive callers can pause-free
     * retry.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create an {@link AtomicRetryPolicy} with a backoff of {@code 0}
     * milliseconds.</li>
     * <li>Call {@link AtomicRetryPolicy#backoff(int) backoff} for several
     * attempt numbers.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Every call returns without an exception.
     */
    @Test
    public void testZeroBackoffNeverSleepsOrThrows() {
        AtomicRetryPolicy policy = AtomicRetryPolicy.create(5, 0);
        for (int attempt = 1; attempt <= 5; ++attempt) {
            policy.backoff(attempt);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a very large attempt number does not
     * overflow the backoff computation.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create an {@link AtomicRetryPolicy} with a backoff of {@code 1}
     * millisecond.</li>
     * <li>Call {@link AtomicRetryPolicy#backoff(int) backoff} with attempt
     * {@code 100}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call returns without an exception (the
     * pause is bounded by the policy's backoff ceiling instead of an overflowed
     * interval).
     */
    @Test
    public void testBackoffBoundsLargeAttemptNumbers() {
        AtomicRetryPolicy.create(100, 1).backoff(100);
    }

    /**
     * <strong>Goal:</strong> Verify that a non-positive attempt number is
     * rejected because attempts are 1-based.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link AtomicRetryPolicy#backoff(int) backoff} with attempt
     * {@code 0}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBackoffRejectsNonPositiveAttempt() {
        AtomicRetryPolicy.defaults().backoff(0);
    }

    /**
     * <strong>Goal:</strong> Verify that the default policy permits at least
     * one retry, so contention does not immediately fail out of the box.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link AtomicRetryPolicy#defaults() defaults}.</li>
     * <li>Call {@link AtomicRetryPolicy#limit() limit}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The limit is greater than {@code 0}.
     */
    @Test
    public void testDefaultsPermitRetries() {
        Assert.assertTrue(AtomicRetryPolicy.defaults().limit() > 0);
    }

}
