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

import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.base.CheckedExceptions;
import com.google.common.base.Preconditions;

/**
 * An {@link AtomicRetryPolicy} governs how an atomic read-modify-write
 * operation responds to contention: how many times a lost race may be retried
 * and how long to pause between attempts.
 * <p>
 * The policy applies to every operation that retries after losing a race with a
 * concurrent writer (e.g. the {@link Record} single-key atomic operations).
 * When an operation exhausts the retry {@link #limit() limit}, it throws
 * {@link RetryExhaustedException} instead of returning a non-committed result.
 * </p>
 * <p>
 * Between attempts, {@link #backoff(int) backoff} pauses the caller for a
 * randomized interval that grows with the attempt number and is bounded by a
 * ceiling, so contending callers disperse instead of colliding again in
 * lockstep. A backoff interval of {@code 0} disables the pause entirely.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public final class AtomicRetryPolicy {

    /**
     * The default bound on the number of retries.
     */
    private static final int DEFAULT_LIMIT = 5;

    /**
     * The default base interval, in milliseconds, for the jittered backoff.
     */
    private static final long DEFAULT_BACKOFF_MILLIS = 10;

    /**
     * The largest exponent applied to the backoff base, which caps the pause
     * ceiling regardless of the attempt number.
     */
    private static final int MAX_BACKOFF_EXPONENT = 10;

    /**
     * The shared instance returned from {@link #defaults()}.
     */
    private static final AtomicRetryPolicy DEFAULT = new AtomicRetryPolicy(
            DEFAULT_LIMIT, DEFAULT_BACKOFF_MILLIS);

    /**
     * Return an {@link AtomicRetryPolicy} that permits up to {@code limit}
     * retries after an operation's initial attempt and paces them with a
     * jittered backoff scaled from {@code backoffMillis}.
     *
     * @param limit the maximum number of retries after the initial attempt;
     *            {@code 0} disables retries
     * @param backoffMillis the base interval, in milliseconds, from which each
     *            pause is scaled; {@code 0} disables the pause
     * @return the {@link AtomicRetryPolicy}
     * @throws IllegalArgumentException if {@code limit} or
     *             {@code backoffMillis} is negative
     */
    public static AtomicRetryPolicy create(int limit, long backoffMillis) {
        return new AtomicRetryPolicy(limit, backoffMillis);
    }

    /**
     * Return the {@link AtomicRetryPolicy} that is used when no explicit policy
     * is configured.
     *
     * @return the default {@link AtomicRetryPolicy}
     */
    public static AtomicRetryPolicy defaults() {
        return DEFAULT;
    }

    /**
     * The maximum number of retries permitted after an operation's initial
     * attempt.
     */
    private final int limit;

    /**
     * The base interval, in milliseconds, from which each pause is scaled.
     */
    private final long backoffMillis;

    /**
     * Construct a new instance.
     *
     * @param limit the maximum number of retries after the initial attempt
     * @param backoffMillis the base interval, in milliseconds, from which each
     *            pause is scaled
     */
    private AtomicRetryPolicy(int limit, long backoffMillis) {
        Preconditions.checkArgument(limit >= 0,
                "The retry limit cannot be negative");
        Preconditions.checkArgument(backoffMillis >= 0,
                "The backoff interval cannot be negative");
        this.limit = limit;
        this.backoffMillis = backoffMillis;
    }

    /**
     * Pause the calling thread before retry {@code attempt} for a randomized
     * interval that grows with the attempt number, dispersing contending
     * callers so they do not collide again in lockstep.
     * <p>
     * The pause never exceeds the policy's ceiling, no matter how large
     * {@code attempt} grows. If the calling thread is interrupted while paused,
     * the interrupt flag is restored and the {@link InterruptedException}
     * propagates as a {@link RuntimeException} so the caller abandons the retry
     * loop instead of silently continuing to contend.
     * </p>
     *
     * @param attempt the 1-based number of the attempt that just failed
     * @throws IllegalArgumentException if {@code attempt} is not positive
     */
    public void backoff(int attempt) {
        Preconditions.checkArgument(attempt > 0,
                "The attempt number must be positive");
        long ceiling = backoffMillis
                * (1L << Math.min(attempt - 1, MAX_BACKOFF_EXPONENT));
        long delay = ceiling > 0
                ? ThreadLocalRandom.current().nextLong(ceiling + 1)
                : 0;
        if(delay > 0) {
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
    }

    /**
     * Return the maximum number of retries permitted after an operation's
     * initial attempt.
     *
     * @return the retry limit
     */
    public int limit() {
        return limit;
    }

}
