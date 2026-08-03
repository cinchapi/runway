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

/**
 * A {@link RetryExhaustedException} is thrown by an atomic read-modify-write
 * operation (e.g. the {@link Record} single-key atomic operations) that cannot
 * commit within the bounds of the governing {@link AtomicRetryPolicy}:
 * concurrent writes to the target data prevented the update from committing.
 * <p>
 * This is semantically distinct from a {@code null} or empty result, which
 * means no target existed. A {@link RetryExhaustedException} means the target
 * data existed but persistent contention prevented this caller from committing
 * its update; the caller may back off and retry.
 *
 * @author Javier Lores
 */
@SuppressWarnings("serial")
public class RetryExhaustedException extends RunwayException {

    /**
     * The number of attempts that were made before giving up.
     */
    private final int attempts;

    /**
     * Construct a new instance.
     *
     * @param attempts the number of attempts that were made before giving up
     */
    public RetryExhaustedException(int attempts) {
        super("Failed to atomically commit after " + attempts
                + " attempts due to persistent write contention");
        this.attempts = attempts;
    }

    /**
     * Return the number of attempts that were made before giving up.
     *
     * @return the attempt count
     */
    public int attempts() {
        return attempts;
    }

}
