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
 * A {@link SpuriousSaveFailureStrategy} determines how {@link Runway} handles
 * {@code TransactionException} failures during {@link Runway#save(Record...)
 * save} operations.
 * <p>
 * A save failure is <em>spurious</em> when a {@code TransactionException} is
 * thrown but none of the {@link Record Records} involved have stale data
 * &mdash; meaning no other transaction actually modified their data since they
 * were loaded or last saved. This typically occurs when {@link Unique @Unique}
 * constraint checks perform reads that intersect with unrelated concurrent
 * writes, causing Concourse's MVCC to reject the transaction even though the
 * actual data is not in conflict.
 * </p>
 *
 * @author Jeff Nelson
 */
public enum SpuriousSaveFailureStrategy {

    /**
     * Immediately propagate any {@code TransactionException} that occurs during
     * a save, regardless of whether the failure is spurious. This is the legacy
     * behavior.
     */
    FAIL_FAST,

    /**
     * When a {@code TransactionException} occurs during a save, check whether
     * any of the {@link Record Records} involved have stale data. If none do,
     * the failure is spurious and the save is automatically retried in a new
     * transaction. If any {@link Record} has stale data, the failure represents
     * a real conflict and the {@code TransactionException} is thrown.
     */
    RETRY;
}
