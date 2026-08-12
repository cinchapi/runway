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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link Transactional} construct can scope database operations to a single
 * ACID {@link Transaction}.
 * <p>
 * There are two ways to use a transaction. {@link #stage()} (or its alias,
 * {@link #startTransaction()}) returns an open {@link Transaction} whose
 * lifecycle the caller owns. {@link #run(Consumer) run} and
 * {@link #supply(Function) supply} execute work against a
 * {@link TransactionInterface} view and manage the lifecycle on the caller's
 * behalf.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Transactional {

    /**
     * Execute {@code work} within this {@link Transactional Transactional's}
     * transactional scope.
     * <p>
     * This method behaves exactly like {@link #supply(Function)} for work that
     * does not produce a result.
     * </p>
     *
     * @param work the work to run
     */
    public default void run(Consumer<TransactionInterface> work) {
        supply(transaction -> {
            work.accept(transaction);
            return null;
        });
    }

    /**
     * Start a {@link Transaction} that scopes reads and writes to a single ACID
     * transaction.
     * <p>
     * The caller owns the {@link Transaction Transaction's} lifecycle: end it
     * with exactly one of {@link Transaction#commit() commit} or
     * {@link Transaction#abort() abort}, or rely on {@link Transaction#close()
     * close} to abort whatever was not committed. Use a try-with-resources
     * block so the transaction always ends.
     * </p>
     *
     * @return an open {@link Transaction}
     */
    public Transaction stage();

    /**
     * Start a {@link Transaction} that scopes reads and writes to a single ACID
     * transaction.
     * <p>
     * This method is an alias for {@link #stage()}.
     * </p>
     *
     * @return an open {@link Transaction}
     */
    public default Transaction startTransaction() {
        return stage();
    }

    /**
     * Execute {@code work} within this {@link Transactional Transactional's}
     * transactional scope and return its result.
     * <p>
     * If this {@link Transactional} already operates within an open
     * {@link Transaction}, then the work joins it: everything the work stages
     * becomes durable when that transaction's owner commits it. Otherwise, the
     * work receives the {@link TransactionInterface} view of a new
     * {@link Transaction} that commits after the work completes. Either way,
     * the work cannot commit, abort or close the transaction it joins.
     * Conflicts retry within the bounds of the governing
     * {@link AtomicRetryPolicy}, so the work may run more than once and must be
     * free of side effects outside of the transaction.
     * </p>
     *
     * @param work the work to run
     * @return the result of {@code work}
     * @throws RetryExhaustedException if a new transaction cannot commit within
     *             the bounds of the governing {@link AtomicRetryPolicy}
     */
    public <T> T supply(Function<TransactionInterface, T> work);

}
