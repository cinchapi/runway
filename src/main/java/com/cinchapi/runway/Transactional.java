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
 * There are two ways to use a transaction. {@link #startTransaction()} returns
 * an open {@link Transaction} whose lifecycle the caller owns, and whose work
 * runs exactly once. {@link #transact(Consumer) transact} and
 * {@link #transactAndSupply(Function) transactAndSupply} execute work against a
 * {@link TransactionInterface} view, manage the lifecycle on the caller's
 * behalf, and may run the work more than once.
 * </p>
 * <p>
 * The view that scoped work receives passes through
 * {@link #scope(TransactionInterface)}, so operations on it behave the same as
 * operations on this {@link Transactional}.
 * </p>
 * <p>
 * The two forms open the same kind of transaction and enforce the same rules. A
 * view from either one reads what this {@link Transactional} reads, binds the
 * same {@link Record Records}, and commits with the same guarantees, so the
 * choice between them is a choice about lifecycle rather than about semantics.
 * What the managed form adds is the retry cycle: a commit that a conflict
 * defeats discards the transaction and runs the work again against a fresh one,
 * within the bounds of the governing {@link AtomicRetryPolicy}. That is why its
 * work may run more than once and must be free of side effects outside of the
 * transaction, while work in a caller-owned {@link Transaction} runs exactly
 * once and a defeated commit is the caller's to handle.
 * </p>
 * <p>
 * The forms also differ when this {@link Transactional} already operates within
 * an open {@link Transaction}: the managed form joins it, while a caller-owned
 * {@link Transaction} does not nest, so an implementation that is already bound
 * refuses to start another.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface Transactional {

    /**
     * Execute {@code work} within this {@link Transactional Transactional's}
     * transactional scope.
     * <p>
     * This method behaves exactly like {@link #transactAndSupply(Function)} for
     * work that does not produce a result.
     * </p>
     *
     * @param work the work to run
     */
    public default void transact(Consumer<TransactionInterface> work) {
        transactAndSupply(transaction -> {
            work.accept(transaction);
            return null;
        });
    }

    /**
     * Return the {@link TransactionInterface} view of {@code transaction}
     * through which work scoped by this {@link Transactional} operates.
     * <p>
     * Every operation on the returned view behaves the same as the operation on
     * this {@link Transactional}, just within the confines of the transaction.
     * </p>
     *
     * @param transaction the transaction that scopes the work
     * @return the view the work receives
     */
    public default TransactionInterface scope(
            TransactionInterface transaction) {
        return transaction;
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
     * <p>
     * Every operation on the returned view behaves the same as the operation on
     * this {@link Transactional}, just within the confines of the transaction.
     * </p>
     * <p>
     * Work in the returned {@link Transaction} runs exactly once, and a commit
     * that a conflict defeats is the caller's to handle. Use
     * {@link #transactAndSupply(Function) transactAndSupply} to have that
     * conflict retried under the governing {@link AtomicRetryPolicy}.
     * </p>
     *
     * @return an open {@link Transaction}
     */
    public Transaction startTransaction();

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
     * </p>
     * <p>
     * A {@link Record} loaded through the transaction, saved through it, or
     * {@link TransactionInterface#create(Class, Object...) created} by it
     * stages within it, so everything becomes durable together when the commit
     * succeeds. A {@link Record} bound elsewhere saves against its own binding,
     * outside of the transaction.
     * </p>
     * <p>
     * When the work runs in its own transaction and the commit fails because of
     * a conflict, the transaction is discarded and the work runs again against
     * a fresh one, within the bounds of the governing
     * {@link AtomicRetryPolicy}, so the work may run more than once and must be
     * free of side effects outside of the transaction. A {@link Record
     * Record's} in-memory state is outside of it, so an edit from a discarded
     * attempt survives and is visible to the next one. Set each value
     * absolutely, or derive it from a read through the transaction, rather than
     * increment what a prior attempt left behind. Any other exception thrown by
     * {@code work} aborts the transaction and propagates to the caller.
     * </p>
     *
     * @param work the work to run
     * @return the result of {@code work}
     * @throws RetryExhaustedException if a new transaction cannot commit within
     *             the bounds of the governing {@link AtomicRetryPolicy}
     */
    public <T> T transactAndSupply(Function<TransactionInterface, T> work);

}
