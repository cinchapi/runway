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
 * A {@link TransactionInterface} is the {@link DatabaseInterface} view of an
 * ACID transaction that scoped work operates against: reads observe the
 * transaction's isolated snapshot, writes stage within it and side effects that
 * depend on the outcome can be registered, but the transaction's lifecycle
 * stays with its owner.
 * <p>
 * {@link Runway#run(java.util.function.Consumer) run} and
 * {@link Runway#supply(java.util.function.Function) supply}, along with their
 * {@link Record#run(java.util.function.Consumer) Record}
 * {@link Record#supply(java.util.function.Function) counterparts}, hand work
 * this view, so the work cannot commit, abort or close the transaction it
 * joins. {@link Runway#stage()} returns the full {@link Transaction}, which
 * adds the lifecycle verbs for the caller that owns them.
 * </p>
 *
 * @author Jeff Nelson
 */
public interface TransactionInterface extends DatabaseInterface {

    /**
     * Register a {@code hook} to run once, after the transaction ends without a
     * successful commit.
     * <p>
     * Hooks run synchronously, in registration order, after the transaction
     * ends. A hook that throws does not affect the outcome: the exception
     * propagates to the caller and any remaining hooks are skipped.
     * </p>
     *
     * @param hook the side effect to run after the transaction ends without a
     *            successful commit
     * @throws IllegalStateException if the transaction already ended, or if a
     *             save failed within it
     */
    void afterAbort(Runnable hook);

    /**
     * Register a {@code hook} to run once, after the transaction successfully
     * commits.
     * <p>
     * Hooks run synchronously, in registration order, after the commit
     * succeeds. A hook that throws does not affect the outcome: the transaction
     * remains committed, the exception propagates to the caller and any
     * remaining hooks are skipped.
     * </p>
     *
     * @param hook the side effect to run after a successful commit
     * @throws IllegalStateException if the transaction already ended, or if a
     *             save failed within it
     */
    void afterCommit(Runnable hook);

    /**
     * Create a new {@link Record} of the specified {@code clazz} that is bound
     * to the transaction, so a direct {@link Record#save() save} stages within
     * it.
     * <p>
     * The returned {@link Record} is not saved to the database until
     * {@link Record#save()} is called.
     * </p>
     *
     * @param clazz the type of {@link Record} to create
     * @param args constructor arguments for the {@link Record}
     * @param <T> the type of {@link Record}
     * @return the newly created {@link Record}, not yet saved
     * @throws IllegalStateException if a save failed within the open
     *             transaction
     */
    <T extends Record> T create(Class<T> clazz, Object... args);

    /**
     * Save all changes in the provided {@code records} within the transaction.
     *
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     */
    default boolean save(Record... records) {
        return save(false, records);
    }

    /**
     * Save all changes in the provided {@code records} within the transaction.
     * <p>
     * The records, and every {@link Record} linked from them, are bound to the
     * transaction, and the staged changes become durable when the transaction
     * commits. Until then, no reader outside the transaction can observe them.
     * </p>
     *
     * @param preventStaleWrites if {@code true}, reject the save when any
     *            {@link Record} in the object graph has stale data
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     * @throws StaleDataException if {@code preventStaleWrites} is {@code true}
     *             and any {@link Record} has been externally modified
     */
    boolean save(boolean preventStaleWrites, Record... records);

}
