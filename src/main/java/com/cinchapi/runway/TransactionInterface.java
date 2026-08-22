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

import java.util.function.UnaryOperator;

import javax.annotation.Nullable;

import com.cinchapi.common.base.Verify;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;

/**
 * A {@link TransactionInterface} is the {@link DatabaseInterface} view of an
 * ACID transaction that scoped work operates against: reads observe the
 * transaction's isolated snapshot, writes stage within it and side effects that
 * depend on the outcome can be registered, but the transaction's lifecycle
 * stays with its owner.
 * <p>
 * {@link Runway#transact(java.util.function.Consumer) transact} and
 * {@link Runway#transactAndSupply(java.util.function.Function)
 * transactAndSupply}, along with their
 * {@link Record#transact(java.util.function.Consumer) Record}
 * {@link Record#transactAndSupply(java.util.function.Function) counterparts},
 * hand work this view, so the work cannot commit, abort or close the
 * transaction it joins. {@link Runway#startTransaction()} returns the full
 * {@link Transaction}, which adds the lifecycle verbs for the caller that owns
 * them.
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
     * propagates to the caller and any remaining hooks are skipped. A failed
     * save within the transaction does not refuse registration.
     * </p>
     *
     * @param hook the side effect to run after the transaction ends without a
     *            successful commit
     * @throws IllegalStateException if the transaction already ended
     */
    void afterAbort(Runnable hook);

    /**
     * Register a {@code hook} to run once, after the transaction successfully
     * commits.
     * <p>
     * Hooks run synchronously, in registration order, after the commit
     * succeeds. A hook that throws does not affect the outcome: the transaction
     * remains committed, the exception propagates to the caller and any
     * remaining hooks are skipped. A failure while the commit's consequences
     * dispatch skips the hooks the same way.
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
     * Atomically find the first {@link Record} in the hierarchy of
     * {@code clazz} that matches the {@code criteria} under the supplied
     * {@code order} and update the value of {@code key} by applying the
     * {@code update} operator; the find and the write stage within the
     * transaction, so they commit or abort with it.
     * <p>
     * Return the updated {@link Record}, or {@code null} when nothing matches,
     * in which case the {@code update} operator never runs and nothing is
     * staged. The write stages as a save of the match, so save-time validation
     * applies to the whole record and a failed save poisons the transaction.
     * The field eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}.
     * </p>
     *
     * @param clazz the {@link Record} type whose hierarchy is searched
     * @param criteria the {@link Criteria} the record must match
     * @param order the {@link Order} that defines "first"
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one, which is {@code null} when the field has no
     *            value; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws IllegalArgumentException if {@code order} is {@code null}, if
     *             {@code key} is not eligible for atomic operations, or if
     *             {@code update} returns {@code null} or a value that is not an
     *             instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction, or if the value produced by {@code update}
     *             violates the field's constraints
     * @throws NonWritableFieldException if the governing
     *             {@link DynamicWritePolicy} does not permit writing to the
     *             field named by {@code key}
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findAnyFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findAnyFirstAndUpdate requires an Order");
        return Record.stageAtomicUpdate(this,
                findAnyFirst(clazz, criteria, order), key, update);
    }

    /**
     * Atomically find the one {@link Record} in the hierarchy of {@code clazz}
     * that matches the {@code criteria} and update the value of {@code key} by
     * applying the {@code update} operator; the find and the write stage within
     * the transaction, so they commit or abort with it.
     * <p>
     * Return the updated {@link Record}, or {@code null} when nothing matches,
     * in which case the {@code update} operator never runs and nothing is
     * staged. The write stages as a save of the match, so save-time validation
     * applies to the whole record and a failed save poisons the transaction.
     * Throw {@link DuplicateEntryException} when more than one record in the
     * hierarchy matches, consistent with
     * {@link DatabaseInterface#findAnyUnique(Class, Criteria) findAnyUnique}.
     * The field eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}.
     * </p>
     *
     * @param clazz the {@link Record} type whose hierarchy is searched
     * @param criteria the {@link Criteria} the record must match
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one, which is {@code null} when the field has no
     *            value; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws DuplicateEntryException if more than one record in the hierarchy
     *             matches
     * @throws IllegalArgumentException if {@code key} is not eligible for
     *             atomic operations, or if {@code update} returns {@code null}
     *             or a value that is not an instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction, or if the value produced by {@code update}
     *             violates the field's constraints
     * @throws NonWritableFieldException if the governing
     *             {@link DynamicWritePolicy} does not permit writing to the
     *             field named by {@code key}
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findAnyUniqueAndUpdate(
            Class<T> clazz, Criteria criteria, String key,
            UnaryOperator<V> update) {
        return Record.stageAtomicUpdate(this, findAnyUnique(clazz, criteria),
                key, update);
    }

    /**
     * Atomically find the first {@link Record} of type {@code clazz} that
     * matches the {@code criteria} under the supplied {@code order} and update
     * the value of {@code key} by applying the {@code update} operator; the
     * find and the write stage within the transaction, so they commit or abort
     * with it.
     * <p>
     * Return the updated {@link Record}, or {@code null} when nothing matches,
     * in which case the {@code update} operator never runs and nothing is
     * staged. The write stages as a save of the match, so save-time validation
     * applies to the whole record and a failed save poisons the transaction.
     * The field eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}.
     * </p>
     *
     * @param clazz the {@link Record} type to find
     * @param criteria the {@link Criteria} the record must match
     * @param order the {@link Order} that defines "first"
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one, which is {@code null} when the field has no
     *            value; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws IllegalArgumentException if {@code order} is {@code null}, if
     *             {@code key} is not eligible for atomic operations, or if
     *             {@code update} returns {@code null} or a value that is not an
     *             instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction, or if the value produced by {@code update}
     *             violates the field's constraints
     * @throws NonWritableFieldException if the governing
     *             {@link DynamicWritePolicy} does not permit writing to the
     *             field named by {@code key}
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findFirstAndUpdate requires an Order");
        return Record.stageAtomicUpdate(this, findFirst(clazz, criteria, order),
                key, update);
    }

    /**
     * Atomically find the one {@link Record} of type {@code clazz} that matches
     * the {@code criteria} and update the value of {@code key} by applying the
     * {@code update} operator; the find and the write stage within the
     * transaction, so they commit or abort with it.
     * <p>
     * Return the updated {@link Record}, or {@code null} when nothing matches,
     * in which case the {@code update} operator never runs and nothing is
     * staged. The write stages as a save of the match, so save-time validation
     * applies to the whole record and a failed save poisons the transaction.
     * Throw {@link DuplicateEntryException} when more than one record matches,
     * consistent with {@link DatabaseInterface#findUnique(Class, Criteria)
     * findUnique}. The field eligibility rules and value constraints of
     * {@link Record#getAndUpdate(String, UnaryOperator) getAndUpdate} apply to
     * {@code key} and {@code update}.
     * </p>
     *
     * @param clazz the {@link Record} type to find
     * @param criteria the {@link Criteria} the record must match
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one, which is {@code null} when the field has no
     *            value; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws DuplicateEntryException if more than one record matches
     * @throws IllegalArgumentException if {@code key} is not eligible for
     *             atomic operations, or if {@code update} returns {@code null}
     *             or a value that is not an instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction, or if the value produced by {@code update}
     *             violates the field's constraints
     * @throws NonWritableFieldException if the governing
     *             {@link DynamicWritePolicy} does not permit writing to the
     *             field named by {@code key}
     */
    @Nullable
    @Override
    public default <T extends Record, V> T findUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        return Record.stageAtomicUpdate(this, findUnique(clazz, criteria), key,
                update);
    }

    /**
     * Return the unique {@link Record} that agrees with every {@link Unique}
     * constraint of {@code record}, or save {@code record} when none exists.
     * <p>
     * A {@link Record Record's} identity is the current data under its
     * {@link Unique} constraints, each scoped by its declaration: a
     * {@link Unique#any() hierarchy-scoped} constraint applies across the class
     * that declares it and every descendant, and a class-scoped constraint
     * applies among records of the record's concrete class. Another record
     * shares the identity only if it agrees with every constraint and has the
     * same concrete class; a {@code null} value does not participate. If no
     * record shares the identity, then {@code record} itself is saved and
     * returned. If an existing record shares some but not all of the identity,
     * or claims it from another class, then there is no match, and the save of
     * {@code record} fails {@link Unique} enforcement.
     * </p>
     * <p>
     * The lookup and the save stage within the transaction, so at commit
     * exactly one record claims the identity. If the staged save of
     * {@code record} fails (for example, a partial identity collision fails
     * {@link Unique} enforcement), then the transaction is poisoned: the staged
     * writes can never commit, and the view refuses every later operation.
     * </p>
     *
     * @param record the {@link Record} whose identity is interned
     * @param <T> the type of {@link Record}
     * @return the {@link Record} that claims the identity: the sole existing
     *         match, or {@code record} once saved
     * @throws DuplicateEntryException if more than one record shares the
     *             identity
     * @throws IllegalArgumentException if no field under a {@link Unique}
     *             constraint of {@code record} has a non-null value
     * @throws IllegalStateException if a save failed within the open
     *             transaction
     */
    <T extends Record> T intern(T record);

    /**
     * Save all changes in the provided {@code records} within the transaction.
     * <p>
     * The records, and every {@link Record} linked from them, are bound to the
     * transaction, and the staged changes become durable when the transaction
     * commits. Until then, no reader outside the transaction can observe them.
     * </p>
     *
     * @param preventStaleWrites if {@code true}, reject the save if it would
     *            overwrite a value that another writer changed
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     * @throws DeletedRecordException if a {@link Record} that the save writes
     *             holds no data in the database, so the save would restore a
     *             record that another writer erased
     * @throws StaleDataException if {@code preventStaleWrites} is {@code true}
     *             and the save would overwrite a value that another writer
     *             changed, or if another writer changed a value that
     *             {@link Record#verifyOnSave(String...) verifyOnSave} declared
     * @throws IllegalStateException if any of the {@code records} overrides the
     *             save pipeline, if any {@link Record} that the save processes
     *             is bound to a different open transaction, or if a prior save
     *             failed within the transaction
     */
    boolean save(boolean preventStaleWrites, Record... records);

    /**
     * Save all changes in the provided {@code records} within the transaction.
     *
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     * @throws IllegalStateException if any of the {@code records} overrides the
     *             save pipeline, if any {@link Record} that the save processes
     *             is bound to a different open transaction, or if a prior save
     *             failed within the transaction
     */
    default boolean save(Record... records) {
        return save(false, records);
    }

}
