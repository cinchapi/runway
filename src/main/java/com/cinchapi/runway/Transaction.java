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

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.Verify;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.ForwardingConcourse;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.thrift.TransactionToken;
import com.cinchapi.runway.db.BatchReader;
import com.cinchapi.runway.db.BatchSaver;
import com.cinchapi.runway.db.ConcourseProvider;
import com.cinchapi.runway.db.IncrementalReader;
import com.cinchapi.runway.db.IncrementalSaver;
import com.cinchapi.runway.db.Reader;
import com.cinchapi.runway.db.Saver;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * A {@link DatabaseInterface} that scopes every operation to a single ACID
 * transaction.
 * <p>
 * Every read observes the transaction's isolated snapshot, including its own
 * uncommitted writes, and joins its conflict footprint. Results resolve
 * eagerly. Writes become durable when {@link #commit()} succeeds; until then no
 * reader outside the transaction can observe them.
 * </p>
 * <p>
 * Every loaded {@link Record}, including the records reachable from its fields,
 * is bound to this view, so each {@link Record#save() save} stages within the
 * transaction. A {@link Record} {@link #create(Class, Object...) created}
 * through the view is bound to it as well. A {@link Record} that is not bound
 * to the transaction, including one constructed directly, operates against its
 * own binding even while the transaction is open.
 * </p>
 * <p>
 * An {@link com.cinchapi.runway.access.Audience Audience} loaded through the
 * view routes the operations it performs through the transaction, so
 * access-controlled reads and writes stay within the snapshot. A
 * {@link DeferredReference} that is first accessed within the transaction
 * resolves within it as well. Only {@link Record Records} persisted in the
 * database are visible; records supplied by an attached {@link AdHocDataSource}
 * are not.
 * </p>
 * <p>
 * A {@link Transaction} is confined to the thread that starts it and must be
 * ended by exactly one of {@link #commit()} or {@link #abort()};
 * {@link #close()} aborts whatever was not committed, so a try-with-resources
 * block guarantees a clean end. After the transaction ends, it forwards reads
 * and saves to the enclosing {@link Runway}, so a {@link Record} bound to it
 * unwinds to the database scope; only another {@link #commit()} and new
 * {@link #afterCommit(Runnable) afterCommit}/{@link #afterAbort(Runnable)
 * afterAbort} registrations are refused. Side effects that depend on the
 * outcome can be registered with {@link #afterCommit(Runnable)} and
 * {@link #afterAbort(Runnable)}.
 * </p>
 * <p>
 * If a save fails after its arguments are accepted, or if a {@link Record}
 * created by {@link #intern(Record) intern} fails its verification, then the
 * transaction is poisoned: the staged writes can never commit. A poisoned
 * transaction refuses every operation except {@link #abort()} (or
 * {@link #close()}) and {@link #afterAbort(Runnable) afterAbort} registration.
 * A save argument that fails its checks is rejected before anything is staged,
 * and the transaction remains usable.
 * </p>
 * <p>
 * A deletion staged within the transaction is final. A later save of an
 * id-equal {@link Record} adopts the deletion instead of restoring data, a
 * reference to the deleted record that a later save stages is removed the same
 * as one staged alongside the deletion, and the lifecycle consequences of the
 * {@link #commit()} dispatch once per record.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class Transaction extends Binding implements
        AutoCloseable,
        TransactionInterface {

    /**
     * The {@link Runway} instance that this {@link Transaction} operates
     * against and falls through to after the transaction ends.
     */
    private final Runway database;

    /**
     * The connection that hosts the staged transaction and services every read,
     * or {@code null} when this view represents the absence of a transaction.
     */
    private final Concourse concourse;

    /**
     * Whether this view owns {@link #concourse} and must release it back to the
     * connection pool when the transaction ends.
     */
    private final boolean owned;

    /**
     * The only {@link Thread} that may operate on this view.
     */
    private final Thread owner;

    /**
     * Whether the transaction is still active. The field is volatile so a
     * thread that operates on a bound {@link Record} after the transaction ends
     * observes the ended state and falls through to the enclosing
     * {@link Runway}.
     */
    private volatile boolean open;

    /**
     * Whether a failure left staged writes that can never {@link #commit()} and
     * poisoned the transaction. A poisoned transaction refuses every operation
     * except {@link #abort()} (or {@link #close()}) and
     * {@link #afterAbort(Runnable) afterAbort} registration.
     */
    private boolean poisoned = false;

    /**
     * The number of operations that are in flight on this transaction. While an
     * operation is in flight, {@link #commit()} and {@link #abort()} are
     * refused, so a hook that the operation runs cannot end the transaction
     * underneath it.
     */
    private int operating = 0;

    /**
     * Whether an {@link #afterCommit(Runnable) afterCommit} or
     * {@link #afterAbort(Runnable) afterAbort} hook, or the dispatch of a
     * commit's consequences, threw while the transaction was ending. The
     * exception must propagate to the caller instead of being mistaken for a
     * commit conflict.
     */
    private boolean hookFailed = false;

    /**
     * Whether the transaction successfully committed.
     */
    private boolean committed = false;

    /**
     * The server timestamp at which the staged transaction began, or {@code 0}
     * before the first {@link #startTimestamp()} access.
     */
    private long startTimestamp = 0;

    /**
     * The {@link SaveContext} for every save staged within the transaction, in
     * staging order, so the lifecycle consequences can be dispatched at
     * {@link #commit()} or unwound at {@link #abort()}.
     */
    private final List<SaveContext> saves = new ArrayList<>();

    /**
     * The id of every record that a staged save deleted, so the deletion stays
     * final for the saves that follow it within the transaction.
     */
    private final Set<Long> deletions = new LinkedHashSet<>();

    /**
     * The hooks to run, in registration order, after a successful
     * {@link #commit()}.
     */
    private final List<Runnable> afterCommitHooks = new ArrayList<>();

    /**
     * The hooks to run, in registration order, after the transaction ends
     * without a successful commit.
     */
    private final List<Runnable> afterAbortHooks = new ArrayList<>();

    /**
     * The {@link ConcourseProvider} that scopes a bound {@link Record Record's}
     * operations to this transaction.
     */
    private final ConcourseProvider provider = new ConcourseProvider() {

        @Override
        public void release(Concourse connection) {
            if(open) {
                // The Transaction owns the connection until the transaction
                // ends, so the return only closes the operation window that
                // the request opened.
                operating--;
            }
            else {
                database.connections.release(connection);
            }
        }

        @Override
        public Concourse request() {
            if(open) {
                verifyOwner();
                verifyNotPoisoned();
                // A borrowed connection is an operation in flight, so
                // commit() and abort() are refused until the release.
                operating++;
                return concourse;
            }
            else {
                return database.connections.request();
            }
        }

    };

    /**
     * Construct an instance that represents the absence of a transaction: it
     * has no connection and stages nothing, so every operation behaves exactly
     * as it does on {@code database}.
     *
     * @param database the {@link Runway} instance that this transaction
     *            operates against
     */
    Transaction(Runway database) {
        this.database = database;
        this.concourse = null;
        this.owned = false;
        this.owner = Thread.currentThread();
        this.open = false;
    }

    /**
     * Construct a new instance and {@link Concourse#stage() stage} a
     * transaction on {@code concourse}.
     *
     * @param database the {@link Runway} instance that this transaction
     *            operates against
     * @param concourse the {@link Concourse} connection that services every
     *            operation; must not already be in a transaction
     * @param owned {@code true} if this view must release {@code concourse}
     *            back to the pool when the transaction ends
     */
    Transaction(Runway database, Concourse concourse, boolean owned) {
        this.database = database;
        this.concourse = concourse;
        this.owned = owned;
        this.owner = Thread.currentThread();
        this.open = true;
        concourse.stage();
    }

    /**
     * Abort the transaction and discard every staged write; a {@link Record
     * Record's} in-memory edits remain, the same as after a failed save. Every
     * {@link #afterAbort(Runnable) afterAbort} hook runs.
     * <p>
     * This method has no effect if the transaction already ended.
     * </p>
     *
     * @throws IllegalStateException if one of the transaction's own operations
     *             is in flight
     */
    public void abort() {
        if(open) {
            verifyOwner();
            verifyNotOperating();
            try {
                concourse.abort();
            }
            finally {
                end(false);
            }
        }
    }

    /**
     * Register a {@code hook} to run once, after this {@link Transaction} ends
     * without a successful commit: an explicit {@link #abort()}, a
     * {@link #close()} that aborts, or a {@link #commit()} that fails.
     * <p>
     * Use this for side effects that must happen only when the staged writes
     * are discarded (e.g., compensating cleanup or telemetry). Hooks run
     * synchronously on the owner thread, in registration order, after the
     * transaction ends. If the transaction commits, then the hooks never run;
     * within {@link Runway#run(java.util.function.Consumer) run} and
     * {@link Runway#supply(java.util.function.Function) supply}, each attempt
     * is a distinct {@link Transaction}, so a hook registered by the work runs
     * for its own attempt, including an attempt that a conflict retry discards.
     * A poisoned transaction still accepts registration, so cleanup can be
     * scheduled before the required {@link #abort()}.
     * </p>
     * <p>
     * A hook that throws does not affect the outcome: the exception propagates
     * to the caller and any remaining hooks are skipped.
     * </p>
     *
     * @param hook the side effect to run after the transaction ends without a
     *            successful commit
     * @throws IllegalStateException if the transaction already ended
     */
    @Override
    public void afterAbort(Runnable hook) {
        verifyOpen();
        afterAbortHooks.add(hook);
    }

    /**
     * Register a {@code hook} to run once, after this {@link Transaction}
     * successfully commits.
     * <p>
     * Use this for side effects that must not happen unless the staged writes
     * become durable (e.g., a notification about a created {@link Record}).
     * Hooks run synchronously on the owner thread, in registration order, after
     * the commit succeeds. If the transaction ends without a successful commit,
     * then the hooks registered on it never run; within
     * {@link Runway#run(java.util.function.Consumer) run} and
     * {@link Runway#supply(java.util.function.Function) supply}, each attempt
     * is a distinct {@link Transaction}, so a hook registered by the work never
     * runs for an attempt that a conflict retry discards.
     * </p>
     * <p>
     * A hook that throws does not affect the outcome: the transaction remains
     * committed, the exception propagates to the caller and any remaining hooks
     * are skipped. A failure while the commit's consequences dispatch skips the
     * hooks the same way: the transaction remains committed and the exception
     * propagates.
     * </p>
     *
     * @param hook the side effect to run after a successful commit
     * @throws IllegalStateException if the transaction already ended, or if a
     *             save failed within it
     */
    @Override
    public void afterCommit(Runnable hook) {
        verify();
        afterCommitHooks.add(hook);
    }

    @Override
    public void close() {
        abort();
    }

    /**
     * Attempt to commit the transaction and make every staged write durable.
     * <p>
     * On success, the lifecycle consequences of each staged save (e.g., save
     * and delete notifications) are dispatched and every
     * {@link #afterCommit(Runnable) afterCommit} hook runs. A failure while the
     * consequences dispatch propagates the same as a hook failure: the commit
     * itself stands and {@link #committed()} reports it. On failure, every
     * staged write is discarded and every {@link #afterAbort(Runnable)
     * afterAbort} hook runs; a {@link Record Record's} in-memory edits remain,
     * the same as after a failed save. Either way, the transaction ends.
     * </p>
     *
     * @return {@code true} if the transaction commits
     * @throws IllegalStateException if the transaction already ended, if a save
     *             failed within it, or if one of the transaction's own
     *             operations is in flight
     */
    public boolean commit() {
        verify();
        verifyNotOperating();
        try {
            committed = concourse.commit();
        }
        finally {
            end(committed);
        }
        return committed;
    }

    /**
     * Create a new {@link Record} of the specified {@code clazz} that is bound
     * to this {@link Transaction}, so a direct {@link Record#save() save}
     * stages within it.
     * <p>
     * The returned {@link Record} is not saved to the database until
     * {@link Record#save()} is called. After the transaction ends, the
     * {@link Record} operates against the enclosing {@link Runway}.
     * </p>
     *
     * @param clazz the type of {@link Record} to create
     * @param args constructor arguments for the {@link Record}
     * @param <T> the type of {@link Record}
     * @return the newly created {@link Record}, not yet saved
     * @throws IllegalStateException if a save failed within the open
     *             transaction
     */
    @Override
    public <T extends Record> T create(Class<T> clazz, Object... args) {
        if(open) {
            verifyOwner();
            verifyNotPoisoned();
        }
        T record = Reflection.newInstance(clazz, args);
        record.bind(this, provider);
        return record;
    }

    /**
     * Atomically find the first {@link Record} in the hierarchy of
     * {@code clazz} that matches the {@code criteria} under the supplied
     * {@code order} and update the value of {@code key} by applying the
     * {@code update} operator.
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     *
     * @param clazz the {@link Record} type whose hierarchy is searched
     * @param criteria the {@link Criteria} the record must match
     * @param order the {@link Order} that defines "first"
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws IllegalArgumentException if {@code order} is {@code null}, if
     *             {@code key} is not eligible for atomic operations, or if
     *             {@code update} returns {@code null} or a value that is not an
     *             instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction
     */
    @Nullable
    @Override
    public <T extends Record, V> T findAnyFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findAnyFirstAndUpdate requires an Order");
        if(open) {
            return execute(
                    () -> TransactionInterface.super.findAnyFirstAndUpdate(
                            clazz, criteria, order, key, update));
        }
        else {
            return database.findAnyFirstAndUpdate(clazz, criteria, order, key,
                    update);
        }
    }

    /**
     * Atomically find the one {@link Record} in the hierarchy of {@code clazz}
     * that matches the {@code criteria} and update the value of {@code key} by
     * applying the {@code update} operator.
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     *
     * @param clazz the {@link Record} type whose hierarchy is searched
     * @param criteria the {@link Criteria} the record must match
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws DuplicateEntryException if more than one record in the hierarchy
     *             matches
     * @throws IllegalArgumentException if {@code key} is not eligible for
     *             atomic operations, or if {@code update} returns {@code null}
     *             or a value that is not an instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction
     */
    @Nullable
    @Override
    public <T extends Record, V> T findAnyUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        if(open) {
            return execute(
                    () -> TransactionInterface.super.findAnyUniqueAndUpdate(
                            clazz, criteria, key, update));
        }
        else {
            return database.findAnyUniqueAndUpdate(clazz, criteria, key,
                    update);
        }
    }

    /**
     * Atomically find the first {@link Record} of type {@code clazz} that
     * matches the {@code criteria} under the supplied {@code order} and update
     * the value of {@code key} by applying the {@code update} operator.
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     *
     * @param clazz the {@link Record} type to find
     * @param criteria the {@link Criteria} the record must match
     * @param order the {@link Order} that defines "first"
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws IllegalArgumentException if {@code order} is {@code null}, if
     *             {@code key} is not eligible for atomic operations, or if
     *             {@code update} returns {@code null} or a value that is not an
     *             instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction
     */
    @Nullable
    @Override
    public <T extends Record, V> T findFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findFirstAndUpdate requires an Order");
        if(open) {
            return execute(() -> TransactionInterface.super.findFirstAndUpdate(
                    clazz, criteria, order, key, update));
        }
        else {
            return database.findFirstAndUpdate(clazz, criteria, order, key,
                    update);
        }
    }

    /**
     * Atomically find the one {@link Record} of type {@code clazz} that matches
     * the {@code criteria} and update the value of {@code key} by applying the
     * {@code update} operator.
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     *
     * @param clazz the {@link Record} type to find
     * @param criteria the {@link Criteria} the record must match
     * @param key the name of the intrinsic field to update
     * @param update the operator that produces the replacement value from the
     *            current one; it must not return {@code null}
     * @param <T> the type of {@link Record}
     * @param <V> the type of the value stored under {@code key}
     * @return the updated {@link Record}, or {@code null} if none matches
     * @throws DuplicateEntryException if more than one record matches
     * @throws IllegalArgumentException if {@code key} is not eligible for
     *             atomic operations, or if {@code update} returns {@code null}
     *             or a value that is not an instance of the field's type
     * @throws IllegalStateException if a save failed within the open
     *             transaction
     */
    @Nullable
    @Override
    public <T extends Record, V> T findUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        if(open) {
            return execute(() -> TransactionInterface.super.findUniqueAndUpdate(
                    clazz, criteria, key, update));
        }
        else {
            return database.findUniqueAndUpdate(clazz, criteria, key, update);
        }
    }

    /**
     * Return the unique {@link Record} that agrees with every {@link Unique}
     * constraint of {@code record}, or save {@code record} when none exists.
     * <p>
     * While the transaction is open, the lookup and the save stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
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
    @Override
    public <T extends Record> T intern(T record) {
        if(open) {
            @SuppressWarnings("unchecked") Class<T> clazz = (Class<T>) record
                    .getClass();
            Criteria criteria = record.uniqueCriteria();
            return findOrCreate(() -> findUnique(clazz, criteria),
                    () -> record);
        }
        else {
            return database.intern(record);
        }
    }

    /**
     * Save all changes in the provided {@code records} within this transaction.
     * <p>
     * The records, and every {@link Record} linked from them, are bound to this
     * transaction, and the staged changes become durable when {@link #commit()}
     * succeeds. Until then, no reader outside the transaction can observe them.
     * </p>
     *
     * <p>
     * A {@code records} argument that fails its checks (one that is
     * {@code null}, overrides the save pipeline, throws from its
     * {@code overrideSave} accessor, or is bound to a different open
     * {@link Transaction}) is rejected before anything is staged, and the
     * transaction remains usable. Any failure after the arguments are accepted,
     * including a linked {@link Record} that is bound to a different open
     * {@link Transaction}, poisons the transaction: the writes that were staged
     * before the failure can never commit, and every subsequent operation is
     * refused except {@link #abort()} (or {@link #close()}) and
     * {@link #afterAbort(Runnable) afterAbort} registration.
     * </p>
     *
     * @param preventStaleWrites if {@code true}, reject the save when any
     *            {@link Record} in the object graph has stale data
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     * @throws StaleDataException if {@code preventStaleWrites} is {@code true}
     *             and any {@link Record} has been externally modified
     * @throws IllegalStateException if any of the {@code records} overrides the
     *             save pipeline, if any {@link Record} that the save processes
     *             is bound to a different open {@link Transaction}, or if a
     *             prior save failed within the transaction
     */
    @Override
    public boolean save(boolean preventStaleWrites, Record... records) {
        if(open) {
            verifyOwner();
            verifyNotPoisoned();
            for (Record record : records) {
                Verify.that(record.overrideSave() == null,
                        "Cannot save a Record that overrides the save"
                                + " pipeline within a Transaction");
                record.verifySavableThrough(this);
            }
            Saver saver = database.supportsBulkCommands
                    ? new BatchSaver(concourse)
                    : new IncrementalSaver(concourse);
            SaveContext context = new SaveContext(preventStaleWrites, deletions,
                    record -> {
                        record.verifySavableThrough(this);
                        record.bind(this, provider);
                    });
            operating++;
            try {
                // NOTE: The saver is never staged or committed here because
                // the connection is already within this transaction, whose
                // commit is the terminal operation. The flush is what sends a
                // bulk saver's queued writes and runs its validators, so the
                // staged state is on the server, and validated, before any
                // later operation through this transaction reads it.
                Set<Record> seen = Sets.newIdentityHashSet();
                for (Record record : records) {
                    record.bindGraph(this, provider, seen);
                    record.saveWithinTransaction(saver, context);
                }
                database.stageDeletions(saver, context);
                saver.flush();
            }
            catch (Throwable t) {
                // The writes that were staged before the failure cannot be
                // selectively undone, so the transaction must never commit.
                poisoned = true;
                context.restore();
                throw t;
            }
            finally {
                operating--;
            }
            saves.add(context);
            deletions.addAll(context.deletions());
            return true;
        }
        else {
            return database.save(preventStaleWrites, records);
        }
    }

    /**
     * Save all changes in the provided {@code records} within this transaction.
     * <p>
     * The records, and every {@link Record} linked from them, are bound to this
     * transaction, and the staged changes become durable when {@link #commit()}
     * succeeds. Until then, no reader outside the transaction can observe them.
     * </p>
     * <p>
     * A {@code records} argument that fails its checks (one that is
     * {@code null}, overrides the save pipeline, throws from its
     * {@code overrideSave} accessor, or is bound to a different open
     * {@link Transaction}) is rejected before anything is staged, and the
     * transaction remains usable. Any failure after the arguments are accepted,
     * including a linked {@link Record} that is bound to a different open
     * {@link Transaction}, poisons the transaction: the writes that were staged
     * before the failure can never commit, and every subsequent operation is
     * refused except {@link #abort()} (or {@link #close()}) and
     * {@link #afterAbort(Runnable) afterAbort} registration.
     * </p>
     *
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     * @throws IllegalStateException if any of the {@code records} overrides the
     *             save pipeline, if any {@link Record} that the save processes
     *             is bound to a different open {@link Transaction}, or if a
     *             prior save failed within the transaction
     */
    @Override
    public boolean save(Record... records) {
        return save(false, records);
    }

    @Override
    public Selections select(Selection<?>... options) {
        if(open) {
            return execute(() -> {
                DatabaseSelection<?>[] selections = DatabaseSelection
                        .resolve(options);
                try (Reader reader = database.supportsBulkCommands
                        ? new BatchReader(concourse)
                        : new IncrementalReader(concourse)) {
                    for (DatabaseSelection<?> selection : selections) {
                        if(selection.state == Selection.State.RESOLVED) {
                            selection.setState(Selection.State.FINISHED);
                        }
                        else {
                            selection.setState(Selection.State.SUBMITTED);
                            database.$selectFromDatabase(reader, selection,
                                    this);
                        }
                    }
                    reader.drain();
                }
                for (DatabaseSelection<?> selection : selections) {
                    materialize(selection.get());
                }
                return new Selections(selections);
            });
        }
        else {
            return database.select(options);
        }
    }

    /**
     * Return {@code true} if the transaction successfully committed.
     *
     * @return {@code true} if the transaction {@link #committed}
     */
    boolean committed() {
        return committed;
    }

    /**
     * Return the {@link Runway} instance that this {@link Transaction} operates
     * against.
     *
     * @return the enclosing {@link Runway}
     */
    Runway database() {
        return database;
    }

    /**
     * Return {@code true} if an {@link #afterCommit(Runnable) afterCommit} or
     * {@link #afterAbort(Runnable) afterAbort} hook threw while the transaction
     * was ending.
     *
     * @return {@code true} if a hook failed
     */
    boolean hookFailed() {
        return hookFailed;
    }

    /**
     * Bind {@code record}, and every loaded {@link Record} that is reachable
     * from its persistent (non-transient) fields, to this {@link Transaction},
     * so each {@link Record#save() save} stages within it.
     *
     * @param record the {@link Record} that joins this {@link Transaction}
     */
    void join(Record record) {
        record.bindGraph(this, provider, Sets.newIdentityHashSet());
    }

    @Override
    <T extends Record> T load(long id) {
        if(open) {
            return execute(() -> {
                Set<Object> sections = concourse.select(Record.SECTION_KEY, id);
                Class<T> clazz = Reflection
                        .getClassCasted((String) Iterables.getLast(sections));
                return load(clazz, id);
            });
        }
        else {
            return database.load(id);
        }
    }

    /**
     * Return {@code true} if the transaction is still active.
     *
     * @return {@code true} if the transaction is {@link #open}
     */
    boolean open() {
        return open;
    }

    /**
     * Run {@code operation} within this transaction's operation window, so the
     * transaction cannot end while the operation is in flight.
     *
     * @param operation the work to run
     * @param <T> the operation's result type
     * @return the operation's result
     */
    <T> T execute(Supplier<T> operation) {
        verifyOwner();
        verifyNotPoisoned();
        operating++;
        try {
            return operation.get();
        }
        finally {
            operating--;
        }
    }

    /**
     * Return the {@link ConcourseProvider} that scopes a bound {@link Record
     * Record's} operations to this transaction.
     *
     * @return the transaction's {@link ConcourseProvider}
     */
    ConcourseProvider provider() {
        return provider;
    }

    /**
     * Return the server timestamp at which the staged transaction began. Every
     * revision that a read within the transaction observes with a newer
     * timestamp is one of this {@link Transaction Transaction's} own staged
     * writes, because no other writer is visible in the snapshot.
     *
     * @return the server timestamp of the transaction's start
     */
    long startTimestamp() {
        if(startTimestamp == 0) {
            Concourse source = concourse;
            while (source instanceof ForwardingConcourse) {
                source = Reflection.get("concourse", source);
            }
            TransactionToken token = Reflection.get("transaction", source);
            startTimestamp = token.getTimestamp();
        }
        return startTimestamp;
    }

    /**
     * Return a synchronous {@link Reader} whose reads execute within this
     * {@link Transaction}.
     *
     * @return the {@link Reader}
     */
    Reader syncReader() {
        return new IncrementalReader(concourse);
    }

    /**
     * Verify that the transaction is still {@link #open}, is not
     * {@link #poisoned} and that the caller is the {@link #owner} thread.
     */
    void verify() {
        verifyOpen();
        verifyNotPoisoned();
    }

    /**
     * Execute {@link Concourse#verifyOrSet(String, Object, long) verifyOrSet}
     * within the transaction.
     *
     * @param key the field name
     * @param value the value to store as the only value for {@code key} in
     *            {@code record}
     * @param record the record id
     */
    void verifyOrSet(String key, Object value, long record) {
        concourse.verifyOrSet(key, value, record);
    }

    /**
     * End the transaction: dispatch or unwind the lifecycle consequences of
     * every staged save, run the hooks registered for the outcome and, if this
     * view {@link #owned owns} the connection, release it back to the pool.
     *
     * @param committed {@code true} if the transaction committed
     */
    private void end(boolean committed) {
        open = false;
        try {
            if(committed) {
                // The commit is one durable event, so the lifecycle
                // consequences dispatch once per record: the contexts merge
                // under the monotonic Outcome ladder and the latest instance
                // with the highest outcome receives the consequences.
                SaveContext merged = new SaveContext(false);
                for (SaveContext context : saves) {
                    context.forEach(merged::merge);
                }
                try {
                    database.dispatchSaveOutcomes(merged);
                    for (Runnable hook : afterCommitHooks) {
                        hook.run();
                    }
                }
                catch (Throwable t) {
                    hookFailed = true;
                    throw t;
                }
            }
            else {
                // NOTE: A nested save completes before its parent, so list
                // order cannot identify a record's oldest snapshot; the
                // capture sequence can. The oldest snapshot is the record's
                // true pre-save state.
                Map<Record, Record.Snapshot> oldest = new IdentityHashMap<>();
                for (SaveContext context : saves) {
                    context.forEachSnapshot((record, snapshot) -> oldest.merge(
                            record, snapshot,
                            (a, b) -> a.sequence <= b.sequence ? a : b));
                }
                oldest.forEach((record, snapshot) -> record.restore(snapshot));
                try {
                    for (Runnable hook : afterAbortHooks) {
                        hook.run();
                    }
                }
                catch (Throwable t) {
                    hookFailed = true;
                    throw t;
                }
            }
        }
        finally {
            // A Record stays bound to this Transaction after it ends, so drop
            // the staged contexts and hooks; otherwise one retained Record
            // would pin every record and closure the transaction touched.
            saves.clear();
            deletions.clear();
            afterCommitHooks.clear();
            afterAbortHooks.clear();
            // A hook that throws must not prevent the connection from being
            // released.
            if(owned) {
                database.connections.release(concourse);
            }
        }
    }

    /**
     * Return the unique {@link Record} that the {@code lookup} matches, or
     * create and save one from {@code factory} when none exists.
     * <p>
     * If the {@code factory} tries to end the transaction, then the call is
     * refused. If verification fails after the save, then the transaction is
     * poisoned and the staged save can never commit.
     * </p>
     *
     * @param lookup performs the criteria lookup within the transaction
     * @param factory supplies the {@link Record} to create when none match
     * @param <T> the type of {@link Record}
     * @return the matched or created {@link Record}
     */
    private <T extends Record> T findOrCreate(Supplier<T> lookup,
            Supplier<T> factory) {
        return execute(() -> {
            T record = lookup.get();
            if(record == null) {
                record = factory.get();
                Verify.thatArgument(record != null,
                        "The factory cannot return null");
                save(record);
                try {
                    T found = lookup.get();
                    Verify.thatArgument(
                            found != null && record.id() == found.id(),
                            "The created Record does not match the criteria");
                }
                catch (Throwable t) {
                    poisoned = true;
                    throw t;
                }
            }
            return record;
        });
    }

    /**
     * Ensure that {@code result}, including every element of an
     * {@link Iterable} result, is fully materialized while the transaction is
     * open, so no part of a {@link Selection} result resolves after the
     * transaction ends.
     *
     * @param result a resolved {@link Selection} result
     */
    private void materialize(Object result) {
        if(result instanceof Iterable) {
            for (Object item : (Iterable<?>) result) {
                materialize(item);
            }
        }
        else {
            // A non-iterable result (a single Record, a count, or null) is
            // already materialized.
        }
    }

    /**
     * Verify that no operation is in flight on this transaction.
     */
    private void verifyNotOperating() {
        Verify.that(operating == 0,
                "Cannot end the Transaction while an operation is in flight");
    }

    /**
     * Verify that the transaction is not {@link #poisoned}.
     */
    private void verifyNotPoisoned() {
        Verify.that(!poisoned,
                "The Transaction cannot continue because a failure left"
                        + " staged writes that can never commit; abort and"
                        + " retry the work in a new Transaction");
    }

    /**
     * Verify that the transaction is still {@link #open} and that the caller is
     * the {@link #owner} thread.
     */
    private void verifyOpen() {
        verifyOwner();
        Verify.that(open, "The Transaction has ended");
    }

    /**
     * Verify that the caller is the {@link #owner} thread.
     */
    private void verifyOwner() {
        Verify.that(Thread.currentThread() == owner,
                "A Transaction is confined to the thread that started it");
    }

}
