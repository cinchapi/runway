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
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.Verify;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.runway.db.BatchReader;
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
 * unwinds to the database scope; only another {@link #commit()} is refused.
 * Side effects that depend on the outcome can be registered with
 * {@link #afterCommit(Runnable)} and {@link #afterAbort(Runnable)}.
 * </p>
 * <p>
 * A save that fails within an open transaction poisons it: the writes that were
 * staged before the failure can never commit, so every subsequent operation is
 * refused except {@link #abort()} (or {@link #close()}).
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public class Transaction extends Binding implements AutoCloseable {

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
     * Whether a failed save poisoned the transaction. A poisoned transaction
     * refuses every operation except {@link #abort()}, so the writes that were
     * staged before the failure can never {@link #commit()}.
     */
    private boolean poisoned = false;

    /**
     * Whether the transaction successfully committed.
     */
    private boolean committed = false;

    /**
     * The {@link SaveContext} for every save staged within the transaction, in
     * staging order, so the lifecycle consequences can be dispatched at
     * {@link #commit()} or unwound at {@link #abort()}.
     */
    private final List<SaveContext> saves = new ArrayList<>();

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
        public Concourse request() {
            if(open) {
                verifyOwner();
                return concourse;
            }
            else {
                return database.connections.request();
            }
        }

        @Override
        public void release(Concourse connection) {
            if(!open) {
                database.connections.release(connection);
            }
            else {
                // no-op: the Transaction owns the connection until the
                // transaction ends
            }
        }

    };

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

    @Override
    public Selections select(Selection<?>... options) {
        if(open) {
            verifyOwner();
            verifyNotPoisoned();
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
                        database.$selectFromDatabase(reader, selection, this);
                    }
                }
                reader.drain();
            }
            Set<Record> seen = Sets.newIdentityHashSet();
            for (DatabaseSelection<?> selection : selections) {
                bind(selection.get(), seen);
            }
            return new Selections(selections);
        }
        else {
            return database.select(options);
        }
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
     */
    public <T extends Record> T create(Class<T> clazz, Object... args) {
        if(open) {
            verifyOwner();
            verifyNotPoisoned();
        }
        T record = Reflection.newInstance(clazz, args);
        record.bind(this, provider);
        return record;
    }

    @Override
    <T extends Record> T load(long id) {
        if(open) {
            verifyOwner();
            verifyNotPoisoned();
            Set<Object> sections = concourse.select(Record.SECTION_KEY, id);
            Class<T> clazz = Reflection
                    .getClassCasted((String) Iterables.getLast(sections));
            return load(clazz, id);
        }
        else {
            return database.load(id);
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
     * A {@link Record} that overrides the save pipeline is rejected before
     * anything is staged, and the transaction remains usable. If the save fails
     * after staging begins, then the transaction is poisoned: the writes that
     * were staged before the failure can never commit, and every subsequent
     * operation is refused except {@link #abort()}.
     * </p>
     *
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     * @throws IllegalStateException if any of the {@code records} overrides the
     *             save pipeline, or if a prior save failed within the
     *             transaction
     */
    @Override
    public boolean save(Record... records) {
        return save(false, records);
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
     * A {@link Record} that overrides the save pipeline is rejected before
     * anything is staged, and the transaction remains usable. If the save fails
     * after staging begins, then the transaction is poisoned: the writes that
     * were staged before the failure can never commit, and every subsequent
     * operation is refused except {@link #abort()}.
     * </p>
     *
     * @param preventStaleWrites if {@code true}, reject the save when any
     *            {@link Record} in the object graph has stale data
     * @param records one or more {@link Record Records} to save
     * @return {@code true} when the changes are staged
     * @throws StaleDataException if {@code preventStaleWrites} is {@code true}
     *             and any {@link Record} has been externally modified
     * @throws IllegalStateException if any of the {@code records} overrides the
     *             save pipeline, or if a prior save failed within the
     *             transaction
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
            }
            Saver saver = new IncrementalSaver(concourse);
            SaveContext context = new SaveContext(preventStaleWrites);
            try {
                // NOTE: The saver is never staged or committed here because
                // the connection is already within this transaction, whose
                // commit is the terminal operation.
                Set<Record> seen = Sets.newIdentityHashSet();
                for (Record record : records) {
                    record.bindGraph(this, provider, seen);
                    record.saveWithinTransaction(saver, context);
                }
                database.stageDeletions(saver, context);
            }
            catch (Throwable t) {
                // The writes that were staged before the failure cannot be
                // surgically undone, so the transaction must never commit.
                poisoned = true;
                context.restore();
                throw t;
            }
            saves.add(context);
            return true;
        }
        else {
            return database.save(preventStaleWrites, records);
        }
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
     * are skipped.
     * </p>
     *
     * @param hook the side effect to run after a successful commit
     * @throws IllegalStateException if the transaction already ended, or if a
     *             save failed within it
     */
    public void afterCommit(Runnable hook) {
        verify();
        afterCommitHooks.add(hook);
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
     * </p>
     * <p>
     * A hook that throws does not affect the outcome: the exception propagates
     * to the caller and any remaining hooks are skipped.
     * </p>
     *
     * @param hook the side effect to run after the transaction ends without a
     *            successful commit
     * @throws IllegalStateException if the transaction already ended, or if a
     *             save failed within it
     */
    public void afterAbort(Runnable hook) {
        verify();
        afterAbortHooks.add(hook);
    }

    /**
     * Attempt to commit the transaction and make every staged write durable.
     * <p>
     * On success, the lifecycle consequences of each staged save (e.g., save
     * and delete notifications) are dispatched and every
     * {@link #afterCommit(Runnable) afterCommit} hook runs. On failure, every
     * staged write is discarded and every {@link #afterAbort(Runnable)
     * afterAbort} hook runs; a {@link Record Record's} in-memory edits remain,
     * the same as after a failed save. Either way, the transaction ends.
     * </p>
     *
     * @return {@code true} if the transaction commits
     * @throws IllegalStateException if the transaction already ended, or if a
     *             save failed within it
     */
    public boolean commit() {
        verify();
        try {
            committed = concourse.commit();
        }
        finally {
            end(committed);
        }
        return committed;
    }

    /**
     * Abort the transaction and discard every staged write; a {@link Record
     * Record's} in-memory edits remain, the same as after a failed save. Every
     * {@link #afterAbort(Runnable) afterAbort} hook runs.
     * <p>
     * This method has no effect if the transaction already ended.
     * </p>
     */
    public void abort() {
        if(open) {
            verifyOwner();
            try {
                concourse.abort();
            }
            finally {
                end(false);
            }
        }
    }

    @Override
    public void close() {
        abort();
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
     * Return the {@link Runway} instance that this {@link Transaction} operates
     * against.
     *
     * @return the enclosing {@link Runway}
     */
    Runway database() {
        return database;
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
     * Return {@code true} if the transaction successfully committed.
     *
     * @return {@code true} if the transaction {@link #committed}
     */
    boolean committed() {
        return committed;
    }

    /**
     * Bind {@code result} to this {@link Transaction}: a {@link Record} is
     * bound along with its loaded graph, an {@link Iterable} is bound
     * element-wise, and any other result is left alone.
     *
     * @param result a resolved {@link Selection} result
     * @param seen the identity set of {@link Record Records} that are already
     *            bound
     */
    private void bind(Object result, Set<Record> seen) {
        if(result instanceof Record) {
            ((Record) result).bindGraph(this, provider, seen);
        }
        else if(result instanceof Iterable) {
            for (Object item : (Iterable<?>) result) {
                bind(item, seen);
            }
        }
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
                for (SaveContext context : saves) {
                    database.dispatchSaveOutcomes(context);
                }
                for (Runnable hook : afterCommitHooks) {
                    hook.run();
                }
            }
            else {
                for (int i = saves.size() - 1; i >= 0; --i) {
                    saves.get(i).restore();
                }
                for (Runnable hook : afterAbortHooks) {
                    hook.run();
                }
            }
        }
        finally {
            // A hook that throws must not prevent the connection from being
            // released.
            if(owned) {
                database.connections.release(concourse);
            }
        }
    }

    /**
     * Verify that the transaction is still {@link #open}, is not
     * {@link #poisoned} and that the caller is the {@link #owner} thread.
     */
    private void verify() {
        verifyOwner();
        Verify.that(open, "The Transaction has ended");
        verifyNotPoisoned();
    }

    /**
     * Verify that the transaction is not {@link #poisoned}.
     */
    private void verifyNotPoisoned() {
        Verify.that(!poisoned,
                "The Transaction cannot continue because a save failed"
                        + " within it; abort and retry the work in a new"
                        + " Transaction");
    }

    /**
     * Verify that the caller is the {@link #owner} thread.
     */
    private void verifyOwner() {
        Verify.that(Thread.currentThread() == owner,
                "A Transaction is confined to the thread that started it");
    }

}
