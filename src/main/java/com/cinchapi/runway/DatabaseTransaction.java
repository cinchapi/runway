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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.Verify;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.ForwardingConcourse;
import com.cinchapi.concourse.TransactionException;
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
 * The {@link Transaction} implementation that stages against a dedicated
 * {@link Concourse} connection.
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
class DatabaseTransaction extends Binding implements Transaction {

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
     * The {@link Record} that an in-flight {@link #intern(Record)} is saving
     * after its lookup found the identity unclaimed, or {@code null} when no
     * intern save is in flight. Only the {@link #owner} thread reads or writes
     * this.
     */
    @Nullable
    private Record internCandidate = null;

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
     * The latest value that a single-key atomic operation wrote for each
     * (record, key) pair within the transaction.
     */
    private final Map<Record, Map<String, Object>> atomicValues = new IdentityHashMap<>();

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
            if(open && connection == concourse) {
                // The Transaction owns the connection until the transaction
                // ends, so the return only closes the operation window that
                // the request opened.
                operating--;
            }
            else {
                // Any other connection belongs to the database's pool, so a
                // foreign release must not close an operation window that
                // this Transaction's own request opened.
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
    DatabaseTransaction(Runway database) {
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
    DatabaseTransaction(Runway database, Concourse concourse, boolean owned) {
        this.database = database;
        this.concourse = concourse;
        this.owned = owned;
        this.owner = Thread.currentThread();
        this.open = true;
        concourse.stage();
    }

    @Override
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
     * within {@link Runway#transact(java.util.function.Consumer) transact} and
     * {@link Runway#transactAndSupply(java.util.function.Function)
     * transactAndSupply}, each attempt is a distinct {@link Transaction}, so a
     * hook registered by the work runs for its own attempt, including an
     * attempt that a conflict retry discards. A poisoned transaction still
     * accepts registration, so cleanup can be scheduled before the required
     * {@link #abort()}.
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
     * {@link Runway#transact(java.util.function.Consumer) transact} and
     * {@link Runway#transactAndSupply(java.util.function.Function)
     * transactAndSupply}, each attempt is a distinct {@link Transaction}, so a
     * hook registered by the work never runs for an attempt that a conflict
     * retry discards.
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

    @Override
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
     * {@inheritDoc}
     * <p>
     * After the transaction ends, the {@link Record} operates against the
     * enclosing {@link Runway}.
     * </p>
     */
    @Override
    public <T extends Record> T create(Class<T> clazz, Object... args) {
        if(open) {
            verifyOwner();
            verifyNotPoisoned();
        }
        T record = Reflection.newInstance(clazz, args);
        join(record);
        return record;
    }

    /**
     * {@inheritDoc}
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     */
    @Nullable
    @Override
    public <T extends Record, V> T findAnyFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findAnyFirstAndUpdate requires an Order");
        if(open) {
            return execute(() -> Transaction.super.findAnyFirstAndUpdate(clazz,
                    criteria, order, key, update));
        }
        else {
            return database.findAnyFirstAndUpdate(clazz, criteria, order, key,
                    update);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     */
    @Nullable
    @Override
    public <T extends Record, V> T findAnyUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        if(open) {
            return execute(() -> Transaction.super.findAnyUniqueAndUpdate(clazz,
                    criteria, key, update));
        }
        else {
            return database.findAnyUniqueAndUpdate(clazz, criteria, key,
                    update);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     */
    @Nullable
    @Override
    public <T extends Record, V> T findFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        Verify.thatArgument(order != null,
                "findFirstAndUpdate requires an Order");
        if(open) {
            return execute(() -> Transaction.super.findFirstAndUpdate(clazz,
                    criteria, order, key, update));
        }
        else {
            return database.findFirstAndUpdate(clazz, criteria, order, key,
                    update);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * While the transaction is open, the lookup and the write stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     */
    @Nullable
    @Override
    public <T extends Record, V> T findUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        if(open) {
            return execute(() -> Transaction.super.findUniqueAndUpdate(clazz,
                    criteria, key, update));
        }
        else {
            return database.findUniqueAndUpdate(clazz, criteria, key, update);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * While the transaction is open, the lookup and the save stage within it.
     * After the transaction ends, the operation runs atomically against the
     * enclosing {@link Runway}.
     * </p>
     */
    @Override
    public <T extends Record> T intern(T record) {
        if(open) {
            return execute(() -> {
                T match = resolveFullIdentityMatch(record);
                if(match == null) {
                    Record prior = internCandidate;
                    internCandidate = record;
                    try {
                        save(record);
                    }
                    finally {
                        internCandidate = prior;
                    }
                    try {
                        T found = resolveFullIdentityMatch(record);
                        Verify.thatArgument(
                                found != null && record.id() == found.id(),
                                "The created Record does not match the criteria");
                    }
                    catch (Throwable t) {
                        poisoned = true;
                        throw t;
                    }
                    return record;
                }
                else {
                    return match;
                }
            });
        }
        else {
            return database.intern(record);
        }
    }

    /**
     * {@inheritDoc}
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
            SaveContext context = new SaveContext(preventStaleWrites,
                    database::hasDeleteListener, deletions, record -> {
                        record.verifySavableThrough(this);
                        record.bind(this, provider);
                    });
            execute(() -> {
                try {
                    // NOTE: The saver is never staged or committed here
                    // because the connection is already within this
                    // transaction, whose commit is the terminal operation. The
                    // flush is what sends a bulk saver's queued writes and
                    // runs its validators, so the staged state is on the
                    // server, and validated, before any later operation
                    // through this transaction reads it.
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
                    // selectively undone, so the transaction must never
                    // commit.
                    poisoned = true;
                    context.restore();
                    if(t instanceof TransactionException
                            || t instanceof StaleDataException
                            || t instanceof DeletedRecordException
                            || t instanceof Record.TransactionBoundaryException) {
                        throw t;
                    }
                    else if(internCandidate != null
                            && t instanceof Record.ConstraintViolationException
                            && ((Record.ConstraintViolationException) t)
                                    .record() == internCandidate) {
                        // This transaction just verified the identity was
                        // unclaimed, so a uniqueness refusal on the record
                        // being interned means a rival claimed it. Abort and
                        // retry so the next attempt adopts the winner.
                        throw new IdentityConflictException(t.getMessage());
                    }
                    else {
                        // A save cannot report a refusal by returning false
                        // here, because what it staged cannot be selectively
                        // undone. The refusal is delivered as the exception a
                        // Runway bound save delivers, so a caller handles a
                        // refusal the same either way.
                        SuppressedRunwayException refusal = new SuppressedRunwayException(
                                t.getMessage());
                        refusal.setStackTrace(t.getStackTrace());
                        throw refusal;
                    }
                }
                return Boolean.TRUE;
            });
            saves.add(context);
            deletions.addAll(context.deletions());
            return true;
        }
        else {
            return database.save(preventStaleWrites, records);
        }
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
     * {@inheritDoc}
     * <p>
     * While the transaction is open, starting another is refused. After the
     * transaction ends, a new {@link Transaction} starts on the enclosing
     * {@link Runway}.
     * </p>
     */
    @Override
    public Transaction startTransaction() {
        if(open) {
            throw new IllegalStateException("Cannot start a Transaction"
                    + " within an open Transaction because transactions do"
                    + " not nest; use transact or transactAndSupply to join"
                    + " this one");
        }
        else {
            return database.startTransaction();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * While the transaction is open, the work joins it: the work runs exactly
     * once, cannot commit or abort, and a conflict at commit belongs to the
     * transaction's owner. After the transaction ends, the work runs in its own
     * managed transaction on the enclosing {@link Runway}.
     * </p>
     */
    @Override
    public <T> T transactAndSupply(Function<TransactionInterface, T> work) {
        if(open) {
            return execute(() -> work.apply(this));
        }
        else {
            return database.transactAndSupply(work);
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
     * Return {@code true} if a failure poisoned the transaction, so its staged
     * writes can never commit.
     *
     * @return {@code true} if the transaction is {@link #poisoned}
     */
    boolean poisoned() {
        return poisoned;
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
     * Record that a single-key atomic operation wrote {@code value} for
     * {@code key} in {@code record}. If this transaction ends without a
     * successful commit, then {@code record} does not carry the write as an
     * unsaved change.
     *
     * @param record the {@link Record} that the operation wrote
     * @param key the name of the field the operation wrote
     * @param value the value the operation wrote, in its unserialized form
     */
    void recordAtomicValue(Record record, String key, Object value) {
        atomicValues.computeIfAbsent(record, ignore -> new HashMap<>()).put(key,
                value);
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
                    merged.merge(context);
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
                // A restore is whole-record, so it may revert baseline
                // entries that single-key atomic operations mirrored. Those
                // mirrors are not user edits, so reinstate them: the record
                // keeps the latest mirrored replacement and only real edits
                // remain unsaved.
                atomicValues.forEach((record, values) -> values
                        .forEach(record::updateBaseline));
                // NOTE: A later snapshot of the same record holds the keys
                // that a later save carried, and none of those saves
                // committed, so every one of them is declared again.
                for (SaveContext context : saves) {
                    context.forEachSnapshot((record, snapshot) -> record
                            .redeclareVerifyKeys(snapshot.verifyKeys));
                }
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
            atomicValues.clear();
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
     * Return the one {@link Record} that fully claims {@code record}'s unique
     * identity, or {@code null} when no record does.
     * <p>
     * Each {@link Unique} constraint is evaluated within its declared scope. A
     * match is a record that agrees with every participating constraint and
     * shares {@code record}'s concrete class. A partial claim, two different
     * claimants, or a claimant of another class yields {@code null}, so the
     * caller's subsequent save fails the scoped {@link Unique} enforcement,
     * which surfaces the conflict instead of a silent adoption.
     * </p>
     *
     * @param record the {@link Record} whose identity is resolved
     * @param <T> the type of {@link Record}
     * @return the full-identity match, or {@code null} when none exists
     * @throws DuplicateEntryException if more than one record matches a single
     *             constraint
     * @throws IllegalArgumentException if no field under a {@link Unique}
     *             constraint of {@code record} has a non-null value
     */
    @Nullable
    private <T extends Record> T resolveFullIdentityMatch(T record) {
        Class<?> clazz = record.getClass();
        Record match = null;
        for (UniqueIdentity identity : record.uniqueIdentities()) {
            Record candidate = identity.any()
                    ? findAnyUnique(identity.window(), identity.criteria())
                    : findUnique(identity.window(), identity.criteria());
            if(candidate == null
                    || (match != null && !candidate.equals(match))) {
                return null;
            }
            else {
                match = candidate;
            }
        }
        if(match != null && match.getClass() == clazz) {
            @SuppressWarnings("unchecked") T adopted = (T) match;
            return adopted;
        }
        else {
            return null;
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
