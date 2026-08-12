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
 * A {@link DatabaseInterface} that scopes every operation to a single ACID
 * transaction whose lifecycle the caller owns.
 * <p>
 * Every read observes the transaction's isolated snapshot, including its own
 * uncommitted writes, and joins its conflict footprint. Results resolve
 * eagerly. Writes become durable when {@link #commit()} succeeds; until then no
 * reader outside the transaction can observe them.
 * </p>
 * <p>
 * Every loaded {@link Record}, including the records reachable from its fields,
 * is bound to the transaction, so each {@link Record#save() save} stages within
 * it. A {@link Record} {@link #create(Class, Object...) created} through the
 * view is bound to it as well. A {@link Record} that is not bound to the
 * transaction, including one constructed directly, operates against its own
 * binding even while the transaction is open.
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
public interface Transaction extends AutoCloseable, TransactionInterface {

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
    public void abort();

    /**
     * End the transaction, if it is still open, by {@link #abort() aborting}
     * whatever was not committed.
     */
    @Override
    public void close();

    /**
     * Attempt to commit the transaction and make every staged write durable.
     * <p>
     * On success, the lifecycle consequences of each staged save (e.g., save
     * and delete notifications) are dispatched and every
     * {@link #afterCommit(Runnable) afterCommit} hook runs. A failure while the
     * consequences dispatch propagates the same as a hook failure: the commit
     * itself stands. On failure, every staged write is discarded and every
     * {@link #afterAbort(Runnable) afterAbort} hook runs; a {@link Record
     * Record's} in-memory edits remain, the same as after a failed save. Either
     * way, the transaction ends.
     * </p>
     *
     * @return {@code true} if the transaction commits
     * @throws IllegalStateException if the transaction already ended, if a save
     *             failed within it, or if one of the transaction's own
     *             operations is in flight
     */
    public boolean commit();

}
