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
package com.cinchapi.runway.db;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;

/**
 * A {@link Saver} encapsulates the database interaction for one save &mdash;
 * the staged transaction, the save-time validation reads, the persisted writes,
 * and the terminal commit or abort &mdash; behind a single type that
 * {@link com.cinchapi.runway.Record#saveWithinTransaction(Saver, java.util.Map, java.util.Map, boolean)}
 * can target without knowing whether the underlying database supports the bulk
 * {@link com.cinchapi.concourse.lang.CommandGroup CommandGroup} command API.
 * <p>
 * <h2>Reads</h2> Validation reads ({@link #audit(long, Consumer) audit},
 * {@link #find(Criteria, Consumer) find}) accept a {@link Consumer} that
 * receives the read's result and may throw to signal a validation failure.
 * Implementations decide <em>when</em> the {@link Consumer} runs: immediately
 * for the synchronous path; queued-and-flushed inside {@link #commit()} for the
 * bulk path. Either way the {@link Consumer} sees the same data and any
 * exception it raises propagates to the caller of the recording method (for
 * immediate implementations) or the caller of {@link #commit()} (for bulk
 * implementations).
 * </p>
 * <h2>Writes</h2> Writes ({@link #set set}, {@link #clear(String, long) clear},
 * {@link #verifyOrSet verifyOrSet}, {@link #reconcile reconcile}) are recorded
 * against the active transaction. Bulk implementations defer their server- side
 * execution until {@link #commit()}; synchronous implementations execute each
 * write immediately. In both cases the staged transaction is the unit of
 * atomicity &mdash; a failure anywhere before {@link #commit()} succeeds aborts
 * the entire save.
 *
 * <h2>Lifecycle</h2> The caller drives the lifecycle: {@link #stage()} once,
 * any number of recording calls, then exactly one of {@link #commit()} or
 * {@link #abort()}. A {@link Saver} is single-use and is not safe for
 * concurrent access.
 *
 * @author Jeff Nelson
 */
public interface Saver {

    /**
     * Return the underlying {@link Concourse} connection.
     *
     * @return the wrapped {@link Concourse} connection
     */
    Concourse concourse();

    /**
     * Begin a staged transaction for the save this {@link Saver} represents.
     * <p>
     * Must be called exactly once before any recording method. For synchronous
     * implementations the stage takes effect on the server immediately; for
     * bulk implementations the stage is recorded for execution as the first
     * command of the read submission inside {@link #commit()}.
     * </p>
     */
    void stage();

    /**
     * Commit the staged transaction.
     * <p>
     * For synchronous implementations this delegates straight to the underlying
     * connection's commit. Bulk implementations submit accumulated recordings
     * with as few round trips as the save permits: a single submission carrying
     * {@link #stage()}, every write, and the terminal commit when no validation
     * reads were recorded; otherwise the reads submission first (running every
     * queued validator against its result list) followed by the
     * writes-plus-commit submission.
     * </p>
     * <p>
     * If any queued validator throws, the writes are <strong>not</strong>
     * submitted; the exception propagates and the caller is responsible for
     * calling {@link #abort()}.
     * </p>
     *
     * @return {@code true} if the staged transaction committed; {@code false}
     *         if the server rejected the commit (e.g. a spurious commit failure
     *         that the caller may retry)
     */
    boolean commit();

    /**
     * Abort the staged transaction.
     * <p>
     * Safe to call even when nothing has been submitted yet (for bulk
     * implementations) &mdash; in that case the call is a no-op because no
     * server-side state exists to roll back.
     * </p>
     */
    void abort();

    /**
     * Record an {@link Concourse#audit(long) audit} for {@code record} and
     * arrange to apply {@code validator} to the result.
     * <p>
     * The {@code validator} may throw to signal a validation failure (typically
     * {@link com.cinchapi.runway.StaleDataException}); the exception propagates
     * from the recording call for synchronous implementations and from
     * {@link #commit()} for bulk implementations.
     * </p>
     *
     * @param record the record id whose change history is being inspected
     * @param validator a {@link Consumer} that receives the audit result and
     *            may throw to reject the save
     */
    void audit(long record, Consumer<Map<Timestamp, List<String>>> validator);

    /**
     * Record a {@link Concourse#find(Criteria) find} for the {@code criteria}
     * and arrange to apply {@code validator} to the matching record ids.
     * <p>
     * The {@code validator} may throw to signal a validation failure (typically
     * {@link IllegalStateException} for a {@link com.cinchapi.runway.Unique}
     * violation); the exception propagates from the recording call for
     * synchronous implementations and from {@link #commit()} for bulk
     * implementations.
     * </p>
     *
     * @param criteria the {@link Criteria} that identifies the matching records
     * @param validator a {@link Consumer} that receives the matching ids and
     *            may throw to reject the save
     */
    void find(Criteria criteria, Consumer<Set<Long>> validator);

    /**
     * Record a {@link Concourse#select(String, Criteria) select} of values for
     * {@code key} on every record matching {@code criteria} and arrange to
     * apply {@code consumer} to the resulting record-keyed map.
     * <p>
     * Unlike {@link #audit audit} and {@link #find find}, this read drives
     * control flow rather than a throw/no-throw validation &mdash; the
     * {@code consumer} typically iterates the result and triggers further save
     * work (e.g. cascade-delete loads). Implementations therefore guarantee
     * that {@code consumer} runs before the recording call returns. For bulk
     * implementations this means an early submission of any reads accumulated
     * so far so the result is available; subsequent recordings start a fresh
     * batch.
     * </p>
     *
     * @param key the field name whose values should be returned
     * @param criteria the {@link Criteria} that identifies the matching records
     * @param consumer a {@link Consumer} that receives the result and may
     *            mutate caller state, trigger further recordings on this
     *            {@link Saver}, or throw to reject the save
     */
    void select(String key, Criteria criteria,
            Consumer<Map<Long, Set<Object>>> consumer);

    /**
     * Record a {@link Concourse#set(String, Object, long) set} of {@code value}
     * for {@code key} in {@code record}.
     *
     * @param key the field name to set
     * @param value the value to associate with {@code key}
     * @param record the record id to set into
     */
    void set(String key, Object value, long record);

    /**
     * Record a {@link Concourse#clear(String, long) clear} of all values for
     * {@code key} in {@code record}.
     *
     * @param key the field name to clear
     * @param record the record id to clear from
     */
    void clear(String key, long record);

    /**
     * Record a {@link Concourse#clear(long) clear} of every value stored in
     * {@code record}, leaving the record empty.
     *
     * @param record the record id to clear
     */
    void clear(long record);

    /**
     * Record a {@link Concourse#verifyOrSet(String, Object, long) verifyOrSet}
     * of {@code value} for {@code key} in {@code record}.
     *
     * @param key the field name to verifyOrSet
     * @param value the value to associate with {@code key}
     * @param record the record id whose mapping is being verified or set
     */
    void verifyOrSet(String key, Object value, long record);

    /**
     * Record a {@link Concourse#reconcile(String, long, Collection) reconcile}
     * of {@code values} for {@code key} in {@code record}.
     *
     * @param key the field name whose values are being reconciled
     * @param record the record id whose mapping is being reconciled
     * @param values the canonical set of values that should remain under
     *            {@code key}
     */
    void reconcile(String key, long record, Collection<?> values);

    /**
     * Record a {@link Concourse#reconcile(String, long, Collection) reconcile}
     * of {@code values} for {@code key} in {@code record}.
     *
     * @param key the field name whose values are being reconciled
     * @param record the record id whose mapping is being reconciled
     * @param values the canonical set of values that should remain under
     *            {@code key}
     */
    void reconcile(String key, long record, Object[] values);

    /**
     * Declare that {@code record} intends to write the value identified by
     * {@code canonical} within the current save batch, so that batched
     * implementations can detect intra-batch duplicates that the database
     * itself cannot see (because writes are deferred until {@link #commit()}).
     * <p>
     * Synchronous implementations no-op because each write enters the staged
     * transaction immediately and the next read observes it. Batched
     * implementations track each declared {@code canonical} and throw
     * {@link IllegalStateException} carrying {@code errorMessage} when the same
     * {@code canonical} is declared a second time with a different
     * {@code record}.
     * </p>
     *
     * @param canonical a value-equal key identifying the uniqueness constraint
     *            being asserted; equal keys across calls indicate the same
     *            constraint
     * @param record the id of the {@link com.cinchapi.runway.Record Record}
     *            that intends to write this value
     * @param errorMessage the message attached to the
     *            {@link IllegalStateException} thrown on an intra-batch
     *            conflict
     * @throws IllegalStateException if another
     *             {@link com.cinchapi.runway.Record Record} in this save batch
     *             has already declared {@code canonical}
     */
    default void declareUniqueIntent(Object canonical, long record,
            String errorMessage) {/* no-op */}

}
