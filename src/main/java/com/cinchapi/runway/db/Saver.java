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
 * A {@link Saver} encapsulates the database interaction for one save: the
 * staged transaction, the save-time validation reads, the persisted writes, and
 * the terminal commit or abort.
 * <p>
 * <h2>Reads</h2> A validation read accepts a {@link Consumer} that may throw to
 * signal a validation failure. Depending on the implementation, the
 * {@link Consumer} runs either inline at the recording call or deferred until
 * {@link #commit()} or {@link #flush()}; the throw propagates from whichever
 * site invokes the {@link Consumer}.
 * </p>
 * <h2>Writes</h2> A write is recorded against the active staged transaction,
 * which is the unit of atomicity: a failure anywhere before {@link #commit()}
 * succeeds aborts the entire save.
 *
 * <h2>Lifecycle</h2> The caller drives the lifecycle. For a self-contained
 * save: {@link #stage()} once, any number of recording calls, then exactly one
 * of {@link #commit()} or {@link #abort()}. For a save that participates in an
 * externally managed transaction: no {@link #stage()}, any number of recording
 * calls, then {@link #flush()}; the external transaction's own commit or abort
 * is terminal. A {@link Saver} is single-use and is not safe for concurrent
 * access.
 *
 * @author Jeff Nelson
 */
public interface Saver {

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
     * Record an {@link Concourse#add(String, Object, long) add} of
     * {@code value} to {@code key} in {@code record}. The addition is a no-op
     * when {@code key} already holds {@code value}.
     *
     * @param key the field name to add to
     * @param value the value to add
     * @param record the record id to add into
     */
    void add(String key, Object value, long record);

    /**
     * Record an {@link Concourse#audit(long) audit} for {@code record} and
     * arrange to apply {@code validator} to the result.
     * <p>
     * The {@code validator} may throw to signal a validation failure (typically
     * {@link com.cinchapi.runway.StaleDataException}); the exception propagates
     * from the recording call for synchronous implementations and from
     * {@link #commit()} or {@link #flush()} for bulk implementations.
     * </p>
     *
     * @param record the record id whose change history is being inspected
     * @param validator a {@link Consumer} that receives the audit result and
     *            may throw to reject the save
     */
    void audit(long record, Consumer<Map<Timestamp, List<String>>> validator);

    /**
     * Record a {@link Concourse#clear(long) clear} of every value stored in
     * {@code record}, leaving the record empty.
     *
     * @param record the record id to clear
     */
    void clear(long record);

    /**
     * Record a {@link Concourse#clear(String, long) clear} of all values for
     * {@code key} in {@code record}.
     *
     * @param key the field name to clear
     * @param record the record id to clear from
     */
    void clear(String key, long record);

    /**
     * Commit the staged transaction.
     * <p>
     * For synchronous implementations this delegates straight to the underlying
     * connection's commit. Bulk implementations submit accumulated recordings
     * with as few round trips as the save permits. When no validation reads
     * were recorded, a single submission carries {@link #stage()}, every write,
     * and the terminal commit. When validation reads were recorded, they are
     * submitted first &mdash; alongside the writes &mdash; so every queued
     * validator can run against the result; the commit then follows in a second
     * submission once validation passes.
     * </p>
     * <p>
     * If a queued validator throws, the commit is <strong>not</strong>
     * submitted. The writes share the validation submission and have therefore
     * already reached the server within the staged transaction; the exception
     * propagates and the caller is responsible for calling {@link #abort()} to
     * roll them back.
     * </p>
     *
     * @return {@code true} if the staged transaction committed; {@code false}
     *         if the server rejected the commit (e.g. a spurious commit failure
     *         that the caller may retry)
     */
    boolean commit();

    /**
     * Record a {@link Concourse#find(Criteria) find} for the {@code criteria}
     * and arrange to apply {@code validator} to the matching record ids.
     * <p>
     * The {@code validator} may throw to signal a validation failure (typically
     * a {@link com.cinchapi.runway.Unique} violation surfaces as a
     * {@link com.cinchapi.runway.Record.ConstraintViolationException}); the
     * exception propagates from the recording call for synchronous
     * implementations and from {@link #commit()} or {@link #flush()} for bulk
     * implementations.
     * </p>
     *
     * @param criteria the {@link Criteria} that identifies the matching records
     * @param validator a {@link Consumer} that receives the matching ids and
     *            may throw to reject the save
     */
    void find(Criteria criteria, Consumer<Set<Long>> validator);

    /**
     * Ensure that every recorded operation has reached the server and that
     * every queued validator has run.
     * <p>
     * Synchronous implementations perform each operation when it is recorded,
     * so the default implementation does nothing. Bulk implementations submit
     * every pending operation and apply the queued validators, any of which may
     * throw to reject the save; the operations that shared the submission have
     * already reached the server, so the caller is responsible for the
     * enclosing transaction's rollback after handling the exception. When the
     * recorded operations participate in an externally managed transaction, the
     * caller does not invoke {@link #commit()} and must instead call this
     * method after its final recording, so no operation remains client-side.
     * </p>
     */
    default void flush() {}

    /**
     * Record a {@link Concourse#reconcile(String, long, Collection) reconcile}
     * of {@code values} for {@code key} in {@code record}.
     * <p>
     * An empty {@code values} is equivalent to {@link #clear(String, long)
     * clear(key, record)}; {@code reconcile} mandates a non-empty canonical and
     * "leave nothing" is what {@code clear} is for. Implementations route an
     * empty {@code values} through {@link #clear(String, long)} so callers see
     * uniform behavior regardless of transport.
     * </p>
     *
     * @param key the field name whose values are being reconciled
     * @param record the record id whose mapping is being reconciled
     * @param values the canonical set of values that should remain under
     *            {@code key}; an empty {@link Collection} is routed through
     *            {@link #clear(String, long)}
     */
    void reconcile(String key, long record, Collection<?> values);

    /**
     * Record a {@link Concourse#reconcile(String, long, Collection) reconcile}
     * of {@code values} for {@code key} in {@code record}.
     * <p>
     * An empty {@code values} is equivalent to {@link #clear(String, long)
     * clear(key, record)}; {@code reconcile} mandates a non-empty canonical and
     * "leave nothing" is what {@code clear} is for. Implementations route an
     * empty {@code values} through {@link #clear(String, long)} so callers see
     * uniform behavior regardless of transport.
     * </p>
     *
     * @param key the field name whose values are being reconciled
     * @param record the record id whose mapping is being reconciled
     * @param values the canonical set of values that should remain under
     *            {@code key}; an empty array is routed through
     *            {@link #clear(String, long)}
     */
    void reconcile(String key, long record, Object[] values);

    /**
     * Record a {@link Concourse#remove(String, Object, long) remove} of
     * {@code value} from {@code key} in {@code record}. The removal is a no-op
     * when {@code key} does not hold {@code value}.
     *
     * @param key the field name to remove from
     * @param value the value to remove
     * @param record the record id to remove from
     */
    void remove(String key, Object value, long record);

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
     * Begin a staged transaction for the save this {@link Saver} represents.
     * <p>
     * Must be called exactly once before any recording method for a
     * self-contained save, and never for a save that participates in an
     * externally managed transaction. For synchronous implementations the stage
     * takes effect on the server immediately; for bulk implementations the
     * stage is recorded for execution as the first command of the first
     * submission.
     * </p>
     */
    void stage();

    /**
     * Record a {@link Concourse#verifyOrSet(String, Object, long) verifyOrSet}
     * of {@code value} for {@code key} in {@code record}.
     *
     * @param key the field name to verifyOrSet
     * @param value the value to associate with {@code key}
     * @param record the record id whose mapping is being verified or set
     */
    void verifyOrSet(String key, Object value, long record);

}
