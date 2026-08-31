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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nullable;

/**
 * A {@link SaveContext} tracks the state of one save.
 * <p>
 * A save can process multiple in-memory instances of the same logical record
 * (e.g., the caller's instance alongside a copy loaded for
 * {@link CaptureDelete} or {@link JoinDelete} handling). The context keeps one
 * entry per record id that pairs the record's {@link Outcome} with its speaking
 * instance: the latest instance that reported the record's highest outcome.
 * </p>
 * <p>
 * An {@link Outcome} is monotonic: it only ever rises from {@link Outcome#CLEAN
 * CLEAN} to {@link Outcome#CHANGED CHANGED} to {@link Outcome#DELETED DELETED}
 * and never falls.
 * </p>
 * <p>
 * The context also holds the identity-keyed metadata {@link Record.Snapshot
 * snapshots} needed to {@link #restore() restore} every participating instance
 * if the save fails, and the queue of companion deletions that annotations like
 * {@link CascadeDelete} and {@link JoinDelete} schedule. Snapshots survive
 * {@link #reset() retry attempts}; all other state is per-attempt.
 * </p>
 * <p>
 * A save within a {@link Transaction} also treats the ids that the
 * transaction's earlier saves deleted as {@link #isDeleted(long) deleted}, so a
 * deletion stays final across the saves that share a commit.
 * </p>
 *
 * @author Jeff Nelson
 */
final class SaveContext {

    /**
     * An {@link #admit(Record) admission} that accepts every {@link Record}.
     */
    private static final Consumer<Record> NO_ADMISSION = record -> {};

    /**
     * The check that every {@link Record} must pass when it
     * {@link #admit(Record) enters} the save.
     */
    private final Consumer<Record> admission;

    /**
     * The stored state that each deleted record held when the active attempt
     * deleted it, keyed by record id.
     */
    private final Map<Long, Map<String, Set<Object>>> deletionData = new HashMap<>();

    /**
     * One {@link Entry} per record id processed within the active attempt.
     */
    private final Map<Long, Entry> entries = new HashMap<>();

    /**
     * A metadata {@link Record.Snapshot snapshot} for every instance that
     * participated in any attempt of this save.
     */
    private final Map<Record, Record.Snapshot> snapshots = new IdentityHashMap<>();

    /**
     * {@link Record Records} that companion deletions scheduled but that the
     * save has not yet processed.
     */
    private final Deque<Record> pendingDeletions = new ArrayDeque<>();

    /**
     * The ids of records that earlier saves in the same transaction deleted.
     */
    private final Set<Long> priorDeletions;

    /**
     * Whether the save fails if it would overwrite a value that another writer
     * changed.
     */
    private final boolean shouldPreventStaleWrite;

    /**
     * Construct a new instance.
     *
     * @param shouldPreventStaleWrite whether the save fails if it would
     *            overwrite a value that another writer changed
     */
    SaveContext(boolean shouldPreventStaleWrite) {
        this(shouldPreventStaleWrite, NO_ADMISSION);
    }

    /**
     * Construct a new instance whose {@code admission} checks every
     * {@link Record} that enters the save.
     *
     * @param shouldPreventStaleWrite whether the save fails if it would
     *            overwrite a value that another writer changed
     * @param admission the check that every {@link Record} must pass when it
     *            enters the save
     */
    SaveContext(boolean shouldPreventStaleWrite, Consumer<Record> admission) {
        this(shouldPreventStaleWrite, Collections.emptySet(), admission);
    }

    /**
     * Construct a new instance for a save within a transaction whose earlier
     * saves deleted the {@code priorDeletions}.
     *
     * @param shouldPreventStaleWrite whether the save fails if it would
     *            overwrite a value that another writer changed
     * @param priorDeletions the ids of records that earlier saves in the same
     *            transaction deleted
     * @param admission the check that every {@link Record} must pass when it
     *            enters the save
     */
    SaveContext(boolean shouldPreventStaleWrite, Set<Long> priorDeletions,
            Consumer<Record> admission) {
        this.shouldPreventStaleWrite = shouldPreventStaleWrite;
        this.priorDeletions = priorDeletions;
        this.admission = admission;
    }

    /**
     * Add {@code record} as a processed instance of its id. The record becomes
     * the speaking instance only while the id's {@link Outcome} is still
     * {@link Outcome#CLEAN CLEAN}; the {@link Outcome} itself is not affected.
     *
     * @param record the instance that is currently processed by the save
     */
    void add(Record record) {
        raise(record, Outcome.CLEAN);
    }

    /**
     * Apply the save's admission check to {@code record} as it enters the save.
     *
     * @param record the {@link Record} that enters the save
     * @throws IllegalStateException if the {@link Record} cannot participate in
     *             the save (e.g., it is bound to a different open
     *             {@link Transaction})
     */
    void admit(Record record) {
        admission.accept(record);
    }

    /**
     * Return the ids of every record whose deletion binds the active attempt:
     * the attempt's own {@link #deletions() deletions} and the ids that earlier
     * saves in the same transaction deleted.
     *
     * @return the deleted ids
     */
    Set<Long> allDeletions() {
        if(priorDeletions.isEmpty()) {
            return deletions();
        }
        else {
            Set<Long> ids = new LinkedHashSet<>(priorDeletions);
            ids.addAll(deletions());
            return ids;
        }
    }

    /**
     * Return {@code true} if any in-memory instance of the record with
     * {@code id} was processed within the active attempt.
     *
     * @param id the record id to test
     * @return {@code true} if the record was processed
     */
    boolean contains(long id) {
        return entries.containsKey(id);
    }

    /**
     * Return {@code true} if any in-memory instance that shares the id of
     * {@code record} was processed within the active attempt.
     *
     * @param record the {@link Record} to test
     * @return {@code true} if the record was processed
     */
    boolean contains(Record record) {
        return contains(record.id());
    }

    /**
     * Return the state that the record with {@code id} stored when the active
     * attempt deleted it.
     *
     * @param id the record id
     * @return the stored state, or an empty {@link Map} if the attempt did not
     *         delete the record
     */
    Map<String, Set<Object>> deletionData(long id) {
        return deletionData.getOrDefault(id, Collections.emptyMap());
    }

    /**
     * Return the ids of every record that the active attempt deleted.
     *
     * @return the deleted ids
     */
    Set<Long> deletions() {
        Set<Long> ids = new LinkedHashSet<>();
        entries.forEach((id, entry) -> {
            if(entry.outcome == Outcome.DELETED) {
                ids.add(id);
            }
        });
        return ids;
    }

    /**
     * Dispatch every processed record and its {@link Outcome} to the
     * {@code consumer}.
     *
     * @param consumer the consumer that accepts each speaking instance and the
     *            record's {@link Outcome}
     */
    void forEach(BiConsumer<Record, Outcome> consumer) {
        for (Entry entry : entries.values()) {
            consumer.accept(entry.instance, entry.outcome);
        }
    }

    /**
     * Dispatch every snapshotted {@link Record} and its {@link Record.Snapshot}
     * to the {@code consumer}.
     *
     * @param consumer the consumer that accepts each {@link Record} and its
     *            snapshot
     */
    void forEachSnapshot(BiConsumer<Record, Record.Snapshot> consumer) {
        snapshots.forEach(consumer);
    }

    /**
     * Return the instance that speaks for the record with {@code id}, or
     * {@code null} if the active attempt has not processed the id.
     *
     * @param id the record id
     * @return the speaking instance, or {@code null}
     */
    @Nullable
    Record instance(long id) {
        Entry entry = entries.get(id);
        return entry != null ? entry.instance : null;
    }

    /**
     * Return {@code true} if the active attempt, or an earlier save in the same
     * transaction, deleted the record with {@code id}.
     *
     * @param id the record id to test
     * @return {@code true} if the record was deleted
     */
    boolean isDeleted(long id) {
        Entry entry = entries.get(id);
        if(entry != null && entry.outcome == Outcome.DELETED) {
            return true;
        }
        else {
            return priorDeletions.contains(id);
        }
    }

    /**
     * Record that the active attempt stages data changes for {@code record} and
     * make it the instance that speaks for its id, unless the record was
     * already deleted.
     *
     * @param record the {@link Record} whose data the save changes
     */
    void markChanged(Record record) {
        raise(record, Outcome.CHANGED);
    }

    /**
     * Record that the active attempt deletes {@code record} and make it the
     * instance that speaks for its id.
     *
     * @param record the {@link Record} that the save deletes
     */
    void markDeleted(Record record) {
        raise(record, Outcome.DELETED);
    }

    /**
     * Merge {@code record} and its {@code outcome} from another
     * {@link SaveContext} into this one, under the same rule that governs a
     * single save: the record's {@link Outcome} only rises, and {@code record}
     * becomes the speaking instance only if {@code outcome} is at least the
     * current one.
     *
     * @param record an instance that another {@link SaveContext} processed
     * @param outcome the {@link Outcome} that the other context recorded for
     *            the instance
     */
    void merge(Record record, Outcome outcome) {
        raise(record, outcome);
    }

    /**
     * Remove and return the next {@link Record} that a companion deletion
     * scheduled, or {@code null} if none remain.
     *
     * @return the next scheduled {@link Record} or {@code null}
     */
    @Nullable
    Record pollPendingDeletion() {
        return pendingDeletions.poll();
    }

    /**
     * Associate the state that the record with {@code id} stored when the
     * active attempt deleted it, so a successful save can report it.
     *
     * @param id the record id
     * @param data the record's stored state
     */
    void recordDeletionData(long id, Map<String, Set<Object>> data) {
        deletionData.put(id, data);
    }

    /**
     * Reset the per-attempt state for a new save attempt. The
     * {@link Record.Snapshot snapshots} are kept.
     */
    void reset() {
        entries.clear();
        pendingDeletions.clear();
        deletionData.clear();
    }

    /**
     * Restore the metadata of every instance that participated in any attempt
     * of this save.
     */
    void restore() {
        snapshots.forEach((record, snapshot) -> record.restore(snapshot));
    }

    /**
     * Schedule {@code record} for deletion as a companion of a record that the
     * save is deleting.
     *
     * @param record the {@link Record} to delete alongside its companion
     */
    void scheduleDeletion(Record record) {
        pendingDeletions.add(record);
    }

    /**
     * Return whether the save fails if it would overwrite a value that another
     * writer changed.
     *
     * @return {@code true} if stale writes are rejected
     */
    boolean shouldPreventStaleWrite() {
        return shouldPreventStaleWrite;
    }

    /**
     * Snapshot the metadata of {@code record} if this save has not already done
     * so.
     *
     * @param record the {@link Record} to snapshot
     */
    void snapshot(Record record) {
        if(!snapshots.containsKey(record)) {
            snapshots.put(record, record.snapshot());
        }
    }

    /**
     * Return the {@link Entry} for the {@code record}, creating it if the
     * active attempt has not processed the record's id yet.
     *
     * @param record a {@link Record} instance
     * @return the {@link Entry} for the record's id
     */
    private Entry entry(Record record) {
        return entries.computeIfAbsent(record.id(), ignore -> new Entry());
    }

    /**
     * Raise the record's {@link Outcome} to {@code outcome} if it is higher
     * than the current one, and make {@code record} the instance that speaks
     * for its id if {@code outcome} is at least the current one.
     *
     * @param record the instance that reports the fact
     * @param outcome the {@link Outcome} to raise to
     */
    private void raise(Record record, Outcome outcome) {
        Entry entry = entry(record);
        if(outcome.compareTo(entry.outcome) >= 0) {
            entry.instance = record;
            entry.outcome = outcome;
        }
        else {
            // The instance that reported the higher outcome keeps speaking
            // for the id.
        }
    }

    /**
     * The save's outcome for one record. The constants form a ladder in
     * declaration order and a record's {@link Outcome} only ever rises.
     */
    enum Outcome {

        /**
         * The save processed the record but staged no data changes for it.
         */
        CLEAN,

        /**
         * The save staged data changes for the record.
         */
        CHANGED,

        /**
         * The save deleted the record.
         */
        DELETED
    }

    /**
     * The pairing of the instance that speaks for a record with the record's
     * {@link Outcome}.
     */
    private static final class Entry {

        /**
         * The instance that speaks for the record.
         */
        Record instance;

        /**
         * The record's {@link Outcome}.
         */
        Outcome outcome = Outcome.CLEAN;
    }

}
