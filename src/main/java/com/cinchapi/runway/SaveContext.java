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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

/**
 * A {@link SaveContext} tracks the state of one save.
 * <p>
 * A save can process multiple in-memory instances of the same logical record
 * (e.g., the caller's instance alongside a copy loaded for
 * {@link CaptureDelete} or {@link JoinDelete} handling). The context keeps one
 * entry per record id that pairs the instance that currently speaks for the
 * record with the record's {@link Outcome}.
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
 *
 * @author Jeff Nelson
 */
final class SaveContext {

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
     * Whether the save rejects any {@link Record} that has been externally
     * modified.
     */
    private final boolean preventStaleWrite;

    /**
     * Construct a new instance.
     *
     * @param preventStaleWrite whether the save rejects any {@link Record} that
     *            has been externally modified
     */
    SaveContext(boolean preventStaleWrite) {
        this.preventStaleWrite = preventStaleWrite;
    }

    /**
     * Add {@code record} as the instance that speaks for its id. The record's
     * {@link Outcome} is not affected.
     *
     * @param record the instance that is currently processed by the save
     */
    void add(Record record) {
        entry(record).instance = record;
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
     * Record that the active attempt stages data changes for {@code record} and
     * make it the instance that speaks for its id.
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
     * Return whether the save rejects any {@link Record} that has been
     * externally modified.
     *
     * @return {@code true} if stale writes are rejected
     */
    boolean preventStaleWrite() {
        return preventStaleWrite;
    }

    /**
     * Return the instance that speaks for each record processed within the
     * active attempt.
     *
     * @return the speaking instances
     */
    Collection<Record> records() {
        Collection<Record> records = new ArrayList<>(entries.size());
        for (Entry entry : entries.values()) {
            records.add(entry.instance);
        }
        return records;
    }

    /**
     * Reset the per-attempt state for a new save attempt. The
     * {@link Record.Snapshot snapshots} are kept.
     */
    void reset() {
        entries.clear();
        pendingDeletions.clear();
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
     * Make {@code record} the instance that speaks for its id and raise the
     * record's {@link Outcome} to {@code outcome} if it is higher than the
     * current one.
     *
     * @param record the instance that reports the fact
     * @param outcome the {@link Outcome} to raise to
     */
    private void raise(Record record, Outcome outcome) {
        Entry entry = entry(record);
        entry.instance = record;
        if(outcome.compareTo(entry.outcome) > 0) {
            entry.outcome = outcome;
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
