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

import com.cinchapi.runway.db.Saver;

/**
 * A {@link Save} is one logical save operation in motion.
 * <p>
 * A save can process multiple in-memory instances of the same logical record
 * (e.g., the caller's instance alongside a copy loaded for
 * {@link CaptureDelete} or {@link JoinDelete} handling). The save keeps one
 * entry per record id that pairs the instance that currently speaks for the
 * record with the record's {@link Outcome}, so every question asked about a
 * record during the save has exactly one answer.
 * </p>
 * <p>
 * An {@link Outcome} is monotonic: it only ever rises from {@link Outcome#CLEAN
 * CLEAN} to {@link Outcome#CHANGED CHANGED} to {@link Outcome#DELETED DELETED}
 * and never falls, so a fact recorded by one instance cannot be erased by a
 * later instance of the same record.
 * </p>
 * <p>
 * The save also holds the metadata {@link Record.Snapshot snapshots} needed to
 * {@link #restore() restore} every participating instance if the save fails,
 * and the queue of companion deletions that annotations like
 * {@link CascadeDelete} and {@link JoinDelete} schedule. Snapshots are
 * identity-keyed, because id-equal copies must each restore their own metadata,
 * and they survive {@link #stage(Saver) retry attempts}; all other state is
 * per-attempt.
 * </p>
 * <p>
 * The save also owns the attempt lifecycle: {@link #stage(Saver) stage} opens
 * an attempt's transaction, {@link #commit() commit} enforces deletion finality
 * before committing it, and {@link #abort() abort} discards it. The driver of a
 * save reaches the transaction only through those methods, while each
 * participating {@link Record} stages its reads and writes through the
 * {@link #saver() Saver} that the save carries.
 * </p>
 *
 * @author Jeff Nelson
 */
final class Save {

    /**
     * One {@link Entry} per record id processed within the active attempt.
     */
    private final Map<Long, Entry> entries = new HashMap<>();

    /**
     * A metadata {@link Record.Snapshot snapshot} for every instance that
     * participated in any attempt of this save. A save mutates record metadata
     * (e.g., the change checksum) before the transaction commits, so a failed
     * save must {@link #restore() restore} that metadata for a later save to
     * still observe the record's unsaved changes.
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
     * The {@link Saver} that owns the active attempt's transaction.
     */
    private Saver saver;

    /**
     * Construct a new instance.
     *
     * @param preventStaleWrite whether the save rejects any {@link Record} that
     *            has been externally modified
     */
    Save(boolean preventStaleWrite) {
        this.preventStaleWrite = preventStaleWrite;
    }

    /**
     * Abort the active attempt's staged transaction. Instance metadata is not
     * affected; a save that terminally fails must also {@link #restore()} it.
     */
    void abort() {
        saver.abort();
    }

    /**
     * Make {@code record} the instance that speaks for its id. The record's
     * {@link Outcome} is not affected.
     *
     * @param record the instance that is currently processed by the save
     */
    void claim(Record record) {
        entry(record).instance = record;
    }

    /**
     * Commit the active attempt.
     * <p>
     * A deletion is final within a save. Before the transaction commits, every
     * reference that a surviving record holds to a deleted record is removed
     * and each deletion is re-asserted as the last write for its record. A
     * survivor whose stored data changes as a result is marked
     * {@link Outcome#CHANGED CHANGED} so that it reports the save.
     * </p>
     *
     * @return {@code true} if the attempt's staged transaction committed;
     *         {@code false} if the database rejected the commit (e.g., a
     *         spurious failure that the caller may retry)
     */
    boolean commit() {
        Set<Long> deletions = deletions();
        if(!deletions.isEmpty()) {
            // NOTE: A record staged before a deletion may reference (or hold
            // data for) a record that the save has since deleted, so stage the
            // removal of every survivor's references to deleted records and
            // re-assert each deletion as the last write for its record. The
            // staged removals leave every survivor's in-memory state
            // untouched, so a failed transaction has nothing to roll back.
            for (Entry entry : entries.values()) {
                if(entry.outcome != Outcome.DELETED && entry.instance
                        .reconcileCaptureDeleteReferences(saver, deletions)) {
                    markChanged(entry.instance);
                }
            }
            for (long id : deletions) {
                saver.clear(id);
            }
        }
        return saver.commit();
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
     * Return {@code true} if any instance of the record with {@code id} was
     * processed within the active attempt.
     *
     * @param id the record id to test
     * @return {@code true} if the id was processed
     */
    boolean isSeen(long id) {
        return entries.containsKey(id);
    }

    /**
     * Return {@code true} if any instance of {@code record} was processed
     * within the active attempt.
     *
     * @param record the {@link Record} to test
     * @return {@code true} if the record's id was processed
     */
    boolean isSeen(Record record) {
        return isSeen(record.id());
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
     * Restore the metadata of every instance that participated in any attempt
     * of this save.
     */
    void restore() {
        snapshots.forEach((record, snapshot) -> record.restore(snapshot));
    }

    /**
     * Return the {@link Saver} through which a participating {@link Record}
     * stages its reads and writes against the active attempt's transaction.
     *
     * @return the {@link Saver}
     */
    Saver saver() {
        return saver;
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
     * Stage the transaction for a new attempt against {@code saver} and forget
     * the per-attempt state. The {@link Record.Snapshot snapshots} are kept, so
     * a retry can still {@link #restore() restore} instances that participated
     * in an earlier attempt.
     *
     * @param saver the {@link Saver} that owns the attempt's transaction
     */
    void stage(Saver saver) {
        this.saver = saver;
        entries.clear();
        pendingDeletions.clear();
        saver.stage();
    }

    /**
     * Return the ids of every record that the active attempt deleted.
     *
     * @return the deleted ids
     */
    private Set<Long> deletions() {
        Set<Long> ids = new LinkedHashSet<>();
        entries.forEach((id, entry) -> {
            if(entry.outcome == Outcome.DELETED) {
                ids.add(id);
            }
        });
        return ids;
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
     * declaration order and a record's {@link Outcome} only ever rises, so a
     * fact recorded by one instance survives every later instance of the same
     * record.
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
