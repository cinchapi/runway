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
 * A {@link Binding} is a {@link DatabaseInterface} that can persist
 * {@link Record} changes. A {@link Record} binds to a {@link Binding}, and its
 * reads and saves resolve against the bound scope.
 *
 * @author Jeff Nelson
 */
abstract class Binding implements DatabaseInterface {

    /**
     * Load the {@link Record} identified by {@code id} and resolve its actual
     * type.
     *
     * @param id the record id
     * @return the loaded {@link Record}
     */
    abstract <T extends Record> T load(long id);

    /**
     * Save all changes in the provided {@code records}.
     *
     * @param records one or more {@link Record Records} to save
     * @return {@code true} if the changes are accepted
     */
    public boolean save(Record... records) {
        return save(false, records);
    }

    /**
     * Save all changes in the provided {@code records}.
     *
     * @param preventStaleWrites if {@code true}, reject the save if it would
     *            overwrite a value that another writer changed
     * @param records one or more {@link Record Records} to save
     * @return {@code true} if the changes are accepted
     * @throws DeletedRecordException if a {@link Record} that the save writes
     *             holds no data in the database, so the save would restore a
     *             record that another writer erased
     * @throws StaleDataException if {@code preventStaleWrites} is {@code true}
     *             and the save would overwrite a value that another writer
     *             changed, or if another writer changed a value that
     *             {@link Record#verifyOnSave(String...) verifyOnSave} declared
     */
    public abstract boolean save(boolean preventStaleWrites, Record... records);

}
