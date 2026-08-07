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
 * A {@link Binding} is a {@link DatabaseInterface} that can also persist
 * {@link Record} changes, so it is the contract that a {@link Record} binds to:
 * reads and saves both resolve against the same scope.
 *
 * @author Jeff Nelson
 */
interface Binding extends DatabaseInterface {

    /**
     * Load the {@link Record} that is identified by {@code id} without knowing
     * its {@link Class} in advance.
     *
     * @param id the record id
     * @return the loaded {@link Record}
     */
    public <T extends Record> T load(long id);

    /**
     * Save all changes in the provided {@code records}.
     *
     * @param records one or more {@link Record Records} to save
     * @return {@code true} if the changes are accepted
     */
    public default boolean save(Record... records) {
        return save(false, records);
    }

    /**
     * Save all changes in the provided {@code records}.
     *
     * @param preventStaleWrites if {@code true}, reject the save when any
     *            {@link Record} in the object graph has stale data
     * @param records one or more {@link Record Records} to save
     * @return {@code true} if the changes are accepted
     * @throws StaleDataException if {@code preventStaleWrites} is {@code true}
     *             and any {@link Record} has been externally modified
     */
    public boolean save(boolean preventStaleWrites, Record... records);

}
