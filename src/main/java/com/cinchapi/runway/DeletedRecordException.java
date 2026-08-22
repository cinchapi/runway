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
 * A {@link DeletedRecordException} is thrown when a save would write into a
 * {@link Record} that holds no data in the database. The write would restore a
 * record that no longer exists, so the save is refused and writes nothing.
 * <p>
 * The {@link #id()} names the {@link Record} that holds no data. That is not
 * always one of the records passed to the save, because a linked {@link Record}
 * that the save writes is checked the same way.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public class DeletedRecordException extends RunwayException {

    /**
     * The primary key of the deleted {@link Record}.
     */
    private final long id;

    /**
     * Construct a new instance.
     *
     * @param id the primary key of the deleted {@link Record}
     */
    public DeletedRecordException(long id) {
        super("Record " + id + " no longer exists");
        this.id = id;
    }

    /**
     * Return the primary key of the deleted {@link Record}.
     *
     * @return the deleted {@link Record Record's} primary key
     */
    public long id() {
        return id;
    }

}
