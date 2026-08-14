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
 * A {@link StaleDataException} is thrown when a save with stale-write
 * prevention is rejected because a value the save writes changed in the
 * database since the {@link Record} that holds it last loaded or saved it.
 * <p>
 * The {@link #id()} names the {@link Record} that holds the changed value,
 * which is not necessarily one of the records the caller passed to the save: a
 * linked {@link Record} that the save writes is subject to the same test.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public class StaleDataException extends RunwayException {

    /**
     * The primary key of the stale {@link Record}.
     */
    private final long id;

    /**
     * Construct a new instance.
     *
     * @param id the primary key of the stale {@link Record}
     */
    public StaleDataException(long id) {
        super("Record " + id + " has stale data");
        this.id = id;
    }

    /**
     * Return the primary key of the stale {@link Record}.
     *
     * @return the stale {@link Record Record's} primary key
     */
    public long id() {
        return id;
    }

}
