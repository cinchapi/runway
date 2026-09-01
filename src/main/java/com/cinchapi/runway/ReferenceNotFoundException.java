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

import com.cinchapi.common.base.AnyStrings;

/**
 * A {@link ReferenceNotFoundException} is thrown when a load encounters a stale
 * reference on a field whose {@link ReferenceNotFoundPolicy} is
 * {@link ReferenceNotFoundPolicy#ERROR ERROR}.
 * <p>
 * The message names the housing record, since that is the {@link Record} that
 * failed to load. {@link #key()} returns the field that holds the stale
 * reference, and {@link #target()} returns the id of the record with no stored
 * data.
 * </p>
 *
 * @author Jeff Nelson
 */
@SuppressWarnings("serial")
public class ReferenceNotFoundException extends RunwayException {

    /**
     * The name of the field that holds the stale reference.
     */
    private final String key;

    /**
     * The primary key of the referenced record.
     */
    private final long target;

    /**
     * Construct a new instance.
     *
     * @param holder the housing {@link Record}
     * @param key the name of the field that holds the stale reference
     * @param target the primary key of the referenced record
     */
    public ReferenceNotFoundException(Record holder, String key, long target) {
        super(AnyStrings.format(
                "{} {} references record {} through '{}', but that record "
                        + "has no stored data",
                holder.getClass().getSimpleName(), holder.id(), target, key));
        this.key = key;
        this.target = target;
    }

    /**
     * Return the name of the field that holds the stale reference.
     *
     * @return the field name
     */
    public String key() {
        return key;
    }

    /**
     * Return the primary key of the referenced record.
     *
     * @return the referenced record's primary key
     */
    public long target() {
        return target;
    }

}
