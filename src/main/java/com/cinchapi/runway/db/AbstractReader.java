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

import com.cinchapi.concourse.Concourse;
import com.google.common.base.Preconditions;

/**
 * Base class for {@link Reader} implementations that wrap a single
 * {@link Concourse} connection.
 *
 * @author Jeff Nelson
 */
public abstract class AbstractReader implements Reader {

    /**
     * The {@link Concourse} connection against which reads are issued or
     * submitted.
     */
    protected final Concourse concourse;

    /**
     * Construct a new {@link AbstractReader}.
     *
     * @param concourse the {@link Concourse} connection; must not be
     *            {@code null}
     */
    protected AbstractReader(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
    }

}
