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

import java.util.ArrayList;
import java.util.List;

import com.cinchapi.concourse.Concourse;
import com.google.common.base.Preconditions;

/**
 * Base class for {@link Reader} implementations that wrap a single
 * {@link Concourse} connection. Provides shared bookkeeping for completions
 * registered via {@link #onDrain(Runnable)} and a {@link #drain()} template
 * that delegates the implementation-specific flush step to
 * {@link #prepareDrain()}.
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
     * Completions registered via {@link #onDrain(Runnable)} and run by
     * {@link #drain()} in registration order.
     */
    private final List<Runnable> completions;

    /**
     * Whether the deferred reads have been issued (drained); when {@code true},
     * subsequent {@link #drain()} calls are no-ops regardless of whether every
     * completion ran.
     */
    private boolean drained;

    /**
     * Construct a new {@link AbstractReader}.
     *
     * @param concourse the {@link Concourse} connection; must not be
     *            {@code null}
     */
    protected AbstractReader(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
        this.completions = new ArrayList<>();
        this.drained = false;
    }

    @Override
    public final Concourse concourse() {
        return concourse;
    }

    @Override
    public final void onDrain(Runnable completion) {
        completions.add(Preconditions.checkNotNull(completion));
    }

    @Override
    public final void drain() {
        if(drained) {
            return;
        }
        prepareDrain();
        drained = true;
        try {
            for (Runnable completion : completions) {
                completion.run();
            }
        }
        finally {
            completions.clear();
        }
    }

    /**
     * Issue any deferred work (e.g., submit a batched
     * {@link com.cinchapi.concourse.lang.CommandGroup CommandGroup}) before
     * {@link #onDrain registered completions} run.
     */
    protected abstract void prepareDrain();

}
