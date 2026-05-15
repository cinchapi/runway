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
 * {@link Concourse} connection. Provides the shared {@link #drain()} template
 * and the package-private {@link #register(Runnable)} hook that {@link Pending}
 * uses to schedule its own resolution.
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
     * Resolution work registered via {@link #register(Runnable)} and run by
     * {@link #drain()} in registration order.
     */
    private final List<Runnable> registered;

    /**
     * Construct a new {@link AbstractReader}.
     *
     * @param concourse the {@link Concourse} connection; must not be
     *            {@code null}
     */
    protected AbstractReader(Concourse concourse) {
        this.concourse = Preconditions.checkNotNull(concourse);
        this.registered = new ArrayList<>();
    }

    @Override
    public final Concourse concourse() {
        return concourse;
    }

    @Override
    public final void drain() {
        while (!registered.isEmpty()) {
            prepareDrain();
            List<Runnable> snapshot = new ArrayList<>(registered);
            registered.clear();
            for (Runnable work : snapshot) {
                work.run();
            }
        }
    }

    /**
     * Register {@code work} to run during {@link #drain()}, in registration
     * order. Work registered while {@link #drain()} is iterating runs in a
     * subsequent pass after any reads it records have been issued.
     *
     * @param work the resolution work to run
     */
    final void register(Runnable work) {
        registered.add(Preconditions.checkNotNull(work));
    }

    /**
     * Issue any deferred work (e.g., submit a batched
     * {@link com.cinchapi.concourse.lang.CommandGroup CommandGroup}) before the
     * next wave of {@link #register registered} resolutions run.
     */
    protected abstract void prepareDrain();

}
