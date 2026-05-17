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
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.google.common.base.Preconditions;

/**
 * Base class for {@link Reader} implementations. Owns the {@link Concourse}
 * connection lifecycle behind {@link #concourse()} and {@link #close()}, the
 * shared {@link #drain()} template, and the package-private
 * {@link #register(Runnable)} hook that {@link Pending} uses to schedule its
 * own resolution.
 *
 * @author Jeff Nelson
 */
public abstract class AbstractReader implements Reader {

    /**
     * Yields the {@link Concourse} connection that backs this {@link Reader}.
     */
    private final Supplier<Concourse> acquirer;

    /**
     * Receives the {@link Concourse} connection produced by {@link #acquirer}
     * for cleanup.
     */
    private final Consumer<Concourse> releaser;

    /**
     * Resolution work registered via {@link #register(Runnable)} and run by
     * {@link #drain()} in registration order.
     */
    private final List<Runnable> registered;

    /**
     * The {@link Concourse} connection that backs this {@link Reader}, or
     * {@code null} when none has been acquired.
     */
    private Concourse concourse;

    /**
     * Whether {@link #close()} has run.
     */
    private boolean closed;

    /**
     * Construct an {@link AbstractReader} that borrows a {@link Concourse}
     * connection from {@code pool} and returns it to {@code pool} on
     * {@link #close()}.
     *
     * @param pool the {@link ConnectionPool} that owns the {@link Concourse}
     *            connection; must not be {@code null}
     */
    protected AbstractReader(ConnectionPool pool) {
        this(Preconditions.checkNotNull(pool)::request, pool::release);
    }

    /**
     * Construct an {@link AbstractReader} that uses {@code connection}
     * directly. The caller retains ownership of the connection lifecycle;
     * {@link #close()} does <strong>not</strong> close it.
     *
     * @param connection the {@link Concourse} connection to use; must not be
     *            {@code null}
     */
    protected AbstractReader(Concourse connection) {
        this(() -> Preconditions.checkNotNull(connection),
                c -> {/* caller owns */});
    }

    /**
     * Construct an {@link AbstractReader} backed by a custom acquire/release
     * pair.
     *
     * @param acquirer yields the {@link Concourse} connection
     * @param releaser receives the connection when one was acquired and
     *            {@link #close()} runs
     */
    private AbstractReader(Supplier<Concourse> acquirer,
            Consumer<Concourse> releaser) {
        this.acquirer = acquirer;
        this.releaser = releaser;
        this.registered = new ArrayList<>();
        this.concourse = null;
        this.closed = false;
    }

    @Override
    public final Concourse concourse() {
        if(concourse == null) {
            Preconditions.checkState(!closed,
                    "Reader has been closed; no new connection can be "
                            + "acquired");
            concourse = Preconditions.checkNotNull(acquirer.get(),
                    "Acquirer returned a null Concourse connection");
        }
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

    @Override
    public final void close() {
        if(!closed) {
            closed = true;
            if(concourse != null) {
                releaser.accept(concourse);
                concourse = null;
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
