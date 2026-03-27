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
 * A scoped attachment of {@link AdHocDataSource AdHocDataSources} to a
 * {@link Runway} instance.
 * <p>
 * An {@link AttachmentScope} is returned by
 * {@link Runway#attach(AdHocDataSource...)} and provides a
 * {@link DatabaseInterface} that delegates to the underlying {@link Runway}
 * while the attached sources are active. When {@link #close() closed}, all
 * attached sources are automatically detached.
 * </p>
 * <p>
 * This class implements {@link AutoCloseable} for use with try-with-resources:
 * </p>
 *
 * <pre>
 * {@code
 * try (AttachmentScope scope = runway.attach(source)) {
 *     scope.load(MyAdHocRecord.class); // Uses attached source
 * }
 * // Source automatically detached
 * }
 * </pre>
 *
 * @author Jeff Nelson
 */
public class AttachmentScope implements DatabaseInterface, AutoCloseable {

    /**
     * The underlying {@link Runway} instance.
     */
    private final Runway runway;

    /**
     * The attached sources, held for detachment on close.
     */
    private final AdHocDataSource<?>[] sources;

    /**
     * Flag indicating whether this scope has been closed.
     */
    private boolean closed = false;

    /**
     * Construct a new instance.
     *
     * @param runway the underlying {@link Runway}
     * @param sources the attached sources
     */
    AttachmentScope(Runway runway, AdHocDataSource<?>[] sources) {
        this.runway = runway;
        this.sources = sources;
    }

    @Override
    public void close() {
        if(!closed) {
            for (AdHocDataSource<?> source : sources) {
                runway.detach(source);
            }
            closed = true;
        }
    }

    @Override
    public Selections select(Selection<?>... selections) {
        return runway.select(selections);
    }

}
