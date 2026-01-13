/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway;

import java.util.function.Supplier;

/**
 * A read-only {@link Record} that is not persisted to the database.
 * <p>
 * An {@link AdHocRecord} is intended for temporary, non-persistent data
 * structures that need to be compatible with the framework's data access
 * patterns. This is useful for generating report-like structures, aggregated
 * data views, or other read-only data representations.
 * </p>
 * <p>
 * Subclasses define their schema through fields, just like regular
 * {@link Record Records}. However, attempts to persist or modify an
 * {@link AdHocRecord} will fail.
 * </p>
 * <p>
 * {@link AdHocRecord AdHocRecords} are typically served through an
 * {@link AdHocDatabase} and can be federated with a persistent data source via
 * {@link FederatedRunway}.
 * </p>
 *
 * @author Jeff Nelson
 */
public abstract class AdHocRecord extends Record {

    /**
     * A dummy field to work around a Runway static analysis limitation that
     * requires at least one field to be defined.
     */
    @SuppressWarnings("unused")
    private transient Object $$_$$;

    @Override
    public final void deleteOnSave() {
        throw new UnsupportedOperationException(
                "AdHocRecord cannot be deleted");
    }

    @Override
    public final void set(String key, Object value) {
        throw new UnsupportedOperationException("AdHocRecord is read-only");
    }

    @Override
    protected final Supplier<Boolean> overrideSave() {
        return () -> true;
    }

}
