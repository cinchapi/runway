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

/**
 * A {@link DatabaseInterface data source} that can be dynamically
 * {@link Runway#attach(FederatedDataSource...) attached} to a {@link Runway}
 * instance to serve {@link Record Records} of a specific type.
 * <p>
 * When attached, queries for the source's {@link Record} type are routed to
 * this source instead of the underlying database.
 * </p>
 * <p>
 * This enables transparent federation where programmatic data sources can be
 * seamlessly integrated with persistent database records through a single
 * {@link Runway} interface.
 * </p>
 *
 * @author Jeff Nelson
 * @see Runway#attach(FederatedDataSource...)
 * @see Runway#detach(FederatedDataSource)
 * @see AdHocDataSource
 */
public interface FederatedDataSource<T extends Record> extends DatabaseInterface {

    /**
     * Return the {@link Record} {@link Class} served by this data source.
     * <p>
     * Queries for this class (or its superclasses, depending on the query
     * method) will be routed to this data source when attached to a
     * {@link Runway} instance.
     * </p>
     *
     * @return the {@link Record} class this source serves
     */
    Class<T> type();

}
