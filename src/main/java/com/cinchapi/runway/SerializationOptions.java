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

import javax.annotation.concurrent.Immutable;

/**
 * A parameter object that encapsulates data serialization options.
 */
@Immutable
public final class SerializationOptions {

    /**
     * Return a builder to construct the preferred {@link SerializationOptions}.
     * 
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Return the default {@link SerializationOptions}.
     * 
     * @return default {@link SerializationOptions}.
     */
    public static SerializationOptions defaults() {
        return builder().build();
    }

    /**
     * A boolean that indicates if single element collections should be
     * flattened to a single value
     */
    private final boolean flattenSingleElementCollections;

    /**
     * A boolean that indicates if null values should be serialized
     */
    private final boolean serializeNullValues;

    /**
     * A boolean that indicates if {@link Computed @Computed} properties should
     * be materialized when a serialization call does not positively name them.
     */
    private final boolean includeComputedValuesByDefault;

    /**
     * Constructor
     *
     * @param flattenSingleElementCollections
     * @param serializeNullValues
     * @param includeComputedValuesByDefault
     */
    private SerializationOptions(boolean flattenSingleElementCollections,
            boolean serializeNullValues,
            boolean includeComputedValuesByDefault) {
        this.flattenSingleElementCollections = flattenSingleElementCollections;
        this.serializeNullValues = serializeNullValues;
        this.includeComputedValuesByDefault = includeComputedValuesByDefault;
    }

    /**
     * Returns if single element collections should be flattened to a single
     * value
     * 
     * @return boolean
     */
    public boolean flattenSingleElementCollections() {
        return flattenSingleElementCollections;
    }

    /**
     * Returns if null values should be serialized
     *
     * @return boolean
     */
    public boolean serializeNullValues() {
        return serializeNullValues;
    }

    /**
     * Return whether {@link Computed @Computed} properties are included in
     * serialized output when the caller does not positively name them.
     * <p>
     * The default is {@code false}, meaning {@code @Computed} properties are
     * excluded unless the caller positively names them or explicitly opts in by
     * setting this option to {@code true}.
     * </p>
     *
     * @return {@code true} if {@code @Computed} properties are included by
     *         default; {@code false} otherwise
     */
    public boolean includeComputedValuesByDefault() {
        return includeComputedValuesByDefault;
    }

    /**
     * Returned from {@link #builder()}.
     *
     * @author Jeff Nelson
     */
    public static class Builder {

        private boolean flattenSingleElementCollections = false;
        private boolean serializeNullValues = false;
        private boolean includeComputedValuesByDefault = false;

        /**
         * Construct a new instance.
         */
        private Builder() {}

        /**
         * Build the {@link SerializationOptions} with the parameters that were
         * provided to this builder.
         *
         * @return the {@link SerializationOptions}.
         */
        public SerializationOptions build() {
            return new SerializationOptions(flattenSingleElementCollections,
                    serializeNullValues, includeComputedValuesByDefault);
        }

        /**
         * Configure the option to flatten single element collections into a
         * scalar value.
         * 
         * @param flattenSingleElementCollections
         * @return this
         */
        public Builder flattenSingleElementCollections(
                boolean flattenSingleElementCollections) {
            this.flattenSingleElementCollections = flattenSingleElementCollections;
            return this;
        }

        /**
         * Configure the option to include null values within the data that is
         * returned (instead of dropping the key/value pair).
         *
         * @param serializeNullValues
         * @return this
         */
        public Builder serializeNullValues(boolean serializeNullValues) {
            this.serializeNullValues = serializeNullValues;
            return this;
        }

        /**
         * Configure whether {@link Computed @Computed} properties are included
         * in serialized output when the caller does not positively name them.
         * <p>
         * The default is {@code false}. Set this to {@code true} to include
         * {@code @Computed} properties without naming them explicitly.
         * </p>
         *
         * @param includeComputedValuesByDefault
         * @return this
         */
        public Builder includeComputedValuesByDefault(
                boolean includeComputedValuesByDefault) {
            this.includeComputedValuesByDefault = includeComputedValuesByDefault;
            return this;
        }
    }
}
