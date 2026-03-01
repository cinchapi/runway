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
package com.cinchapi.runway.json;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * A function that produces a JSON string for a value. This function can be
 * associated with a type to determine how the type is JSON serialized in the
 * {@link Record#json()} method.
 *
 * @author Jeff Nelson
 */
@FunctionalInterface
@Deprecated
public interface JsonTypeWriter<T> {

    /**
     * Return the JSON string for {@code value}.
     * 
     * @param value
     * @return the JSON string for the {@code value}
     */
    public String jsonValue(T value);

    /**
     * Return a {@link TypeAdapter} that writes the {@link #jsonValue(Object)}
     * to a {@link JsonWriter}.
     * 
     * @return the type adapter
     */
    public default TypeAdapter<T> typeAdapter() {
        return new TypeAdapter<T>() {

            @Override
            public void write(JsonWriter out, T value) throws IOException {
                out.jsonValue(jsonValue(value));
            }

            @Override
            public T read(JsonReader in) throws IOException {
                throw new UnsupportedOperationException();
            }

        };

    }

}
