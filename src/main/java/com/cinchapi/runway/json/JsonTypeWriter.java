/*
 * Cinchapi Inc. CONFIDENTIAL
 * Copyright (c) 2018 Cinchapi Inc. All Rights Reserved.
 *
 * All information contained herein is, and remains the property of Cinchapi.
 * The intellectual and technical concepts contained herein are proprietary to
 * Cinchapi and may be covered by U.S. and Foreign Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained from Cinchapi. Access to the source code
 * contained herein is hereby forbidden to anyone except current Cinchapi
 * employees, managers or contractors who have executed Confidentiality and
 * Non-disclosure agreements explicitly covering such access.
 *
 * The copyright notice above does not evidence any actual or intended
 * publication or disclosure of this source code, which includes information
 * that is confidential and/or proprietary, and is a trade secret, of Cinchapi.
 *
 * ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC PERFORMANCE, OR PUBLIC
 * DISPLAY OF OR THROUGH USE OF THIS SOURCE CODE WITHOUT THE EXPRESS WRITTEN
 * CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
 * LAWS AND INTERNATIONAL TREATIES. THE RECEIPT OR POSSESSION OF THIS SOURCE
 * CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS TO
 * REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR
 * SELL ANYTHING THAT IT MAY DESCRIBE, IN WHOLE OR IN PART.
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
