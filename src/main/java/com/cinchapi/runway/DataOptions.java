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
package com.cinchapi.runway;

import jdk.nashorn.internal.ir.annotations.Immutable;

/**
 * A parameter object that encapsulates data serialization options.
 */
@Immutable
public final class DataOptions {

     /**
      * A boolean that indicates if single element collections should be
      * flattened to a single value
      */
    private final boolean flattenSingleElementsCollections;

    /**
     * A boolean that indicates if null values should be serialized
     */
    private final boolean serializeNullValues;

    /**
     * Constructor
     *
     * @param flattenSingleElementsCollections
     * @param serializeNullValues
     */
    public DataOptions(boolean flattenSingleElementsCollections, boolean serializeNullValues) {
        this.flattenSingleElementsCollections = flattenSingleElementsCollections;
        this.serializeNullValues = serializeNullValues;
    }

    /**
     * Returns if single element collections should be
     * flattened to a single value
     * @return
     */
    public boolean flattenSingleElementsCollections() {
        return flattenSingleElementsCollections;
    }

    /**
     * Returns if null values should be serialized
     *
     * @return
     */
    public boolean serializeNullValues() {
        return  serializeNullValues;
    }
}
