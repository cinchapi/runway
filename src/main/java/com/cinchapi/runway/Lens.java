/*
 * Cinchapi Inc. CONFIDENTIAL
 * Copyright (c) 2023 Cinchapi Inc. All Rights Reserved.
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

import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 *
 *
 * @author jeff
 */
public interface Lens {
    
    public static Set<String> ALL_KEYS = ImmutableSet.of();
    
    public static Set<String> NO_KEYS = null;
    
    public default Set<String> $reads(Record record) {
        return NO_KEYS;
    }
    
    public default Set<String> $writes(Record record) {
        return NO_KEYS;
    }
    
    public default boolean $captures(Record record) {
        return false;
    }
    
    public default boolean $deletes(Record record) {
        return false;
    }

    /*
     * Record methods
     * 
     * - frame(lens, keys) or map(lens, keys)
     * - set(lens, data)
     * - set (lens, key, value)
     * - deleteOnSave(lens)
     * - get(lens, key)
     */
}
