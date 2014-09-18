package org.cinchapi.runway.util;

import java.util.Collection;

import com.google.common.base.Strings;

/**
 * A collection of utilites for operation on Objects with respect for their
 * runtime types even though they may not be known at compile time.
 * 
 * @author jnelson
 */
public final class AnyObject {

    /**
     * Return {@code true} if {@code value} is {@code null} or considered empty.
     * A value is considered empty if:
     * <ul>
     * <li>It is a collection with no members</li>
     * <li>It is an array with a length of 0</li>
     * <li>It is a string with no characters</li>
     * </ul>
     * 
     * @param value
     * @return {@code true} if the object is considered null or empty
     */
    public static boolean isNullOrEmpty(Object value) {
        return value == null
                || (value instanceof Collection && ((Collection<?>) value)
                        .isEmpty())
                || (value.getClass().isArray() && ((Object[]) value).length == 0)
                || (value instanceof String && Strings
                        .isNullOrEmpty((String) value));
    }

    private AnyObject() {/* noop */}

}
