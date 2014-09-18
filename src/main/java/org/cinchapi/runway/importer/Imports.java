package org.cinchapi.runway.importer;

import java.util.Collection;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

/**
 * Utilities for dealing with imported data.
 * 
 * @author jnelson
 */
public class Imports {

    /**
     * A convenience function to get the single element of type T mapped from
     * {@code key} in the {@code data} map.
     * 
     * @param key
     * @param data
     * @return the single value
     */
    @SuppressWarnings("unchecked")
    public static <T> T get(String key, Multimap<String, Object> data) {
        return (T) Iterables.getOnlyElement(data.get(key));
    }

    /**
     * A convenience function to get all the elements of type T mapped from
     * {@code key} in the {@code data} map.
     * 
     * @param key
     * @param data
     * @return all the values
     */
    @SuppressWarnings("unchecked")
    public static <T> Collection<T> getAll(String key,
            Multimap<String, Object> data) {
        return (Collection<T>) data.get(key);
    }

}
