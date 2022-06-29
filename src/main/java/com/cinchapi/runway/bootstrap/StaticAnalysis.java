/*
 * Cinchapi Inc. CONFIDENTIAL
 * Copyright (c) 2022 Cinchapi Inc. All Rights Reserved.
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
package com.cinchapi.runway.bootstrap;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.util.Logging;
import com.cinchapi.runway.Record;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * A collection of static data about available {@link Record} types and their
 * fields to make {@link Runway} operations more efficient.
 *
 * @author Jeff Nelson
 */
public final class StaticAnalysis {

    /**
     * Return the {@link StaticAnalysis}.
     * <p>
     * NOTE: Scanning the classpath to perform static analysis adds startup
     * costs proportional to the number of classes defined, so it is only done
     * once to minimize the effect of the cost.
     * </p>
     * 
     * @return the {@link StaticAnalysis}
     */
    public static StaticAnalysis instance() {
        return INSTANCE;
    }

    static {
        Logging.disable(Reflections.class);
        Reflections.log = null; // turn off reflection logging
    }

    private static final StaticAnalysis INSTANCE = new StaticAnalysis();

    /**
     * A mapping from each {@link Record} class to its traversable paths.
     */
    private final Map<Class<? extends Record>, Set<String>> pathsByClass;

    /**
     * A mapping from each {@link Record} class to the traversable paths in its
     * hierarchy.
     */
    private final Map<Class<? extends Record>, Set<String>> pathsByClassHierarchy;

    /**
     * A mapping from each {@link Record} class to all of its descendants. This
     * facilitates querying across hierarchies.
     */
    private final Multimap<Class<? extends Record>, Class<?>> hierarchies;

    /**
     * A mapping from each {@link Record} class to each its non-internal keys,
     * each of which is mapped to the associated {@link Field} object.
     */
    private final Map<Class<? extends Record>, Map<String, Field>> fieldsByClass;

    /**
     * A mapping from each {@link Record} class to each of its non-internal
     * keys, each of which is mapped to a collection of type arguments
     * associated with that corresponding {@link Field} object.
     */
    private final Map<Class<? extends Record>, Map<String, Collection<Class<?>>>> fieldTypeArgumentsByClass;

    /**
     * Construct a new instance.
     */
    private StaticAnalysis() {
        this.hierarchies = HashMultimap.create();
        this.pathsByClass = new HashMap<>();
        this.pathsByClassHierarchy = new HashMap<>();
        this.fieldsByClass = new HashMap<>();
        this.fieldTypeArgumentsByClass = new HashMap<>();
        Reflections reflection = new Reflections(new SubTypesScanner());
        reflection.getSubTypesOf(Record.class).forEach(type -> {
            // Build class hierarchy
            hierarchies.put(type, type);
            reflection.getSubTypesOf(type)
                    .forEach(subType -> hierarchies.put(type, subType));

            // Get paths for class and class hierarchy
            pathsByClass.put(type, Record.getPaths(type));
            pathsByClassHierarchy.put(type, getPathsAcrossClassHierarchy(type));

            // Get fields and associated metadata
            Record.getFields(type).forEach(field -> {
                Map<String, Field> fields = fieldsByClass.computeIfAbsent(type,
                        $ -> new HashMap<>());
                Map<String, Collection<Class<?>>> fieldTypeArguments = fieldTypeArgumentsByClass
                        .computeIfAbsent(type, $ -> new HashMap<>());
                String key = field.getName();
                fields.put(key, field);
                fieldTypeArguments.put(key, Reflection.getTypeArguments(field));
            });
        });
    }

    /**
     * Return the hierarchy of {@code clazz}.
     * 
     * @param clazz
     * @return the hierarchy
     */
    public <T extends Record> Collection<Class<?>> getClassHierarchy(
            Class<T> clazz) {
        return hierarchies.get(clazz);
    }

    /**
     * Return the {@link Field} object for {@code key} in {@code clazz}.
     * 
     * @param clazz
     * @param key
     * @return the {@link Field} object
     */
    public <T extends Record> Field getField(Class<T> clazz, String key) {
        try {
            return fieldsByClass.get(clazz).get(key);
        }
        catch (NullPointerException e) {
            throw new IllegalArgumentException("Unknown Record type: " + clazz);
        }
    }

    /**
     * Return the {@link Field} object for {@code key} in the {@link Class} of
     * {@code record}.
     * 
     * @param record
     * @param key
     * @return the {@link Field} object
     */
    public <T extends Record> Field getField(T record, String key) {
        return getField(record.getClass(), key);
    }

    /**
     * Return the non-internal {@link Field} objects for {@code clazz}.
     * 
     * @param clazz
     * @return the {@link Fields}
     */
    public <T extends Record> Collection<Field> getFields(Class<T> clazz) {
        try {
            return fieldsByClass.get(clazz).values();
        }
        catch (NullPointerException e) {
            throw new IllegalArgumentException("Unknown Record type: " + clazz);
        }
    }

    /**
     * Return the non-internal {@link Field} objects for {@link Class} of
     * {@code record}.
     * 
     * @param record
     * @return the {@link Fields}
     */
    public <T extends Record> Collection<Field> getFields(T record) {
        return getFields(record.getClass());
    }

    /**
     * Return all the paths (e.g., navigable keys based on fields with linked
     * {@link Record} types) for {@code clazz}.
     * 
     * @param clazz
     * @return the paths
     */
    public <T extends Record> Set<String> getPathsForClass(Class<T> clazz) {
        return pathsByClass.get(clazz);
    }

    /**
     * Return all the paths (e.g., navigable keys based on fields with linked
     * {@link Record} types) for the entire hierarchy of {@code clazz}.
     * 
     * @param clazz
     * @return the paths
     */
    public Set<String> getPathsForClassHierarchy(
            Class<? extends Record> clazz) {
        return pathsByClassHierarchy.get(clazz);
    }

    /**
     * Return any defined type arguments for the field named {@code key} in
     * {@code clazz}.
     * 
     * @param clazz
     * @param key
     * @return the type arguments
     */
    public <T extends Record> Collection<Class<?>> getTypeArguments(
            Class<T> clazz, String key) {
        try {
            return fieldTypeArgumentsByClass.get(clazz).get(key);
        }
        catch (NullPointerException e) {
            throw new IllegalArgumentException("Unknown Record type: " + clazz);
        }
    }

    /**
     * Return any defined type arguments for the field named {@code key} in the
     * {@link Class} of {@code record}.
     * 
     * @param clazz
     * @param key
     * @return the type arguments
     */
    public <T extends Record> Collection<Class<?>> getTypeArguments(T record,
            String key) {
        return getTypeArguments(record.getClass(), key);
    }

    /**
     * Perform {@link Record#getPaths(Class)} for each {@link Class} in the
     * hierarchy of {@code clazz}.
     * 
     * @param clazz
     * @return all the paths in the class hierarchy
     */
    @SuppressWarnings("unchecked")
    private <T extends Record> Set<String> getPathsAcrossClassHierarchy(
            Class<T> clazz) {
        Set<String> paths = new LinkedHashSet<>();
        Collection<Class<?>> hiearchy = hierarchies.get(clazz);
        for (Class<?> type : hiearchy) {
            paths.addAll(Record.getPaths((Class<? extends Record>) type));
        }
        return paths;
    }

}
