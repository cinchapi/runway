/*
 * Copyright (c) 2013-2022 Cinchapi Inc.
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
package com.cinchapi.runway.bootstrap;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
     * {@link #hierarchies hierarchy}.
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
     * A collection containing each {@link Record} class that has at least one
     * field whose type is a subclass of {@link Record}.
     */
    private final Set<Class<? extends Record>> hasRecordFieldTypeByClass;

    /**
     * A collection containing each {@link Record} class that itself or is the
     * ancestors of a {@link Record} class that has at least one field whose
     * type is a subclass of {@link Record}.
     */
    private final Set<Class<? extends Record>> hasRecordFieldTypeByClassHierarchy;

    /**
     * Construct a new instance.
     */
    private StaticAnalysis() {
        this.hierarchies = HashMultimap.create();
        this.pathsByClass = new HashMap<>();
        this.pathsByClassHierarchy = new HashMap<>();
        this.fieldsByClass = new HashMap<>();
        this.fieldTypeArgumentsByClass = new HashMap<>();
        this.hasRecordFieldTypeByClass = new HashSet<>();
        this.hasRecordFieldTypeByClassHierarchy = new HashSet<>();
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
                if(Record.class.isAssignableFrom(field.getType())) {
                    hasRecordFieldTypeByClass.add(type);
                }
            });
        });
        this.hierarchies.forEach((type, relative) -> {
            if(hasRecordFieldTypeByClass.contains(relative)) {
                hasRecordFieldTypeByClassHierarchy.add(type);
            }
        });
    }

    /**
     * Return the descendants of {@code clazz}.
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
     * {@link Record} types) for {@code clazz} and all of its descendents.
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
     * Return {@code true} if {@code clazz} has any fields whose type is a
     * subclass of {@link Record}.
     * 
     * @param clazz
     * @return {@code true} if {@code clazz} has any {@link Record} type fields
     */
    public boolean hasFieldOfTypeRecordInClass(Class<? extends Record> clazz) {
        return hasRecordFieldTypeByClass.contains(clazz);
    }

    /**
     * Return {@code true} if {@code clazz}, or any of its descendants, have any
     * fields whose type is a subclass of {@link Record}.
     * 
     * @param clazz
     * @return {@code true} if {@code clazz}, or any of its children, have any
     *         {@link Record} type fields
     */
    public boolean hasFieldOfTypeRecordInClassHierarchy(
            Class<? extends Record> clazz) {
        return hasRecordFieldTypeByClassHierarchy.contains(clazz);
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
