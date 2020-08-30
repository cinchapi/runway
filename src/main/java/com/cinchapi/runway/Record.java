/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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

import gnu.trove.map.TLongObjectMap;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;

import com.cinchapi.ccl.Parser;
import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.Array;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.collect.AnyMaps;
import com.cinchapi.common.collect.Association;
import com.cinchapi.common.collect.MergeStrategies;
import com.cinchapi.common.collect.Sequences;
import com.cinchapi.common.collect.lazy.LazyTransformSet;
import com.cinchapi.common.describe.Empty;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.BuildableState;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.concourse.server.io.Serializables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Parsers;
import com.cinchapi.concourse.util.TypeAdapters;
import com.cinchapi.concourse.validate.Keys;
import com.cinchapi.runway.json.JsonTypeWriter;
import com.cinchapi.runway.util.ComputedEntry;
import com.cinchapi.runway.util.BackupReadSourcesHashMap;
import com.cinchapi.runway.validation.Validator;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * A {@link Record} is a a wrapper around the same in {@link Concourse} that
 * facilitates object-oriented interaction while automatically preserving
 * transactional security.
 * <p>
 * Each subclass defines its "schema" through {@code non-transient} member
 * variables. When a Record is loaded from Concourse, the member variables are
 * automatically populated with information from the database. And, when a
 * Record is {@link #save() saved}, the values of those variables (including any
 * changes) will automatically be stored/updated in Concourse.
 * </p>
 * <h2>Variable Modifiers</h2>
 * <p>
 * Records will respect the native java modifiers placed on variables. This
 * means that transient fields are never stored or loaded from the database.
 * And, private fields are never included in a {@link #json() json} dump or
 * {@link #toString()} output.
 * </p>
 * 
 * @author Jeff Nelson
 */
@SuppressWarnings({ "restriction", "deprecation" })
public abstract class Record implements Comparable<Record> {

    /**
     * Return {@code true} if the matching {@code condition} for data that is
     * part of the specified {@code clazz} can be resolved entirely by the
     * database, or must be done so locally because it touches {@link #derived()
     * derived} or {@link #computed() computed} data.
     * 
     * @param <T>
     * @param clazz
     * @param condition
     * @return a boolean that indicates if the {@code condition} can be resolved
     *         by the database or not
     */
    public static <T extends Record> boolean isDatabaseResolvableCondition(
            Class<T> clazz, Criteria condition) {
        Set<String> intrinsic = INTRINSIC_PROPERTY_CACHE.computeIfAbsent(clazz,
                c -> LazyTransformSet.of(getFields(c), Field::getName));
        Parser parser = Parsers.create(condition);
        for (String key : parser.analyze().keys()) {
            if(!Keys.isNavigationKey(key) && !intrinsic.contains(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * INTERNAL method to load a {@link Record} from {@code clazz} identified by
     * {@code id}.
     * 
     * @param clazz
     * @param id
     * @param existing
     * @param connections
     * @return the loaded Record
     */
    protected static <T extends Record> T load(Class<?> clazz, long id,
            TLongObjectMap<Record> existing, ConnectionPool connections,
            Runway runway, @Nullable Map<String, Set<Object>> data) {
        Concourse concourse = connections.request();
        try {
            return load(clazz, id, existing, connections, concourse, runway,
                    data);
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Serialize {@code value} by converting it to an object that can be stored
     * within the database. This method assumes that {@code value} is a scalar
     * (e.g. not a {@link Sequences#isSequence(Object)}).
     * 
     * @param value
     * @return the serialized value
     */
    @SuppressWarnings("rawtypes")
    private static Object serialize(Object value) {
        // NOTE: This logic mirrors storage logic in the #store method. But,
        // since the #store method has some optimizations, it doesn't call into
        // this method directly. So, if modifications are made to this method,
        // please make similar and appropriate modifications to #store.
        Preconditions.checkArgument(!Sequences.isSequence(value));
        Preconditions.checkNotNull(value);
        if(value instanceof Record) {
            return Link.to(((Record) value).id());
        }
        else if(value instanceof DeferredReference) {
            return serialize(((DeferredReference) value).get());
        }
        else if(value.getClass().isPrimitive() || value instanceof String
                || value instanceof Tag || value instanceof Link
                || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double
                || value instanceof Boolean || value instanceof Timestamp) {
            return value;
        }
        else if(value instanceof Enum) {
            return Tag.create(((Enum) value).name());
        }
        else if(value instanceof Serializable) {
            ByteBuffer bytes = Serializables.getBytes((Serializable) value);
            Tag base64 = Tag.create(BaseEncoding.base64Url()
                    .encode(ByteBuffers.toByteArray(bytes)));
            return base64;
        }
        else {
            Gson gson = new Gson();
            Tag json = Tag.create(gson.toJson(value));
            return json;
        }
    }

    /**
     * Dereference the {@code value} stored for {@code field} if it is a
     * {@link DeferredReference} or a {@link Sequence} of them.
     * 
     * @param field
     * @param value
     * @return the dereferenced value if it can be dereferenced or the original
     *         input
     */
    private static Object dereference(Field field, Object value) {
        if(value == null) {
            return value;
        }
        else if(value instanceof DeferredReference) {
            value = ((DeferredReference<?>) value).get();
        }
        else if(Sequences.isSequence(value)) {
            Collection<Class<?>> typeArgs = Reflection.getTypeArguments(field);
            if(typeArgs.contains(DeferredReference.class)
                    || typeArgs.contains(Object.class)) {
                value = Sequences.stream(value)
                        .map(item -> dereference(field, item))
                        .collect(Collectors.toCollection(
                                value instanceof Set ? LinkedHashSet::new
                                        : ArrayList::new));
            }
        }
        return value;
    }

    /**
     * Return a {link TypeAdapterFactory} for {@link Record} types that keeps
     * track of linked records to prevent infinite recursion.
     * 
     * @param options
     * @param from the {@link Record} that is the parent link for any
     *            encountered links
     * @param links a {@link Multimap} that represents encountered links as a
     *            mapping from the <strong>destination</strong> record to any
     *            source record that links to it (e.g. the destinations are
     *            indexed to make it easy to look up all the parent nodes in the
     *            document graph)
     * @return the {@link TypeAdapterFactory}
     */
    private static TypeAdapterFactory generateDynamicRecordTypeAdapterFactory(
            SerializationOptions options, Record from,
            Multimap<Record, Record> links) {
        return new TypeAdapterFactory() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                if(Record.class.isAssignableFrom(type.getRawType())) {
                    return (TypeAdapter<T>) new TypeAdapter<Record>() {

                        @Override
                        public Record read(JsonReader in) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void write(JsonWriter out, Record value)
                                throws IOException {
                            if(!isCyclic(from, value)) {
                                links.put(value, from);
                                out.jsonValue(value.json(options, links));
                            }
                            else {
                                out.value(value.id() + " (recursive link)");
                            }
                        }
                    }.nullSafe();
                }
                else {
                    return null;
                }
            }

            /**
             * Return {@code true} of a link {@code from} one {@link Record}
             * {@code to} another one creates a cycle.
             * 
             * @param from
             * @param to
             * @return a boolean that indicates whether the link between two
             *         records would form a cycle
             */
            private boolean isCyclic(Record from, Record to) {
                Collection<Record> grandparents = links.get(from);
                if(grandparents.isEmpty()) {
                    return false;
                }
                else {
                    if(grandparents.contains(to)) {
                        return true;
                    }
                    else {
                        for (Record grandparent : grandparents) {
                            if(isCyclic(grandparent, to)) {
                                return true;
                            }
                        }
                        return false;
                    }
                }
            }

        };
    }

    /**
     * Return all the non-internal {@link Field fields} in this {@code clazz}.
     * 
     * @param clazz
     * @return the non-internal {@link Field fields}
     */
    private static Set<Field> getFields(Class<? extends Record> clazz) {
        return Arrays.asList(Reflection.getAllDeclaredFields(clazz)).stream()
                .filter(field -> !INTERNAL_FIELDS.contains(field))
                .collect(Collectors.toSet());
    }

    /**
     * Return {@code true} if the record identified by {@code id} is in a
     * "zombie" state meaning it exists in the database without any actual data.
     * 
     * @param id
     * @param concourse
     * @return {@code true} if the record is a zombie
     */
    private static boolean inZombieState(long id, Concourse concourse,
            @Nullable Map<String, Set<Object>> data) {
        Set<String> keys = data != null ? data.keySet()
                : concourse.describe(id);
        return keys.equals(ZOMBIE_DESCRIPTION);
    }

    /**
     * Return {@code true} if the {@code field} is considered readable.
     * 
     * @param field
     * @return a boolean that expresses the readability of the field
     */
    private static boolean isReadableField(Field field) {
        return (!Modifier.isPrivate(field.getModifiers())
                || field.isAnnotationPresent(Readable.class))
                && !Modifier.isTransient(field.getModifiers());
    }

    /**
     * INTERNAL method to load a {@link Record} from {@code clazz} identified by
     * {@code id}.
     * 
     * @param clazz
     * @param id
     * @param existing
     * @param connections
     * @param concourse
     * @return the loaded Record
     */
    @SuppressWarnings("unchecked")
    private static <T extends Record> T load(Class<?> clazz, long id,
            TLongObjectMap<Record> existing, ConnectionPool connections,
            Concourse concourse, Runway runway,
            @Nullable Map<String, Set<Object>> data) {
        T record = (T) newDefaultInstance(clazz, connections);
        Reflection.set("id", id, record); /* (authorized) */
        record.assign(runway);
        record.load(concourse, existing, data);
        record.onLoad();
        return record;
    }

    /**
     * Get a new instance of {@code clazz} by calling the default (zero-arg)
     * constructor, if it exists. This method attempts to correctly invoke
     * constructors for nested inner classes.
     * 
     * @param clazz
     * @return the instance of the {@code clazz}.
     */
    @SuppressWarnings("unchecked")
    private static <T> T newDefaultInstance(Class<T> clazz,
            ConnectionPool connections) {
        try {
            // Use Unsafe to allocate the instance and reflectively set all the
            // default fields defined herewithin so there's no requirement for
            // implementing classes to contain a no-arg constructor.
            T instance = (T) unsafe.allocateInstance(clazz);
            Reflection.set("__", clazz.getName(), instance);
            Reflection.set("deleted", false, instance);
            Reflection.set("dynamicData", Maps.newHashMap(), instance);
            Reflection.set("errors", Lists.newArrayList(), instance);
            Reflection.set("id", NULL_ID, instance);
            Reflection.set("inViolation", false, instance);
            Reflection.set("connections", connections, instance);
            Reflection.set("db",
                    new ReactiveDatabaseInterface((Record) instance), instance);
            return instance;
        }
        catch (InstantiationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /* package */ static Runway PINNED_RUNWAY_INSTANCE = null;

    /**
     * The key used to hold the section metadata.
     */
    /* package */ static final String SECTION_KEY = "_"; // just want a
                                                         // simple/short key
                                                         // name that is likely
                                                         // to avoid collisions

    /**
     * The prefix applied to a key provided to the
     * {@link #compareTo(Record, String)} methods when it is desirable to
     * compare the values stored under that key in ascending (i.e. normal)
     * order.
     */
    static final String SORT_DIRECTION_ASCENDING_PREFIX = ">";

    /**
     * The prefix applied to a key provided to the
     * {@link #compareTo(Record, String)} methods when it is desirable to
     * compare the values stored under that key in descending (i.e. reverse)
     * order.
     */
    static final String SORT_DIRECTION_DESCENDING_PREFIX = "<";

    /**
     * The {@link Field fields} that are defined in the base class.
     */
    private static Set<Field> INTERNAL_FIELDS = Sets.newHashSet(
            Arrays.asList(Reflection.getAllDeclaredFields(Record.class)));

    /**
     * A cache of the {@link #intrinsic() properties} for each {@link Class}.
     */
    private static Map<Class<? extends Record>, Set<String>> INTRINSIC_PROPERTY_CACHE = Maps
            .newHashMap();

    /**
     * A placeholder id used to indicate that a record has been deleted or
     * doesn't actually exist in the database.
     */
    private static long NULL_ID = -1;

    /**
     * The coefficient multiplied by the result of a comparison to push the
     * sorting in the ascending direction.
     */
    private static final int SORT_DIRECTION_ASCENDING_COEFFICIENT = 1;

    /**
     * The coefficient multiplied by the result of a comparison to push the
     * sorting in the descending direction.
     */
    private static final int SORT_DIRECTION_DESCENDING_COEFFICIENT = -1;

    /**
     * Instance of {@link sun.misc.Unsafe} to use for hacky operations.
     */
    private static final sun.misc.Unsafe unsafe = Reflection
            .getStatic("theUnsafe", sun.misc.Unsafe.class);

    /**
     * The description of a record that is considered to be in "zombie" state.
     */
    private static final Set<String> ZOMBIE_DESCRIPTION = Sets
            .newHashSet(SECTION_KEY);

    /**
     * The {@link DatabaseInterface} that can be used to make queries within the
     * database from which this {@link Record} is sourced.
     */
    protected final transient DatabaseInterface db = new ReactiveDatabaseInterface(
            this);

    /**
     * A log of any suppressed errors related to this Record. A concatenation of
     * these errors can be thrown at anytime from the
     * {@link #throwSupressedExceptions()} method.
     */
    /* package */ transient List<Throwable> errors = Lists.newArrayList();

    /**
     * The variable that holds the name of the section in the database where
     * this record is stored.
     */
    private transient String __ = getClass().getName();

    /**
     * The {@link Concourse} database in which this {@link Record} is stored.
     */
    private transient ConnectionPool connections = null;

    /**
     * A flag that indicates if the record has been deleted using the
     * {@link #deleteOnSave()} method.
     */
    private transient boolean deleted = false;

    /**
     * Data that is dynamically added using the {@link #set(String, Object)}
     * method.
     */
    private transient final Map<String, Object> dynamicData = Maps.newHashMap();

    /**
     * The primary key that is used to identify this Record in the database.
     */
    private transient long id = NULL_ID;

    /**
     * A flag that indicates this Record is in violation of some constraint and
     * therefore cannot be used without ruining the integrity of the database.
     */
    private transient boolean inViolation = false;

    /**
     * The {@link Runway} instance that has been {@link #assign(Runway)
     * assigned} to this {@link Record}.
     */
    private transient Runway runway = null;

    /**
     * Construct a new instance.
     */
    public Record() {
        this.id = Time.now();
        if(PINNED_RUNWAY_INSTANCE != null) {
            this.connections = PINNED_RUNWAY_INSTANCE.connections;
            this.runway = PINNED_RUNWAY_INSTANCE;
        }
    }

    /**
     * Assign this {@link Record} to a the specified {@code runway} instance.
     * <p>
     * Each {@link Record} must know the {@link Runway} instance in which it is
     * stored. Explicit assignment via this method is only required if there are
     * more than one {@link Runway} instances in the application. Otherwise, the
     * single {@link Runway} instance is auto assigned to all Records when they
     * are created and loaded.
     * </p>
     * 
     * @param runway the {@link Runway} instance where this {@link Record} is
     *            stored.
     */
    public void assign(Runway runway) {
        this.runway = runway;
        this.connections = runway.connections;
    }

    @Override
    public int compareTo(Record record) {
        return compareTo(record, ImmutableMap.of());
    }

    /**
     * Compare this object to another {@code record} and sort based on the
     * {@code order} specification.
     * <p>
     * A sort key can be prepended with {@code <} to indicate that
     * the values for that key should be sorted in ascending (e.g. natural)
     * order. Alternatively, a sort key can be prepended with {@code >} to
     * indicate that the values for that key should be sorted in descending
     * (i.e. reverse) order. If a sort key has no prefix, it is sorted in
     * ascending order.
     * </p>
     * 
     * @param record the object to be compared
     * @param order a list of sort keys
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the specified object.
     */
    public int compareTo(Record record, List<String> order) {
        Map<String, Integer> $order = order.stream().map(key -> {
            int coefficient;
            if(key.startsWith(SORT_DIRECTION_DESCENDING_PREFIX)) {
                coefficient = SORT_DIRECTION_DESCENDING_COEFFICIENT;
                key = key.substring(1);
            }
            else if(key.startsWith(SORT_DIRECTION_ASCENDING_PREFIX)) {
                coefficient = SORT_DIRECTION_ASCENDING_COEFFICIENT;
                key = key.substring(1);
            }
            else {
                coefficient = SORT_DIRECTION_ASCENDING_COEFFICIENT;
            }
            return new AbstractMap.SimpleImmutableEntry<String, Integer>(key,
                    coefficient);
        }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return compareTo(record, $order);
    }

    /**
     * Compare this object to another {@code record} and sort based on the
     * {@code order} specification.
     * <p>
     * A sort key can be prepended with {@code <} to indicate that
     * the values for that key should be sorted in ascending (e.g. natural)
     * order. Alternatively, a sort key can be prepended with {@code >} to
     * indicate that the values for that key should be sorted in descending
     * (i.e. reverse) order. If a sort key has no prefix, it is sorted in
     * ascending order.
     * </p>
     * 
     * @param record the object to be compared
     * @param order a space separated string containing sort keys
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the specified object.
     */
    public int compareTo(Record record, String order) {
        List<String> $order = Arrays
                .stream(order.replaceAll(",", " ").split("\\s"))
                .map(String::trim).collect(Collectors.toList());
        return compareTo(record, $order);
    }

    /**
     * Delete this {@link Record} from Concourse when the {@link #save()} method
     * is called.
     */
    public void deleteOnSave() {
        deleted = true;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Record) {
            return id == ((Record) obj).id;
        }
        else {
            return false;
        }
    }

    /**
     * Retrieve a dynamic value.
     * 
     * @param key the key name
     * @return the dynamic value
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        if(key.equalsIgnoreCase("id")) {
            return (T) new Long(id);
        }
        else {
            String[] stops = key.split("\\.");
            if(stops.length == 1) {
                Object value = dynamicData.get(key);
                if(value == null) {
                    try {
                        Field field = Reflection.getDeclaredField(key, this);
                        if(isReadableField(field)) {
                            value = field.get(this);
                            value = dereference(field, value);
                        }
                    }
                    catch (Exception e) {/* ignore */}
                }
                if(value == null) {
                    value = derived().get(key);
                }
                if(value == null) {
                    Supplier<?> computer = computed().get(key);
                    if(computer != null) {
                        value = computer.get();
                    }
                }
                return (T) value;
            }
            else {
                // The presented key is a navigation key, so incrementally
                // traverse the document graph.
                String stop = stops[0];
                Object destination = get(stop);
                String path = StringUtils.join(stops, '.', 1, stops.length);
                if(destination instanceof Record) {
                    return (T) ((Record) destination).get(path);
                }
                else if(Sequences.isSequence(destination)) {
                    Collection<Object> seq = destination instanceof Set
                            ? Sets.newLinkedHashSet()
                            : Lists.newArrayList();
                    Sequences.forEach(destination, item -> {
                        if(item instanceof Record) {
                            Object next = ((Record) item).get(path);
                            seq.add(next);
                        }
                    });
                    return !seq.isEmpty() ? (T) seq : null;
                }
                else {
                    return null;
                }
            }
        }
    }

    /**
     * Return a map that contains all of the data for the readable {@code keys}
     * in this {@link Record}.
     * <p>
     * If you want to return all the readable data, use the {@link #map()}
     * method.
     * </p>
     * 
     * @return the data in this record
     * @deprecated use {@link #map(String...)} instead
     */
    @Deprecated
    public Map<String, Object> get(String... keys) {
        return Arrays.asList(keys).stream()
                .collect(Collectors.toMap(Function.identity(), this::get));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Return the {@link #id} that uniquely identifies this record.
     * 
     * @return the id
     */
    public final long id() {
        return id;
    }

    /**
     * Return the "readable" intrinsic (e.g. not {@link #derived() or
     * {@link #computed()}) data from this {@link Record} as a {@link Map}.
     * <p>
     * This method be used over {@link #map()} when it is necessary to ensure
     * that {@link #computed() computed} values aren't processed and it isn't
     * feasible to explicitly filter them all out.
     * </p>
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     * 
     * @return the intrinsic data record
     */
    public Map<String, Object> intrinsic() {
        return intrinsic(Array.containing());
    }

    /**
     * Return the "readable" intrinsic (e.g. not {@link #derived() or
     * {@link #computed()}) data from this {@link Record} as a {@link Map}.
     * <p>
     * This method be used over {@link #map()} when it is necessary to ensure
     * that {@link #computed() computed} values aren't processed and it isn't
     * feasible to explicitly filter them all out.
     * </p>
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     * 
     * @param keys
     * @return the intrinsic record data
     */
    public Map<String, Object> intrinsic(String... keys) {
        Set<String> intrinsic = fields().stream().map(Field::getName)
                .collect(Collectors.toSet());
        keys = Arrays.stream(keys)
                .filter(key -> key.startsWith("-")
                        ? intrinsic.contains(key.substring(1))
                        : intrinsic.contains(key))
                .toArray(String[]::new);
        keys = keys.length == 0 ? intrinsic.toArray(Array.containing()) : keys;
        return map(keys);
    }

    /**
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data.
     * 
     * @return json string
     */
    public String json() {
        return json(SerializationOptions.defaults());
    }

    /**
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data.
     * 
     * @param flattenSingleElementCollections
     * @return json string
     * @deprecated use {@link #json(SerializationOptions) instead}
     */
    @Deprecated
    public String json(boolean flattenSingleElementCollections) {
        return json(
                SerializationOptions.builder()
                        .flattenSingleElementCollections(
                                flattenSingleElementCollections)
                        .build(),
                HashMultimap.create());
    }

    /**
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data from the specified {@code keys}.
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     * 
     * @param flattenSingleElementCollections
     * @param keys
     * @return json string
     * @deprecated use {@link #json(SerializationOptions, String...)} instead
     */
    @Deprecated
    public String json(boolean flattenSingleElementCollections,
            String... keys) {
        return json(
                SerializationOptions.builder()
                        .flattenSingleElementCollections(
                                flattenSingleElementCollections)
                        .build(),
                HashMultimap.create(), keys);
    }

    /**
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data.
     *
     * @param options
     * @return json string
     */
    public String json(SerializationOptions options) {
        return json(options, HashMultimap.create());
    }

    /**
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data from the specified {@code keys}.
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     *
     * @param options
     * @param keys
     * @return json string
     */
    public String json(SerializationOptions options, String... keys) {
        return json(options, HashMultimap.create(), keys);
    }

    /**
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data from the specified {@code keys}.
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     * 
     * @param keys
     * @return json string
     */
    public String json(String... keys) {
        return json(SerializationOptions.defaults(), keys);
    };

    /**
     * Return a map that contains "readable" data from this {@link Record}.
     * <p>
     * If no {@code keys} are provided, all the readable data will be
     * returned.
     * </p>
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     * 
     * @return the data in this record
     */
    public Map<String, Object> map() {
        return map(Array.containing());
    }

    /**
     * Return a map that contains "readable" data from this {@link Record}.
     * <p>
     * If no {@code keys} are provided, all the readable data will be
     * returned.
     * </p>
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     *
     * @param keys
     * @return the data in this record
     */
    public Map<String, Object> map(SerializationOptions options,
            String... keys) {
        List<String> include = Lists.newArrayList();
        List<String> exclude = Lists.newArrayList();
        for (String key : keys) {
            if(key.startsWith("-")) {
                key = key.substring(1);
                exclude.add(key);
            }
            else {
                include.add(key);
            }
        }
        Predicate<Entry<String, Object>> filter = entry -> !exclude
                .contains(entry.getKey());
        if(!options.serializeNullValues()) {
            filter = filter.and(entry -> entry.getValue() != null);
        }
        BiConsumer<Map<String, Object>, Entry<String, Object>> accumulator = (
                map, entry) -> {
            Object value = entry.getValue();
            Collection<?> collection;
            if(options.flattenSingleElementCollections()
                    && value instanceof Collection
                    && (collection = (Collection<?>) value).size() == 1) {
                value = Iterables.getOnlyElement(collection);
            }
            if(value != null) {
                map.merge(entry.getKey(), value, MergeStrategies::upsert);
            }
            else {
                map.put(entry.getKey(), value);
            }
        };
        Stream<Entry<String, Object>> pool;
        if(include.isEmpty()) {
            pool = data().entrySet().stream();
        }
        else {
            // NOTE: later on the #filter will attempt to remove keys that are
            // explicitly excluded, which will have no affect here since
            // #include and #exclude will never both have values at the same
            // time.
            pool = include.stream().map(key -> {
                Object value;
                String[] stops = key.split("\\.");
                if(stops.length > 1) {
                    // For mapping navigation keys, we must manually perform the
                    // navigation and collect a series of nested maps/sequences
                    // so that merging multiple navigation keys can be done
                    // sensibly.
                    key = stops[0];
                    String path = StringUtils.join(stops, '.', 1, stops.length);
                    Object destination = get(key);
                    if(destination instanceof Record) {
                        value = ((Record) destination).map(options, path);
                    }
                    else if(Sequences.isSequence(destination)) {
                        List<Object> $value = Lists.newArrayList();
                        Sequences.forEach(destination, item -> {
                            if(item instanceof Record) {
                                $value.add(((Record) item).map(options, path));
                            }
                        });
                        value = $value;
                    }
                    else {
                        value = null;
                    }
                }
                else {
                    value = get(key);
                }
                return new SimpleEntry<>(key, value);
            });
        }
        Map<String, Object> data = pool.filter(filter).collect(Association::of,
                accumulator, MergeStrategies::upsert);
        return data;
    }

    /**
     * Return a map that contains "readable" data from this {@link Record}.
     * <p>
     * If no {@code keys} are provided, all the readable data will be
     * returned.
     * </p>
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     * 
     * @param keys
     * @return the data in this record
     */
    public Map<String, Object> map(String... keys) {
        return map(SerializationOptions.defaults(), keys);
    }

    /**
     * Save any changes made to this {@link Record}.
     * <p>
     * <strong>NOTE:</strong> This method recursively saves any linked
     * {@link Record records}.
     * </p>
     */
    public boolean save() {
        Verify.that(connections != null,
                "Cannot perform an implicit save because this Record isn't pinned to a Concourse instance");
        Concourse concourse = connections.request();
        try {
            return save(concourse, Sets.newHashSet(), runway);
        }
        finally {
            connections.release(concourse);
        }
    }

    /**
     * Set a dynamic attribute in this Record.
     * 
     * @param key the key name
     * @param value the value to set
     */
    public void set(String key, Object value) {
        if(dynamicData.containsKey(key)) {
            dynamicData.put(key, value);
        }
        else {
            try {
                Reflection.set(key, value, this);
            }
            catch (Exception e) {
                Set<String> intrinsic = INTRINSIC_PROPERTY_CACHE
                        .computeIfAbsent(this.getClass(), c -> LazyTransformSet
                                .of(getFields(c), Field::getName));
                if(intrinsic.contains(key)) {
                    throw e;
                }
                else {
                    dynamicData.put(key, value);
                }
            }
        }
    }

    /**
     * Thrown an exception that describes any exceptions that were previously
     * suppressed. If none occurred, then this method does nothing. This is a
     * good way to understand why a save operation fails.
     * 
     * @throws RuntimeException
     */
    public void throwSupressedExceptions() {
        if(!errors.isEmpty()) {
            Iterator<Throwable> it = errors.iterator();
            StringBuilder summary = new StringBuilder();
            ArrayBuilder<StackTraceElement> stacktrace = ArrayBuilder.builder();
            while (it.hasNext()) {
                Throwable t = it.next();
                summary.append(";").append(t.getMessage());
                stacktrace.add(t.getStackTrace());
                it.remove();
            }
            RuntimeException supressed = new RuntimeException(
                    summary.toString().trim().substring(1));
            supressed.setStackTrace(stacktrace.build());
            throw supressed;
        }
    }

    @Override
    public final String toString() {
        return json();
    }

    /**
     * Provide additional data about this Record that might not be encapsulated
     * in its native fields and is "computed" on-demand.
     * <p>
     * Unlike {@link #derived()} attributes, computed data is generally
     * expensive to generate and should only be calculated when explicitly
     * requested.
     * </p>
     * <p>
     * NOTE: Computed attributes are never cached. Each time one is requested,
     * the computation that generates the value is done anew.
     * </p>
     * 
     * @return the computed data
     */
    protected Map<String, Supplier<Object>> computed() {
        return Collections.emptyMap();
    }

    /**
     * Provide additional data about this Record that might not be encapsulated
     * in its fields. For example, this is a good way to provide template
     * specific information that isn't persisted to the database.
     * 
     * @return the additional data
     */
    protected Map<String, Object> derived() {
        return Maps.newHashMap();
    }

    /**
     * Return additional {@link JsonTypeWriter JsonTypeWriters} that should be
     * use when generating the {@link #json()} for this {@link Record}.
     * 
     * @return a mapping from a {@link Class} to a corresponding
     *         {@link JsonTypeWriter}.
     * @deprecated use {@link #typeAdapters()} instead
     */
    @Deprecated
    protected Map<Class<?>, JsonTypeWriter<?>> jsonTypeHierarchyWriters() {
        return Maps.newHashMap();
    }

    /**
     * Return additional {@link JsonTypeWriter JsonTypeWriters} that should be
     * use when generating the {@link #json()} for this {@link Record}.
     * 
     * @return a mapping from a {@link Class} to a corresponding
     *         {@link JsonTypeWriter}.
     * @deprecated use {@link #typeAdapters()} instead
     */
    @Deprecated
    protected Map<Class<?>, JsonTypeWriter<?>> jsonTypeWriters() {
        return Maps.newHashMap();
    }

    /**
     * A hook that is run whenever this {@link Record} is loaded from the
     * database. This method can contain logic to perform upgrade tasks or other
     * maintenance operations on stored data based on the evolution of business
     * logic.
     * <p>
     * <strong>NOTE:</strong> This method should be idempotent.
     * </p>
     */
    protected void onLoad() {}

    /**
     * Return additional {@link TypeAdapter TypeAdapters} that should be used
     * when generating the {@link #json()} for this {@link Record}.
     * <p>
     * Each {@link TypeAdapter} should be mapped from the most generic class or
     * interface for which the adapter applies.
     * </p>
     * 
     * @return the type adapters to use when serializing the Record to JSON.
     */
    protected Map<Class<?>, TypeAdapter<?>> typeAdapters() {
        return ImmutableMap.of();
    }

    /**
     * Load an existing record from the database and add all of it to this
     * instance in memory.
     * 
     * @param concourse
     * @param existing
     */
    final void load(Concourse concourse, TLongObjectMap<Record> existing) {
        load(concourse, existing, null);
    }

    /**
     * Load an existing record from the database and add all of it to this
     * instance in memory.
     * 
     * @param concourse
     * @param existing
     * @param data data that is pre-loaded from {@code concourse}; this should
     *            only be provided from a trusted source
     */
    /* package */ @SuppressWarnings({ "rawtypes", "unchecked" })
    final void load(Concourse concourse, TLongObjectMap<Record> existing,
            @Nullable Map<String, Set<Object>> data) {
        Preconditions.checkState(id != NULL_ID);
        existing.put(id, this); // add the current object so we don't
                                // recurse infinitely
        checkConstraints(concourse, data);
        if(inZombieState(id, concourse, data)) {
            concourse.clear(id);
            throw new ZombieException();
        }
        data = data == null ? concourse.select(id) : data;
        for (Field field : fields()) {
            try {
                if(!Modifier.isTransient(field.getModifiers())) {
                    String key = field.getName();
                    Class<?> type = field.getType();
                    Object value = null;
                    if(Collection.class.isAssignableFrom(type)
                            || type.isArray()) {
                        Set<?> stored = data.getOrDefault(key,
                                ImmutableSet.of());
                        Class<?> collectedType = type
                                .isArray()
                                        ? type.getComponentType()
                                        : Iterables.getFirst(
                                                Reflection.getTypeArguments(key,
                                                        this.getClass()),
                                                Object.class);
                        ArrayBuilder collector = ArrayBuilder.builder();
                        stored.forEach(item -> {
                            Object converted = convert(key, collectedType, item,
                                    concourse, existing);
                            if(converted != null) {
                                collector.add(converted);
                            }
                            else {
                                // TODO: should we remove the object from
                                // Concourse since it results in a #null value?
                            }
                        });
                        if(type.isArray()) {
                            value = collector.build();
                        }
                        else {
                            if(!Modifier.isAbstract(type.getModifiers())
                                    && !Modifier
                                            .isInterface(type.getModifiers())) {
                                // This is a concrete Collection type that can
                                // be instantiated
                                value = Reflection.newInstance(type);
                            }
                            else if(type == Set.class) {
                                value = Sets.newLinkedHashSet();
                            }
                            else { // assume List
                                value = Lists.newArrayList();
                            }
                            Collections.addAll((Collection) value,
                                    collector.length() > 0 ? collector.build()
                                            : Array.containing());
                        }
                    }
                    else {
                        // Populate a non-collection variable with the most
                        // recently stored value for the #key in Concourse.
                        Set<Object> values = data.getOrDefault(key,
                                ImmutableSet.of());
                        Object stored = Iterables.getFirst(values, null);
                        if(stored != null) {
                            value = convert(key, type, stored, concourse,
                                    existing);
                        }
                    }
                    if(value != null) {
                        field.set(this, value);
                    }
                    else if(value == null
                            && field.isAnnotationPresent(Required.class)) {
                        throw new IllegalStateException("Record " + id
                                + " cannot be loaded because '" + key
                                + "' is a required field, but no value is present in the database.");
                    }
                    else {
                        // no-op; NOTE: Java doesn't allow primitive types to
                        // hold null values
                    }
                }
            }
            catch (ReflectiveOperationException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
    }

    /**
     * Return this {@link Record}'s data {@link map()} as a {@link Multimap}.
     * 
     * @return the data {@link Multimap}
     */
    /* package */ Multimap<String, Object> mmap() {
        return mmap(Array.containing());
    }

    /**
     * Return this {@link Record}'s data {@link map()} as a {@link Multimap}.
     * 
     * @param options
     * @param keys
     * @return the data {@link Multimap}
     */
    /* package */ Multimap<String, Object> mmap(SerializationOptions options,
            String... keys) {
        Map<String, Object> data = map(options, keys);
        Multimap<String, Object> mmap = LinkedHashMultimap.create();
        for (Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(Sequences.isSequence(value)) {
                Sequences.forEach(value, v -> mmap.put(key, v));
            }
            else {
                mmap.put(key, value);
            }
        }
        return mmap;
    }

    /**
     * Return this {@link Record}'s data {@link map()} as a {@link Multimap}.
     * 
     * @param keys
     * @return the data {@link Multimap}
     */
    /* package */ Multimap<String, Object> mmap(String... keys) {
        return mmap(SerializationOptions.defaults(), keys);
    }

    /**
     * Save all changes that have been made to this record using an ACID
     * transaction with the provided {@code runway} instance.
     * <p>
     * Use {@link Runway#save(Record...)} to save changes in multiple records
     * within a single ACID transaction. Even if saving a single record, prefer
     * to use the save method in the {@link Runway} class instead of this for
     * consistent semantics.
     * </p>
     * 
     * @return {@code true} if all the changes have been atomically saved.
     */
    /* package */ final boolean save(Concourse concourse, Set<Record> seen,
            Runway runway) {
        assign(runway);
        try {
            Preconditions.checkState(!inViolation);
            errors.clear();
            concourse.stage();
            if(deleted) {
                delete(concourse);
            }
            else {
                saveWithinTransaction(concourse, seen);
            }
            return concourse.commit();
        }
        catch (Throwable t) {
            concourse.abort();
            if(inZombieState(concourse)) {
                concourse.clear(id);
            }
            errors.add(t);
            return false;
        }
    }

    /**
     * Save the data in this record using the specified {@code concourse}
     * connection. This method assumes that the caller has already started an
     * transaction, if necessary and will commit the transaction after this
     * method completes.
     * 
     * @param concourse
     */
    /* package */ void saveWithinTransaction(final Concourse concourse,
            Set<Record> seen) {
        concourse.verifyOrSet(SECTION_KEY, __, id);
        Set<String> alreadyVerifiedUniqueConstraints = Sets.newHashSet();
        fields().forEach(field -> {
            try {
                if(!Modifier.isTransient(field.getModifiers())) {
                    final String key = field.getName();
                    final Object value = field.get(this);
                    if(field.isAnnotationPresent(ValidatedBy.class)) {
                        Class<? extends Validator> validatorClass = field
                                .getAnnotation(ValidatedBy.class).value();
                        Validator validator = Reflection
                                .newInstance(validatorClass);
                        Preconditions.checkState(validator.validate(value),
                                validator.getErrorMessage());
                    }
                    if(field.isAnnotationPresent(Unique.class)) {
                        Unique constraint = field.getAnnotation(Unique.class);
                        String name = constraint.name();
                        if(name.length() == 0) {
                            Preconditions
                                    .checkState(isUnique(concourse, key, value),
                                            field.getName()
                                                    + " must be unique in "
                                                    + __);
                        }
                        else if(!alreadyVerifiedUniqueConstraints
                                .contains(name)) {
                            // Find all the fields that have this constraint and
                            // check for uniqueness.
                            Map<String, Object> values = Maps.newHashMap();
                            values.put(key, value);
                            fields().stream().filter($field -> $field != field)
                                    .filter($field -> $field
                                            .isAnnotationPresent(Unique.class))
                                    .filter($field -> $field
                                            .getAnnotation(Unique.class).name()
                                            .equals(name))
                                    .forEach($field -> {
                                        values.put($field.getName(), Reflection
                                                .get($field.getName(), this));
                                    });
                            if(isUnique(concourse, values)) {
                                alreadyVerifiedUniqueConstraints.add(name);
                            }
                            else {
                                throw new IllegalStateException(AnyStrings
                                        .format("{} must be unique", name));
                            }
                        }
                    }
                    if(field.isAnnotationPresent(Required.class)) {
                        Preconditions.checkState(!Empty.ness().describes(value),
                                field.getName() + " is required in " + __);
                    }
                    if(value != null) {
                        store(key, value, concourse, false, seen);
                    }
                    else {
                        concourse.clear(key, id);
                    }
                }
            }
            catch (ReflectiveOperationException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        });

    }

    /**
     * Check to ensure that this Record does not violate any constraints. If so,
     * throw an {@link IllegalStateException}.
     * 
     * @param concourse
     * @throws IllegalStateException
     */
    private void checkConstraints(Concourse concourse,
            @Nullable Map<String, Set<Object>> data) {
        try {
            String section = null;
            if(data == null) {
                section = concourse.get(SECTION_KEY, id);
            }
            else {
                section = (String) Iterables
                        .getLast(data.computeIfAbsent(SECTION_KEY,
                                ignore -> concourse.select(SECTION_KEY, id)));
            }
            Verify.that(section != null);
            Verify.that(
                    section.equals(__) || Class.forName(__)
                            .isAssignableFrom(Class.forName(section)),
                    "Cannot load a record from section %s "
                            + "into a Record of type %s",
                    section, __);
        }
        catch (ReflectiveOperationException | IllegalStateException e) {
            inViolation = true;
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Compare this object to another {@code record} using the provided
     * {@code order} specification.
     * 
     * @param record the object to be compared
     * @param order an ordered mapping (i.e. {@link LinkedHashMap}) from sort
     *            key to an integer that specifies the sort direction (e.g. a
     *            positive integer means the sorting should be done in ascending
     *            order with the "smallest" values appearing first. A negative
     *            integer implies the opposite).
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the specified object.
     */
    private int compareTo(Record record, Map<String, Integer> order) {
        for (Entry<String, Integer> entry : order.entrySet()) {
            String key = entry.getKey();
            Integer coefficient = entry.getValue();
            Verify.thatArgument(coefficient != 0,
                    "The order coefficient cannot be 0");
            Object mine = get(key);
            Object theirs = record.get(key);
            int comparison;
            if(mine == null && theirs == null) {
                comparison = Ordering.arbitrary().compare(mine, theirs);
                coefficient = 1;
            }
            else if(mine == null && theirs != null) {
                // NULL value is considered "greater" so that it is always at
                // the end.
                comparison = 1;
                coefficient = 1;
            }
            else if(mine != null && theirs == null) {
                // NULL value is considered "greater" so that it is always at
                // the end.
                comparison = -1;
                coefficient = 1;
            }
            else if(mine instanceof Number && theirs instanceof Number) {
                comparison = Numbers.compare((Number) mine, (Number) theirs);
            }
            else if((mine instanceof String || mine instanceof Tag)
                    && (theirs instanceof String || theirs instanceof Tag)) {
                comparison = mine.toString().toLowerCase()
                        .compareTo(theirs.toString().toLowerCase());
            }
            else if(Comparable.class.isAssignableFrom(
                    Reflection.getClosestCommonAncestor(mine.getClass(),
                            theirs.getClass()))) {
                comparison = Ordering.natural().compare((Comparable<?>) mine,
                        (Comparable<?>) theirs);
            }
            else {
                comparison = Ordering.usingToString().compare(mine, theirs);
            }
            if(comparison != 0) {
                return coefficient * comparison;
            }
            else {
                continue;
            }
        }
        return Longs.compare(id(), record.id());
    }

    /**
     * Convert the {@code stored} value for {@code key} into the appropriate
     * Java object based on the field {@code type}.
     * 
     * @param key
     * @param type
     * @param stored
     * @param concourse
     * @param alreadyLoaded
     * @return the converted Object
     */
    @Nullable
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Object convert(String key, Class<?> type, Object stored,
            Concourse concourse, TLongObjectMap<Record> alreadyLoaded) {
        Object converted = null;
        if(Record.class.isAssignableFrom(type)
                || type == DeferredReference.class) {
            Link link = (Link) stored;
            long target = link.longValue();
            converted = alreadyLoaded.get(target);
            if(converted == null) {
                if(type == DeferredReference.class) {
                    converted = new DeferredReference(target, runway);
                }
                else {
                    Map<String, Set<Object>> data = concourse.select(target);
                    String section = (String) Iterables
                            .getLast(data.get(SECTION_KEY), null);
                    if(Empty.ness().describes(section)) {
                        concourse.remove(key, stored, id); // do some ad-hoc
                                                           // cleanup
                    }
                    else {
                        Class<? extends Record> targetClass = Reflection
                                .getClassCasted(section);
                        converted = load(targetClass, target, alreadyLoaded,
                                connections, concourse, runway, data);
                    }
                }
            }
        }
        else if(type.isPrimitive() || ImmutableSet
                .of(String.class, Integer.class, Long.class, Float.class,
                        Double.class, Boolean.class, Timestamp.class)
                .contains(type)) {
            converted = stored;
        }
        else if(type == Tag.class) {
            // NOTE: Concourse returns Tag values as String objects.
            converted = Tag.create(stored.toString());
        }
        else if(type.isEnum()) {
            converted = Enum.valueOf((Class<Enum>) type, stored.toString());
        }
        else if(Serializable.class.isAssignableFrom(type)) {
            ByteBuffer bytes = ByteBuffer
                    .wrap(BaseEncoding.base64Url().decode(stored.toString()));
            converted = Serializables.read(bytes, (Class<Serializable>) type);
        }
        else {
            converted = new Gson().fromJson(stored.toString(), type);
        }
        return converted;

    }

    /**
     * Return a map that contains all of the readable data in this record.
     * 
     * @return the data in this record
     */
    private Map<String, Object> data() {
        // The #computed data must be wrapped in a special map that only
        // computes the requested values on-demand.
        Map<String, Object> computed = new AbstractMap<String, Object>() {

            @Override
            public Set<Entry<String, Object>> entrySet() {
                return computed().entrySet().stream().map(ComputedEntry::new)
                        .collect(Collectors.toSet());
            }

            @Override
            public Object get(Object key) {
                Supplier<?> computer = computed().get(key);
                if(computer != null) {
                    return computer.get();
                }
                else {
                    return null;
                }
            }

            @Override
            public Set<String> keySet() {
                return computed().keySet();
            }

        };
        Map<String, Object> data = BackupReadSourcesHashMap.create(derived(),
                computed);
        fields().forEach(field -> {
            try {
                Object value;
                if(isReadableField(field)) {
                    value = field.get(this);
                    value = dereference(field, value);
                    data.put(field.getName(), value);
                }
            }
            catch (ReflectiveOperationException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        });
        data.put("id", id);
        data.putAll(dynamicData);
        return data;
    }

    /**
     * Perform an actual "deletion" of this record from the database.
     * 
     * @param concourse
     */
    private void delete(Concourse concourse) {
        concourse.clear(id);
    }

    /**
     * Return all the non-internal {@link Field fields} in this class.
     * 
     * @return the non-internal {@link Field fields}
     */
    private Set<Field> fields() {
        return getFields(this.getClass());
    }

    /**
     * Return {@code true} if this record is in a "zombie" state meaning it
     * exists in the database without any actual data.
     * 
     * @param concourse
     * @return {@code true} if this record is a zombie
     */
    private final boolean inZombieState(Concourse concourse) {
        return inZombieState(id, concourse, null);
    }

    /**
     * Return {@code true} if {@code key} as {@code value} for this class is
     * unique, meaning there is no other record in the database in this class
     * with that mapping. If {@code value} is a collection, then this method
     * will return {@code true} if and only if every element in the collection
     * is unique.
     * 
     * @param concourse
     * @param key
     * @param value
     * @return {@code true} if {@code key} as {@code value} is a unique mapping
     *         for this class
     */
    private boolean isUnique(Concourse concourse, String key, Object value) {
        return value != null ? isUnique(concourse, AnyMaps.create(key, value))
                : true;
    }

    /**
     * Return {@code true} if all the key/value pairs in {@code data} are
     * collectively unique for this class. This means that there is no other
     * record in the database for this class with all the mappings.
     * <p>
     * If any of the values in {@code data} are a
     * {@link Sequences#isSequence(Object) sequence}, this method will return
     * {@code true} if and only if every element in every
     * {@link Sequences#isSequence(Object) sequence} is unique.
     * </p>
     * 
     * @param concourse
     * @param data
     * @return
     */
    private boolean isUnique(Concourse concourse, Map<String, Object> data) {
        AtomicReference<BuildableState> $criteria = new AtomicReference<>(
                Criteria.where().key(SECTION_KEY).operator(Operator.EQUALS)
                        .value(getClass().getName()));
        for (Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(value == null) {
                continue; // A null value does not affect uniqueness.
            }
            else if(Sequences.isSequence(value)) {
                AtomicReference<BuildableState> $sub = new AtomicReference<>(
                        null);
                Sequences.forEach(value, item -> {
                    item = serialize(item);
                    if($sub.get() == null) {
                        $sub.set(Criteria.where().key(key)
                                .operator(Operator.EQUALS).value(item));
                    }
                    else {
                        $sub.set($sub.get().or().key(key)
                                .operator(Operator.EQUALS).value(item));
                    }
                });
                $criteria.set(Criteria.where()
                        .group($criteria.get().and().group($sub.get())));
            }
            else {
                value = serialize(value);
                $criteria.set($criteria.get().and().key(key)
                        .operator(Operator.EQUALS).value(value));
            }
        }
        Criteria criteria = $criteria.get();
        Set<Long> records = concourse.find(criteria);
        return records.isEmpty()
                || (records.contains(id) && records.size() == 1);
    }

    /**
     * Return the JSON string for this {@link Record}.
     * 
     * <p>
     * This method also supports <strong>negative filtering</strong>. You can
     * prefix any of the {@code keys} with a minus sign (e.g. {@code -}) to
     * indicate that the key should be excluded from the data that is returned.
     * </p>
     * 
     * @param options
     * @param links
     * @param keys the attributes to include in the JSON
     * @return the JSON string.
     */
    @SuppressWarnings({ "unchecked" })
    private String json(SerializationOptions options,
            Multimap<Record, Record> links, String... keys) {
        Map<String, Object> data = keys.length == 0 ? map(options)
                : map(options, keys);

        // Create a dynamic type Gson instance that will detect recursive links
        // and prevent infinite recursion when trying to generate the JSON.
        GsonBuilder builder = new GsonBuilder().registerTypeAdapterFactory(
                TypeAdapters.primitiveTypesFactory(true));
        if(options.flattenSingleElementCollections()) {
            builder.registerTypeAdapterFactory(
                    TypeAdapters.collectionFactory(true));
        }
        if(options.serializeNullValues()) {
            builder.serializeNulls();
        }
        builder.registerTypeAdapterFactory(
                generateDynamicRecordTypeAdapterFactory(options, this, links))
                .setPrettyPrinting().disableHtmlEscaping();
        Map<Class<?>, TypeAdapter<?>> adapters = Maps.newLinkedHashMap();
        Streams.concat(jsonTypeWriters().entrySet().stream(),
                jsonTypeHierarchyWriters().entrySet().stream())
                .forEach(entry -> {
                    Class<?> clazz = entry.getKey();
                    TypeAdapter<?> adapter = entry.getValue().typeAdapter();
                    adapters.put(clazz, adapter);
                });
        adapters.putAll(typeAdapters());
        adapters.forEach((clazz, adapter) -> {
            builder.registerTypeAdapterFactory(new TypeAdapterFactory() {

                @Override
                public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
                    if(type.getRawType().isAssignableFrom(clazz)) {
                        return (TypeAdapter<T>) adapter.nullSafe();
                    }
                    else {
                        return null;
                    }
                }

            });
        });
        Gson gson = builder.create();
        return gson.toJson(data);
    }

    /**
     * Store {@code key} as {@code value} using the specified {@code concourse}
     * connection. The {@code append} flag is used to indicate if the value
     * should be appended using the {@link Concourse#add(String, Object, long)}
     * method or inserted using the {@link Concourse#set(String, Object, long)}
     * method.
     * 
     * @param key
     * @param value
     * @param concourse
     * @param append
     */
    @SuppressWarnings("rawtypes")
    private void store(String key, Object value, Concourse concourse,
            boolean append, Set<Record> seen) {
        // NOTE: This logic mirrors serialization logic in the #serialize
        // method. Since this method has some optimizations, it doesn't call
        // into #serialize directly. So, if modifications are made to this
        // method, please make similar and appropriate modifications to
        // #serialize.
        // TODO: dirty field detection!
        if(value instanceof Record) {
            Record record = (Record) value;
            if(!seen.contains(record)) {
                seen.add(record);
                record.saveWithinTransaction(concourse, seen);
            }
            if(append) {
                concourse.link(key, record.id, id);
            }
            else {
                concourse.verifyOrSet(key, Link.to(record.id), id);
            }
        }
        else if(value instanceof DeferredReference) {
            DeferredReference deferred = (DeferredReference) value;
            Record ref = deferred.$ref();
            if(ref != null) {
                store(key, ref, concourse, append, seen);
            }
            else {
                // no-op because the reference was not loaded and therefore has
                // no changes to save...
            }
        }
        else if(value instanceof Collection || value.getClass().isArray()) {
            // TODO use reconcile() function once 0.5.0 comes out...
            concourse.clear(key, id); // TODO this is extreme...move to a diff
                                      // based approach to delete only values
                                      // that should be deleted
            for (Object item : (Iterable<?>) value) {
                store(key, item, concourse, true, seen);
            }
        }
        else if(value.getClass().isPrimitive() || value instanceof String
                || value instanceof Tag || value instanceof Link
                || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double
                || value instanceof Boolean || value instanceof Timestamp) {
            if(append) {
                concourse.add(key, value, id);
            }
            else {
                concourse.verifyOrSet(key, value, id);
            }
        }
        else if(value instanceof Enum) {
            concourse.set(key, Tag.create(((Enum) value).name()), id);
        }
        else if(value instanceof Serializable) {
            ByteBuffer bytes = Serializables.getBytes((Serializable) value);
            Tag base64 = Tag.create(BaseEncoding.base64Url()
                    .encode(ByteBuffers.toByteArray(bytes)));
            store(key, base64, concourse, append, seen);
        }
        else {
            Gson gson = new Gson();
            Tag json = Tag.create(gson.toJson(value));
            store(key, json, concourse, append, seen);
        }
    }

    /**
     * A {@link DatabaseInterface} that reacts to the state of the
     * {@link #runway} variable and delegates to it or throws an
     * {@link UnsupportedOperationException} if it is {@code null}.
     *
     * @author Jeff Nelson
     */
    private static class ReactiveDatabaseInterface implements
            DatabaseInterface {

        /**
         * A reference to the enclosing {@link Record} whose state is watched
         * and reacted to. This is needed since this class is static.
         */
        private final Record tracked;

        /**
         * Construct a new instance.
         * 
         * @param tracked
         */
        private ReactiveDatabaseInterface(Record tracked) {
            this.tracked = tracked;
        }

        @Override
        public <T extends Record> Set<T> find(Class<T> clazz,
                Criteria criteria) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
                Order order) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria, order);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
                Order order, Page page) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria, order, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
                Page page) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> findAny(Class<T> clazz,
                Criteria criteria) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> findAny(Class<T> clazz,
                Criteria criteria, Order order) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria, order);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> findAny(Class<T> clazz,
                Criteria criteria, Order order, Page page) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria, order, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> findAny(Class<T> clazz,
                Criteria criteria, Page page) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> T findAnyUnique(Class<T> clazz,
                Criteria criteria) {
            if(tracked.runway != null) {
                return tracked.runway.findAnyUnique(clazz, criteria);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> T findUnique(Class<T> clazz,
                Criteria criteria) {
            if(tracked.runway != null) {
                return tracked.runway.findUnique(clazz, criteria);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> load(Class<T> clazz) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> T load(Class<T> clazz, long id) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, id);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> load(Class<T> clazz, Order order) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, order);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> load(Class<T> clazz, Order order,
                Page page) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, order, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> load(Class<T> clazz, Page page) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> loadAny(Class<T> clazz) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz, order);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
                Page page) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz, order, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> loadAny(Class<T> clazz, Page page) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz, page);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

    }

}
