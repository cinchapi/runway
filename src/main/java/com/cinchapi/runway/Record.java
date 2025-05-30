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

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

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
import com.cinchapi.concourse.util.Logging;
import com.cinchapi.concourse.util.Numbers;
import com.cinchapi.concourse.util.Parsers;
import com.cinchapi.concourse.util.TypeAdapters;
import com.cinchapi.concourse.validate.Keys;
import com.cinchapi.runway.json.JsonTypeWriter;
import com.cinchapi.runway.util.ComputedEntry;
import com.cinchapi.runway.util.BackupReadSourcesHashMap;
import com.cinchapi.runway.validation.Validator;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
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
        Set<String> intrinsic = StaticAnalysis.instance().getKeys(clazz);
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
            ConcurrentMap<Long, Record> existing, ConnectionPool connections,
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
     * Use reflection to get the value for {@code field} in {@code record}.
     * 
     * @param field
     * @param record
     * @return the value
     */
    private static Object getFieldValue(Field field, Record record) {
        try {
            return field.get(record);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Execute the equivalent logic of {@link Map#getOrDefault(Object, Object)}
     * on {@code map}, but ensure that a false negative bug does not occur.
     * 
     * @param map
     * @param key
     * @param defaultValue
     * @return the value associated with {@code key} in {@code map} or
     *         {@code defaultValue} if no such value exists
     */
    private static <K, V> V getOrDefaultSafely(Map<K, V> map, K key,
            V defaultValue) {
        // Handle corner case where it has been observed that Map#getOrDefault
        // occasionally/randomly does not return a value for a key even though
        // that key is indeed in the map.
        V value = map.get(key);
        if(value == null) {
            if(!map.containsKey(key.toString() + "." + IDENTIFIER_KEY)) {
                for (Entry<K, V> entry : map.entrySet()) {
                    if(key.equals(entry.getKey())) {
                        value = entry.getValue();
                        break;
                    }
                }
            }
        }
        return value != null ? value : defaultValue;
    }

    /**
     * Add the canonical data representation of {@code value} to the
     * {@code hasher}.
     * 
     * @param hasher
     * @param value
     */
    @SuppressWarnings("rawtypes")
    private static void hashValue(Hasher hasher, @Nullable Object value) {
        if(value != null) {
            hasher.putString(value.getClass().getName(),
                    StandardCharsets.UTF_8);
            if(Sequences.isSequence(value)) {
                Sequences.forEach(value, item -> hashValue(hasher, item));
            }
            else if(value instanceof Record) {
                Record record = (Record) value;
                hasher.putLong(record.id());
            }
            else if(value instanceof DeferredReference) {
                DeferredReference deferred = (DeferredReference) value;
                hasher.putLong(deferred.$id());
            }
            else if(value instanceof Tag || value instanceof String) {
                hasher.putString(value.toString(), StandardCharsets.UTF_8);
            }
            else if(value instanceof Enum) {
                hasher.putInt(((Enum) value).ordinal());
            }
            else if(value instanceof Boolean
                    || value.getClass() == boolean.class) {
                hasher.putBoolean((boolean) value);
            }
            else if(value instanceof Byte || value.getClass() == byte.class) {
                hasher.putByte((byte) value);
            }
            else if(value instanceof Double
                    || value.getClass() == double.class) {
                hasher.putDouble((double) value);
            }
            else if(value instanceof Float || value.getClass() == float.class) {
                hasher.putFloat((float) value);
            }
            else if(value instanceof Integer || value.getClass() == int.class) {
                hasher.putInt((int) value);
            }
            else if(value instanceof Long || value.getClass() == long.class) {
                hasher.putLong((long) value);
            }
            else if(value instanceof Short || value.getClass() == short.class) {
                hasher.putShort((short) value);
            }
            else if(value instanceof Timestamp) {
                Timestamp timestamp = (Timestamp) value;
                hasher.putLong(timestamp.getMicros());
            }
            else if(value instanceof Serializable) {
                byte[] bytes = ByteBuffers.toByteArray(
                        Serializables.getBytes((Serializable) value));
                hasher.putBytes(bytes);
            }
            else {
                String json = new Gson().toJson(value);
                hasher.putString(json, StandardCharsets.UTF_8);
            }
        }
        else {
            hasher.putInt(0);
            hasher.putInt(0);
        }
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
    private static <T extends Record> T load(Class<?> clazz, long id,
            ConcurrentMap<Long, Record> existing, ConnectionPool connections,
            Concourse concourse, Runway runway,
            @Nullable Map<String, Set<Object>> data) {
        return load(clazz, id, existing, connections, concourse, runway, data,
                null);
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
            ConcurrentMap<Long, Record> existing, ConnectionPool connections,
            Concourse concourse, Runway runway,
            @Nullable Map<String, Set<Object>> data, String prefix) {
        T record = (T) newDefaultInstance(clazz, connections);
        setInternalFieldValue("id", id, record);
        setInternalFieldValue("waitingToBeDeleted", new LinkedHashSet<>(),
                record);
        record.assign(runway);
        record.load(concourse, existing, data, prefix);
        record.onLoad();
        return record;
    }

    /**
     * Get a new instance of {@code clazz} by calling the default (zero-arg)
     * constructor, if it exists. This method attempts to correctly invoke
     * constructors for nested inner classes.
     * 
     * @param clazz
     * @param connections
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
            setInternalFieldValue("__", clazz.getName(), instance);
            setInternalFieldValue("deleted", false, instance);
            setInternalFieldValue("dynamicData", new HashMap<>(), instance);
            setInternalFieldValue("errors", new ArrayList<>(), instance);
            setInternalFieldValue("id", NULL_ID, instance);
            setInternalFieldValue("inViolation", false, instance);
            setInternalFieldValue("connections", connections, instance);
            setInternalFieldValue("db",
                    new ReactiveDatabaseInterface((Record) instance), instance);
            return instance;
        }
        catch (InstantiationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * If {@code value} is a {@link Record}, {@link DeferredReference} or a
     * {@link Sequences#isSequence(Object) Sequence} that contains either, try
     * to {@link #saveWithinTransaction(Concourse, Set) save} it, in case it has
     * {@link #hasUnsavedChanges() unsaved} changes.
     * 
     * @param value
     * @param concourse
     * @param seen
     */
    @SuppressWarnings("rawtypes")
    private static void saveModifiedReferenceWithinTransaction(Object value,
            Concourse concourse, Set<Record> seen) {
        if(Sequences.isSequence(value)) {
            Sequences.forEach(value,
                    item -> saveModifiedReferenceWithinTransaction(item,
                            concourse, seen));
        }
        else {
            Record record = null;
            if(value instanceof Record) {
                record = (Record) value;
            }
            else if(value instanceof DeferredReference) {
                DeferredReference deferred = (DeferredReference) value;
                record = deferred.$ref();
            }

            if(record != null && !seen.contains(record)) {
                record.saveWithinTransaction(concourse, seen);
            }
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
    private static Object serializeScalarValue(@Nonnull Object value) {
        Preconditions.checkArgument(!Sequences.isSequence(value));
        Preconditions.checkNotNull(value);
        if(value instanceof Record) {
            Record record = (Record) value;
            return Link.to(record.id);
        }
        else if(value instanceof DeferredReference) {
            DeferredReference deferred = (DeferredReference) value;
            return Link.to(deferred.$id());
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
            return Tag.create(new Gson().toJson(value));
        }
    }

    /**
     * Set the {@code value} of an {@link #INTERNAL_FIELDS internal field} on
     * the provided {@code instance}.
     * <p>
     * Use this method instead of {@link Reflection#set(String, Object, Object)}
     * to take advantage of performance optimizations.
     * </p>
     * 
     * @param name
     * @param value
     * @param instance
     */
    private static <T> void setInternalFieldValue(String name, Object value,
            T instance) {
        try {
            Field field = INTERNAL_FIELDS.get(name);
            Preconditions.checkArgument(field != null,
                    "%s is not an internal field", name);
            field.setAccessible(true);
            field.set(instance, value);

        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /* package */ static Runway PINNED_RUNWAY_INSTANCE = null;

    /**
     * The key used to hold the {@link #__realms} metadata.
     */
    /* package */ static final String REALMS_KEY = "_realms";

    /**
     * The key used to hold the section metadata.
     */
    /* package */ static final String SECTION_KEY = "_"; // just want a
                                                         // simple/short key
                                                         // name that is likely
                                                         // to avoid collisions

    /**
     * The key that references a records id in Concourse.
     */
    private static final String IDENTIFIER_KEY = "$id$";

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
     * Object types that are boxed versions of primitive types.
     */
    private static Set<Class<?>> BOXED_PRIMITIVE_TYPES = ImmutableSet.of(
            String.class, Integer.class, Long.class, Float.class, Double.class,
            Boolean.class, Timestamp.class);

    /**
     * The {@link Field fields} that are defined in the base class.
     */
    private static Map<String, Field> INTERNAL_FIELDS = Stream
            .of(Reflection.getAllDeclaredFields(Record.class))
            .collect(Collectors.toMap(field -> field.getName(), field -> field,
                    (a, b) -> b, HashMap::new));

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
     * An internal flag that tracks whether {@link #_realms} have been
     * {@link #addRealm(String) added} or {@link #removeRealm(String) removed}.
     * This flag is necessary so that this Record's data cache isn't
     * unnecessarily invalidated when reconciling the realms on
     * {@link #saveWithinTransaction(Concourse, Set)}.
     */
    private transient boolean _hasModifiedRealms = false;

    /**
     * The list of realms to which this Record belongs.
     * <p>
     * Realms allow records to be placed in logically distinct groups while
     * existing in the same physical database and environment.
     * </p>
     * <p>
     * By default, a Record is not explicitly assigned to any realm and is
     * therefore a member of all realms. If this field contains one or more
     * explicit realms, then a Record is considered to only exist in those
     * realms.
     * </p>
     * <p>
     * Runway supports loading data that exists in any realm or within one or
     * more explicit realms.
     * </p>
     */
    private transient Set<String> _realms = ImmutableSet.of();

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
     * A cache of the {@link #$computed() properties}.
     */
    private transient Map<String, Supplier<Object>> computed = null;

    /**
     * A cache of the {@link #$derived() properties}.
     */
    private transient Map<String, Object> derived = null;

    /**
     * Tracks dependent {@link Records} that are pending
     * {@link #delete(Concourse) deletion} from {@link Concourse}.
     */
    private Set<Record> waitingToBeDeleted = new LinkedHashSet<>();

    /**
     * The {@link Record}'s checksum that is generated and cached on
     * {@link #load(Concourse, ConcurrentMap, Map, String) load} and
     * {@link #saveWithinTransaction(Concourse, Set) save} events.
     * <p>
     * This value is <strong>NOT</strong> returned from {@link #checksum()}, but
     * is instead compared against the value returned from that method to
     * determine if this {@link Record} has any {@link #hasUnsavedChanges()
     * unsaved} changes.
     * </p>
     */
    private String __checksum = null;

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
     * Add this {@link Record} to {@code realm}.
     * 
     * @param realm
     * @return {@code true} if this {@link Record} was added to {@link realm};
     *         otherwise {@code false}
     */
    public boolean addRealm(String realm) {
        if(_realms.isEmpty()) {
            _realms = Sets.newLinkedHashSet();
        }
        return _realms.add(realm) && (_hasModifiedRealms = true);
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
     * Set this {@link Record} to be deleted from Concourse when the
     * {@link #save()} method is called.
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
                        Field field = StaticAnalysis.instance().getField(this,
                                key);
                        if(isReadableField(field)) {
                            value = field.get(this);
                            value = dereference(key, value);
                        }
                    }
                    catch (Exception e) {/* ignore */}
                }
                if(value == null) {
                    value = $derived().get(key);
                }
                if(value == null) {
                    Supplier<?> computer = $computed().get(key);
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
     * Return {@code true} if this {@link Record} and the other {@code record}
     * exist in at least one overlapping realm.
     * 
     * @param record
     * @return {@code true} if this and {@code record} share any realms
     */
    public boolean inSameRealm(Record record) {
        return _realms.isEmpty() && record._realms.isEmpty()
                || !Sets.intersection(_realms, record._realms).isEmpty();
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
     * Return the names of all the {@link Realms} where this {@link Record}
     * exists.
     * 
     * @return this {@link Record Record's} realms
     */
    public Set<String> realms() {
        return Collections.unmodifiableSet(_realms);
    }

    /**
     * Remove this {@link Record} from {@code realm}.
     * 
     * @param realm
     * @return {@code true} if this {@link Record} was removed from
     *         {@code realm}; otherwise {@code false} (e.g. this {@link Record}
     *         never existed in {@code realm})
     */
    public boolean removeRealm(String realm) {
        try {
            return _realms.remove(realm) && (_hasModifiedRealms = true);
        }
        finally {
            if(_realms.isEmpty()) {
                _realms = ImmutableSet.of();
            }
        }
    }

    /**
     * Save any changes made to this {@link Record}.
     * <p>
     * <strong>NOTE:</strong> This method recursively saves any linked
     * {@link Record records}.
     * </p>
     */
    public final boolean save() {
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
     * {@link #set(String, Object) Set} each key/value pair within {@code data}
     * as a dynamic attribute in this {@link Record}.
     * 
     * @param data
     */
    public void set(Map<String, Object> data) {
        data.forEach((key, value) -> {
            set(key, value);
        });
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
                Set<String> intrinsic = StaticAnalysis.instance()
                        .getKeys(this.getClass());
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
     * A hook that is executed immediately before this {@link Record} is
     * {@link #save() saved} to the database. This method provides an
     * opportunity to update the record's state or perform validation before
     * persistence.
     * <p>
     * Implementations can use this hook to:
     * <ul>
     * <li>Perform last-minute data transformations or normalizations</li>
     * <li>Update dependent fields based on current state</li>
     * <li>Execute business logic that should affect the saved state</li>
     * <li>Perform custom validation beyond what annotations provide</li>
     * </ul>
     * </p>
     * <p>
     * Any changes made to the record's fields within this method will be
     * included in the save operation. This method is called within the same
     * transaction as the save operation, ensuring atomicity.
     * </p>
     * <p>
     * <strong>Note:</strong> This method should not throw exceptions unless the
     * save operation should be aborted. If an exception is thrown, the
     * transaction will be rolled back.
     * </p>
     */
    protected void beforeSave() {}

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
     * @deprecated Use the {@link Computed} annotation instead
     */
    @Deprecated
    protected Map<String, Supplier<Object>> computed() {
        return Collections.emptyMap();
    }

    /**
     * Provide additional data about this Record that might not be encapsulated
     * in its fields. For example, this is a good way to provide template
     * specific information that isn't persisted to the database.
     * 
     * @return the additional data
     * @deprecated Use the {@link Derived} annotation instead
     */
    @Deprecated
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
     * Provide a hook to completely bypass the standard save routine.
     * <p>
     * This method can be overridden to return a {@link Supplier} that
     * determines the result of a save operation without actually persisting
     * data to the database.
     * This is useful in scenarios such as:
     * </p>
     * <ul>
     * <li>Creating ad hoc/in-memory only records that don't need database
     * persistence</li>
     * <li>Mocking save behavior for testing without database interaction</li>
     * <li>Implementing custom persistence logic that doesn't use the standard
     * flow</li>
     * </ul>
     * <p>
     * When this method returns a non-null value, the normal save process is
     * bypassed entirely, and the boolean result from the supplier is used as
     * the save operation result.
     * </p>
     * <p>
     * <strong>Note:</strong> Use this with caution as it completely circumvents
     * the standard persistence mechanism. Records with overridden save behavior
     * won't trigger save listeners or other standard save-related
     * functionality.
     * </p>
     * 
     * @return a {@link Supplier} that returns the result of the save operation,
     *         or {@code null} to use the standard save process
     */
    protected Supplier<Boolean> overrideSave() {
        return null;
    }

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
     * Return {@code true} if this {@link Record} has any unsaved changes.
     * 
     * @return {@code true} if there are changes that need to be saved.
     */
    /* package */ boolean hasUnsavedChanges() {
        if(__checksum == null) {
            return true;
        }
        else {
            return !__checksum.equals(checksum());
        }
    }

    /**
     * Load an existing record from the database and add all of it to this
     * instance in memory.
     * 
     * @param concourse
     * @param existing
     */
    /* package */ final void load(Concourse concourse,
            ConcurrentMap<Long, Record> existing) {
        load(concourse, existing, null);
    }

    /**
     * Load an existing record from the database and add all of it to this
     * instance in memory.
     * 
     * @param concourse
     * @param existing
     * @param data
     */
    /* package */ final void load(Concourse concourse,
            ConcurrentMap<Long, Record> existing,
            @Nullable Map<String, Set<Object>> data) {
        load(concourse, existing, data, null);
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
    final void load(Concourse concourse, ConcurrentMap<Long, Record> existing,
            @Nullable Map<String, Set<Object>> data, @Nullable String prefix) {
        Preconditions.checkState(id != NULL_ID);
        existing.put(id, this); // add the current object so we don't
                                // recurse infinitely
        if(data == null) {
            Set<String> paths = runway
                    .getPathsForClassIfSupported(this.getClass());
            // @formatter:off
            data = paths != null 
                    ? concourse.select(paths, id)
                    : concourse.select(id);
            // @formatter:on
        }
        if(prefix == null
                || !runway.properties().supportsPreSelectLinkedRecords()) {
            prefix = "";
        }
        checkConstraints(concourse, data, prefix);
        if(inZombieState(id, concourse, data)) {
            concourse.clear(id);
            throw new ZombieException();
        }
        Set<Object> realms = data.getOrDefault(prefix + REALMS_KEY,
                ImmutableSet.of());
        _realms = realms.size() > 0
                ? realms.stream().map(Object::toString)
                        .collect(Collectors.toCollection(LinkedHashSet::new))
                : ImmutableSet.of();
        for (Field field : fields()) {
            try {
                if(!Modifier.isTransient(field.getModifiers())) {
                    String key = field.getName();
                    String path = prefix + key;
                    Class<?> type = field.getType();
                    Object value = null;
                    Set<Object> stored = getOrDefaultSafely(data, path,
                            ImmutableSet.of());
                    if(Collection.class.isAssignableFrom(type)
                            || type.isArray()) {
                        Class<?> collectedType = type.isArray()
                                ? type.getComponentType()
                                : Iterables.getFirst(
                                        StaticAnalysis.instance()
                                                .getTypeArguments(this, key),
                                        Object.class);
                        ArrayBuilder collector = ArrayBuilder.builder();
                        stored.forEach(item -> {
                            Object converted = convert(path, collectedType,
                                    item, concourse, existing);
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
                        Object first = Iterables.getFirst(stored, null);
                        if(first == null
                                && Record.class.isAssignableFrom(type)) {
                            // Check to see if data for a nested Record was
                            // pre-selected and load it without making another
                            // database roundtrip.
                            String prepend = path + ".";
                            Long id = (Long) Iterables.getFirst(
                                    data.getOrDefault(prepend + IDENTIFIER_KEY,
                                            ImmutableSet.of()),
                                    null);
                            if(id != null) {
                                String $type = (String) Iterables.getFirst(
                                        data.getOrDefault(prepend + SECTION_KEY,
                                                ImmutableSet.of()),
                                        null);
                                if($type != null) {
                                    type = Reflection.getClassCasted($type);
                                }
                                value = existing.get(id);
                                value = value == null
                                        ? load(type, id, existing, connections,
                                                concourse, runway, data,
                                                prepend)
                                        : value;
                            }
                        }
                        else if(first != null) {
                            value = convert(path, type, first, concourse,
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
        __checksum = checksum();
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
        Map<String, Collection<Object>> wrapper = new CollectionValueWrapperMap<>(
                data);
        return new SyntheticMultimap<>(wrapper);
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
        Supplier<Boolean> override = overrideSave();
        if(override != null) {
            return override.get();
        }
        assign(runway);
        try {
            Preconditions.checkState(!inViolation);
            errors.clear();
            concourse.stage();
            saveWithinTransaction(concourse, seen);
            boolean success = concourse.commit();
            if(success && runway != null) {
                runway.enqueueSaveNotification(this);
            }
            return success;
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
     * Save the data within this record using the specified {@code concourse}
     * connection, adhering to constraints specified by the record's field
     * annotations.
     * This method assumes that the caller has already started a transaction and
     * will execute within the same transaction context.
     * <p>
     * It iterates over the fields of the record and performs different
     * operations based on field annotations:
     * </p>
     * <ul>
     * <li>{@link Required @Required} enforces that a value is non-empty or
     * contains at least one non-empty item if a sequence.</li>
     * <li>{@link ValidatedBy @ValidatedBy} applies a custom validation defined
     * by the validator class specified in the annotation to each element.</li>
     * <li>{@link Unique @Unique} ensures that each element in a field must be
     * unique across all records in the class, failing to save if duplicate
     * values are found.</li>
     * </ul>
     * <p>
     * If the record has modified realms, it reconciles them with the existing
     * ones. Values are transformed into storable form using the
     * {@link #transform(Object, Concourse, Set)} method before saving. In case
     * of violations of constraints, an {@code IllegalStateException} is thrown.
     * </p>
     *
     * @param concourse The Concourse instance to execute the operation.
     * @param seen Set of records already saved, to prevent infinite recursion.
     * @throws IllegalStateException If required fields are missing, values are
     *             not unique across
     *             all records in the class, or validation fails.
     * @throws ReflectiveOperationException If reflection-related errors occur
     *             during processing.
     */
    /* package */ void saveWithinTransaction(final Concourse concourse,
            Set<Record> seen) {
        seen.add(this);
        if(_hasModifiedRealms) {
            concourse.reconcile(REALMS_KEY, id, _realms);
            _hasModifiedRealms = false;
        }
        if(deleted) {
            deleteWithinTransaction(concourse);
        }
        else if(!hasUnsavedChanges()) {
            // This Record hasn't been modified, so simply go through each field
            // and try to save any outgoing Record references that contain
            // modifications.
            for (Field field : fields()) {
                Object value = getFieldValue(field, this);
                saveModifiedReferenceWithinTransaction(value, concourse, seen);
            }
        }
        else if(overrideSave() != null) {
            overrideSave().get();
        }
        else {
            beforeSave();
            concourse.verifyOrSet(SECTION_KEY, __, id);
            Set<String> alreadyVerifiedUniqueConstraints = Sets.newHashSet();
            for (Field field : fields()) {
                if(!Modifier.isTransient(field.getModifiers())) {
                    String key = field.getName();
                    Object value = getFieldValue(field, this);
                    boolean isSequence = Sequences.isSequence(value);
                    // Enforce that Required fields have a non-empty value
                    if(field.isAnnotationPresent(Required.class) && (isSequence
                            ? Sequences.stream(value)
                                    .allMatch(Empty.ness()::describes)
                            : Empty.ness().describes(value))) {
                        throw new IllegalStateException(AnyStrings
                                .format("{}  is required in {}", key, __));
                    }
                    if(value != null) {
                        // Enforce that Unique fields have non-duplicated
                        // values across the class
                        if(field.isAnnotationPresent(Unique.class)
                                && (isSequence ? Sequences.stream(value)
                                        .anyMatch(Predicates.not(
                                                item -> checkIsUnique(concourse,
                                                        field, key, item,
                                                        alreadyVerifiedUniqueConstraints)))
                                        : !checkIsUnique(concourse, field, key,
                                                value,
                                                alreadyVerifiedUniqueConstraints))) {
                            String name = field.getAnnotation(Unique.class)
                                    .name();
                            name = name.length() == 0 ? key : name;
                            throw new IllegalStateException(AnyStrings.format(
                                    "{} must be unique in {}", name, __));
                        }
                        // Apply custom validation
                        if(field.isAnnotationPresent(ValidatedBy.class)) {
                            Class<? extends Validator> validatorClass = field
                                    .getAnnotation(ValidatedBy.class).value();
                            Validator validator = Reflection
                                    .newInstance(validatorClass);
                            if(isSequence
                                    ? Sequences.stream(value).anyMatch(
                                            Predicates.not(validator::validate))
                                    : !validator.validate(value)) {
                                throw new IllegalStateException(
                                        validator.getErrorMessage());

                            }
                        }
                        value = transform(value, concourse, seen);
                        if(value.getClass().isArray()) {
                            concourse.reconcile(key, id, (Object[]) value);
                        }
                        else {
                            concourse.verifyOrSet(key, value, id);
                        }
                    }
                    else {
                        concourse.clear(key, id);
                    }
                }
            }
            __checksum = checksum();
        }
    }

    /**
     * Gather all of the computed properties.
     * 
     * @return the computer properties
     */
    private Map<String, Supplier<Object>> $computed() {
        if(computed == null) {
            computed = new HashMap<>();
            computed.putAll(computed());
            for (Method method : Reflection.getAllDeclaredMethods(this)) {
                Computed annotation = method.getAnnotation(Computed.class);
                if(annotation != null) {
                    if(method.getParameterCount() == 0) {
                        String key = annotation.value();
                        if(key.isEmpty()) {
                            key = method.getName();
                        }
                        computed.put(key, () -> {
                            try {
                                return method.invoke(this);
                            }
                            catch (ReflectiveOperationException e) {
                                throw CheckedExceptions
                                        .wrapAsRuntimeException(e);
                            }
                        });
                    }
                    else {
                        throw new IllegalArgumentException(
                                "A method annotated with "
                                        + annotation.getClass().getSimpleName()
                                        + " cannot require parameters");
                    }

                }

            }
        }
        return computed;
    }

    /**
     * Gather all of the derived properties.
     * 
     * @return the computer properties
     */
    private Map<String, Object> $derived() {
        if(derived == null) {
            derived = new HashMap<>();
            derived.putAll(derived());
            for (Method method : Reflection.getAllDeclaredMethods(this)) {
                Derived annotation = method.getAnnotation(Derived.class);
                if(annotation != null) {
                    if(method.getParameterCount() == 0) {
                        String key = annotation.value();
                        if(key.isEmpty()) {
                            key = method.getName();
                        }
                        try {
                            derived.put(key, method.invoke(this));
                        }
                        catch (ReflectiveOperationException e) {
                            throw CheckedExceptions.wrapAsRuntimeException(e);
                        }
                    }
                    else {
                        throw new IllegalArgumentException(
                                "A method annotated with "
                                        + annotation.getClass().getSimpleName()
                                        + " cannot require parameters");
                    }

                }

            }
        }
        return derived;
    }

    /**
     * Check to ensure that this Record does not violate any constraints. If so,
     * throw an {@link IllegalStateException}.
     * 
     * @param concourse
     * @throws IllegalStateException
     */
    private void checkConstraints(Concourse concourse,
            @Nullable Map<String, Set<Object>> data, String prefix) {
        try {
            String section = null;
            if(data == null) {
                section = concourse.get(SECTION_KEY, id);
            }
            else {
                Set<Object> $$ = data.computeIfAbsent(prefix + SECTION_KEY,
                        $ -> concourse.select(SECTION_KEY, id));
                if(!$$.isEmpty()) {
                    section = (String) Iterables.getLast($$);
                }
            }
            Verify.that(section != null);
            Verify.that(
                    section.equals(__) || Class.forName(__)
                            .isAssignableFrom(Class.forName(section)),
                    "Cannot load a record from section {} "
                            + "into a Record of type {}",
                    section, __);
        }
        catch (ReflectiveOperationException | IllegalStateException e) {
            inViolation = true;
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Checks whether a given value for a field is unique across all records in
     * the class. If the {@link Unique} constraint has a name, this method
     * verifies the uniqueness for all fields with that same constraint name
     * within the class. If the constraint has been already verified, it is
     * skipped.
     * 
     * @param concourse The Concourse instance managing the transaction.
     * @param field The field that is being checked for uniqueness.
     * @param key The key or name of the field.
     * @param value The value that needs to be checked for uniqueness.
     * @param alreadyVerifiedUniqueConstraints A set containing names of
     *            constraints that have already been verified.
     * @return {@code true} if the value is unique according to the specified
     *         constraints; {@code false} otherwise.
     */
    private boolean checkIsUnique(Concourse concourse, Field field, String key,
            Object value, Set<String> alreadyVerifiedUniqueConstraints) {
        Unique constraint = field.getAnnotation(Unique.class);
        String name = constraint.name();
        if(name.length() == 0 && !isUnique(concourse, key, value)) {
            return false;
        }
        else if(!alreadyVerifiedUniqueConstraints.contains(name)) {
            // Find all the fields that have this constraint and
            // check for uniqueness.
            Map<String, Object> values = Maps.newHashMap();
            values.put(key, value);
            fields().stream().filter($field -> $field != field)
                    .filter($field -> $field.isAnnotationPresent(Unique.class))
                    .filter($field -> $field.getAnnotation(Unique.class).name()
                            .equals(name))
                    .forEach($field -> {
                        values.put($field.getName(),
                                Reflection.get($field.getName(), this));
                    });
            if(isUnique(concourse, values)) {
                alreadyVerifiedUniqueConstraints.add(name);
            }
            else {
                return false;
            }
        }
        return true;
    }

    /**
     * Return a checksum for this {@link Record} based on its current state.
     * 
     * @return the checksum
     */
    private final String checksum() {
        Hasher hasher = Hashing.murmur3_128().newHasher();
        Set<Field> fields = fields().stream()
                .sorted((f1, f2) -> f1.getName().compareTo(f2.getName()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        for (Field field : fields) {
            Object value = getFieldValue(field, this);
            hasher.putString(field.getName(), StandardCharsets.UTF_8);
            hashValue(hasher, value);
        }
        return hasher.hash().toString();
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
            Concourse concourse, ConcurrentMap<Long, Record> alreadyLoaded) {
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
        else if(type.isPrimitive() || BOXED_PRIMITIVE_TYPES.contains(type)) {
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
                return $computed().entrySet().stream().map(ComputedEntry::new)
                        .collect(Collectors.toSet());
            }

            @Override
            public Object get(Object key) {
                Supplier<?> computer = $computed().get(key);
                if(computer != null) {
                    return computer.get();
                }
                else {
                    return null;
                }
            }

            @Override
            public Set<String> keySet() {
                return $computed().keySet();
            }

        };
        Map<String, Object> data = BackupReadSourcesHashMap.create($derived(),
                computed);
        fields().forEach(field -> {
            try {
                Object value;
                if(isReadableField(field)) {
                    value = field.get(this);
                    String key = field.getName();
                    value = dereference(key, value);
                    data.put(key, value);
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
    private void deleteWithinTransaction(Concourse concourse) {
        // Ensure any fields to which this Record must @CascadeDelete are
        // deleted within this transaction
        Set<Field> dependents = StaticAnalysis.instance()
                .getAnnotatedFields(getClass(), CascadeDelete.class);
        for (Field dependent : dependents) {
            try {
                Object value = dependent.get(this);
                if(value instanceof Record) {
                    ensureDeletion((Record) value);
                }
                else if(Sequences.isSequence(value)) {
                    Sequences.forEach(value, item -> {
                        if(item instanceof Record) {
                            ensureDeletion((Record) item);
                        }
                    });
                }
            }
            catch (ReflectiveOperationException e) {
                throw CheckedExceptions.wrapAsRuntimeException(e);
            }
        }

        // Check if there are any incoming links that used the @JoinDelete
        // annotation to join in the deletion of this Record
        Criteria potentialJoinDeletes = StaticAnalysis.instance()
                .getJoinDeleteLookupCondition(this);
        if(potentialJoinDeletes != null) {
            for (Entry<Long, Set<Object>> entry : concourse
                    .select(SECTION_KEY, potentialJoinDeletes).entrySet()) {
                // It's necessary to load each of the Records (instead of
                // directly clearing it in the database) in case any of them
                // also have deletion hook annotations.
                long id = entry.getKey();
                String __ = (String) Iterables.getLast(entry.getValue());
                Class<? extends Record> clazz = Reflection.getClassCasted(__);
                Record record = db.load(clazz, id);
                ensureDeletion(record);
            }
        }

        // Check if there are any incoming links that used the @CaptureDelete
        // annotation to remove references to this Record post deletion. Handle
        // that business and save those records within this transaction
        Criteria potentialCaptureDeletes = StaticAnalysis.instance()
                .getCaptureDeleteLookupCondition(this);
        if(potentialCaptureDeletes != null) {
            for (Entry<Long, Set<Object>> entry : concourse
                    .select(SECTION_KEY, potentialCaptureDeletes).entrySet()) {
                long id = entry.getKey();
                String __ = (String) Iterables.getLast(entry.getValue());
                Class<? extends Record> clazz = Reflection.getClassCasted(__);
                Record record = db.load(clazz, id);
                Set<Field> fields = StaticAnalysis.instance()
                        .getAnnotatedFields(record, CaptureDelete.class);
                for (Field field : fields) {
                    try {
                        Object value = field.get(record);
                        if(value instanceof Collection) {
                            Collection<?> collection = (Collection<?>) value;
                            while (collection.contains(this)) {
                                collection.remove(this);
                            }
                        }
                        else if(value.getClass().isArray()) {
                            ArrayBuilder<Object> ab = ArrayBuilder.builder();
                            Sequences.forEach(value, item -> {
                                if(!item.equals(this)) {
                                    ab.add(item);
                                }
                            });
                            field.set(record, ab.build());
                        }
                        else if(value.equals(this)) {
                            field.set(record, null);
                        }
                    }
                    catch (ReflectiveOperationException e) {
                        throw CheckedExceptions.wrapAsRuntimeException(e);
                    }
                }
                record.saveWithinTransaction(concourse, new HashSet<>());
            }
        }

        // Perform the deletion(s)
        concourse.clear(id);
        for (Record record : waitingToBeDeleted) {
            record.deleteWithinTransaction(concourse);
        }
    }

    /**
     * Dereference the {@code value} stored for {@code key} if it is a
     * {@link DeferredReference} or a {@link Sequence} of them.
     * 
     * @param key
     * @param value
     * @return the dereferenced value if it can be dereferenced or the original
     *         input
     */
    private Object dereference(String key, Object value) {
        if(value == null) {
            return value;
        }
        else if(value instanceof DeferredReference) {
            value = ((DeferredReference<?>) value).get();
        }
        else if(Sequences.isSequence(value)) {
            Collection<Class<?>> typeArgs = StaticAnalysis.instance()
                    .getTypeArguments(this, key);
            if(typeArgs.contains(DeferredReference.class)
                    || typeArgs.contains(Object.class)) {
                value = Sequences.stream(value)
                        .map(item -> dereference(key, item))
                        .collect(Collectors.toCollection(
                                value instanceof Set ? LinkedHashSet::new
                                        : ArrayList::new));
            }
        }
        return value;
    }

    /**
     * Ensure that {@code record} is scheduled for
     * {@link #deleteWithinTransaction(Concourse) deletion} alongside this
     * {@link Record}.
     * 
     * @param record
     */
    private void ensureDeletion(Record record) {
        if(!record.deleted) {
            waitingToBeDeleted.add(record);
            record.deleted = true;
        }
    }

    /**
     * Return all the non-internal {@link Field fields} in this class.
     * 
     * @return the non-internal {@link Field fields}
     */
    private Collection<Field> fields() {
        return StaticAnalysis.instance().getFields(this);
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
                    item = serializeScalarValue(item);
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
                value = serializeScalarValue(value);
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
     * Transforms the provided {@code value} into a primitive form that can be
     * stored within {@link Concourse concourse}. The transformation is
     * recursive, handling nested {@link Record records} and
     * {@link Sequences#isSequence(Object) sequences}.
     * <p>
     * The result will either be a {@link Concourse} primitive or an array of
     * {@link Concourse} primitives; include Java primitive types, {@link Tag
     * tags}, {@link Link links}, and serialized representations of complex
     * objects.
     * </p>
     * <p>
     * If the value is an instance of {@link Record}, it's saved within the
     * current {@link Concourse concourse} transaction and linked. If the value
     * is a {@link DeferredReference}, it is similarly saved if the reference
     * was {@link DeferredReference#get() loaded}.
     * </p>
     * <p>
     * For simplicity, all {@link Sequences#isSequence(Object) Sequences} are
     * transformed into arrays.
     * </p>
     *
     * @param value The value to be transformed.
     * @param concourse The Concourse instance managing the transaction.
     * @param seen the records that have already been {@link #save() saved} (to
     *            prevent infinite recursion)
     * @return a {@link Concourse} primitive or an array of {@link Concourse}
     *         primitives.
     */
    @SuppressWarnings("rawtypes")
    private Object transform(@Nonnull Object value, Concourse concourse,
            Set<Record> seen) {
        if(Sequences.isSequence(value)) {
            ArrayBuilder<Object> array = ArrayBuilder.builder();
            Sequences.forEach(value, item -> {
                array.add(transform(item, concourse, seen));
            });
            return array.length() > 0 ? array.build() : Array.containing();
        }
        else {
            Object primitive = serializeScalarValue(value);
            Record record;
            if(value instanceof Record) {
                record = (Record) value;
            }
            else if(value instanceof DeferredReference) {
                DeferredReference deferred = (DeferredReference) value;
                record = deferred.$ref();
            }
            else {
                record = null;
            }

            // Ensure that Record references are saved within the current
            // transaction
            if(record != null && !seen.contains(record)) {
                seen.add(record);
                record.saveWithinTransaction(concourse, seen);
            }

            return primitive;
        }
    }

    /**
     * A collection of static data about available {@link Record} types and
     * their fields to make {@link Runway} operations more efficient.
     *
     * @author Jeff Nelson
     */
    public static class StaticAnalysis {

        /**
         * Return the {@link StaticAnalysis}.
         * <p>
         * NOTE: Scanning the classpath to perform static analysis adds startup
         * costs proportional to the number of classes defined, so it is only
         * done once to minimize the effect of the cost.
         * </p>
         * 
         * @return the {@link StaticAnalysis}
         */
        public static StaticAnalysis instance() {
            return INSTANCE;
        }

        /**
         * Return the non-cyclic paths (e.g., keys and navigation keys) for
         * the fields in {@code clazz}; all prefixed with {@code prefix} and
         * using {@code ancestors} for cycle detection.
         * 
         * @param clazz
         * @param hierarchies
         * @param fieldsByClass
         * @return the paths
         */
        private static Set<String> computePaths(Class<? extends Record> clazz,
                Multimap<Class<? extends Record>, Class<?>> hierarchies,
                Map<Class<? extends Record>, Map<String, Field>> fieldsByClass) {
            return computePaths(clazz, hierarchies, fieldsByClass, "",
                    Sets.newHashSet());
        }

        /**
         * Return the non-cyclic paths (e.g., keys and navigation keys) for
         * the fields in {@code clazz}; all prefixed with {@code prefix} and
         * using {@code ancestors} for cycle detection.
         * 
         * @param clazz
         * @param hierarchies
         * @param fieldsByClass
         * @param prefix
         * @param ancestors
         * @return the paths
         */
        @SuppressWarnings("unchecked")
        private static Set<String> computePaths(Class<? extends Record> clazz,
                Multimap<Class<? extends Record>, Class<?>> hierarchies,
                Map<Class<? extends Record>, Map<String, Field>> fieldsByClass,
                String prefix, Set<Class<? extends Record>> ancestors) {
            ancestors.add(clazz);
            Set<String> paths = new LinkedHashSet<>();
            paths.add(prefix + SECTION_KEY);
            paths.add(prefix + IDENTIFIER_KEY);
            paths.add(prefix + REALMS_KEY);
            Collection<Field> fields = fieldsByClass
                    .getOrDefault(clazz, ImmutableMap.of()).values();
            for (Field field : fields) {
                Class<?> type = field.getType();
                Set<Class<? extends Record>> lineage = new HashSet<>(ancestors);
                if(Record.class.isAssignableFrom(type)
                        && !ancestors.contains(type)
                        && (COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS
                                || !hasDescendantDefinedFields(type,
                                        hierarchies, fieldsByClass))) {
                    Class<? extends Record> _type = (Class<? extends Record>) type;
                    lineage.add(_type);
                    Collection<Class<?>> hierarchy = hierarchies.get(_type);
                    Set<String> nested = new HashSet<>();
                    for (Class<?> descendant : hierarchy) {
                        // Account for declared types having descendant defined
                        // fields in child classes by computing the paths for
                        // each descendant type at this junction, in the path
                        nested.addAll(computePaths(
                                (Class<? extends Record>) descendant,
                                hierarchies, fieldsByClass,
                                prefix + field.getName() + ".", lineage));
                    }
                    paths.addAll(nested);
                }
                else {
                    paths.add(prefix + field.getName());
                }
            }
            return paths;
        }

        /**
         * Perform {@link #computePaths(Class, Multimap, Map)} for each
         * {@link Class} in the {@link #hierarchies hierarchy} of {@code clazz}.
         * 
         * @param clazz
         * @param hierarchies
         * @param fieldsByClass
         * @return all the paths in the hierarchy
         */
        @SuppressWarnings("unchecked")
        private static Set<String> computePathsHierarchy(
                Class<? extends Record> clazz,
                Multimap<Class<? extends Record>, Class<?>> hierarchies,
                Map<Class<? extends Record>, Map<String, Field>> fieldsByClass) {
            Set<String> paths = new LinkedHashSet<>();
            Collection<Class<?>> hiearchy = hierarchies.get(clazz);
            for (Class<?> type : hiearchy) {
                paths.addAll(computePaths((Class<? extends Record>) type,
                        hierarchies, fieldsByClass));
            }
            return paths;
        }

        /**
         * Return {@code true} if {@code clazz} has any descendants in its
         * hierarchy that have additional fields that are not defined in
         * {@code clazz}.
         * 
         * @param clazz
         * @param hierarchies
         * @param fieldsByClass
         * @return {@code true} if {@code clazz} is the parent to any
         *         descendants with descendant defined fields
         */
        @SuppressWarnings("unchecked")
        private static boolean hasDescendantDefinedFields(Class<?> clazz,
                Multimap<Class<? extends Record>, Class<?>> hierarchies,
                Map<Class<? extends Record>, Map<String, Field>> fieldsByClass) {
            Collection<Class<?>> descendants = hierarchies
                    .get((Class<? extends Record>) clazz);
            if(descendants.size() == 1) {
                return false;
            }
            else {
                Map<String, Field> fields = fieldsByClass.get(clazz);
                for (Class<?> descendant : descendants) {
                    if(!fields.equals(fieldsByClass.get(descendant))) {
                        return true;
                    }
                }
                return false;
            }
        }

        /**
         * Internal toggle to control the aggressiveness of the logic for
         * {@link #computePaths(Class, Multimap, Map) computing paths} when a
         * declared field type is a the parent to a descendant class with
         * {@link #hasDescendantDefinedFields(Class, Multimap, Map) descendant
         * defined fields}.
         * <p>
         * When this field is set to {@code true}, the computed paths should
         * include those for every possible descendant defined field. Otherwise,
         * fields whose declared type is a parent to a descendant with
         * descendant defined fields should not be expanded.
         * </p>
         */
        // Visible for Testing
        static boolean COMPUTE_PATHS_FOR_DESCENDANT_DEFINED_FIELDS = false;

        static {
            Logging.disable(Reflections.class);
            Reflections.log = null; // turn off reflection logging
        }

        private static final StaticAnalysis INSTANCE = new StaticAnalysis();

        /**
         * A mapping from each {@link Record} class to its traversable paths.
         */
        private Map<Class<? extends Record>, Set<String>> pathsByClass;

        /**
         * A mapping from each {@link Record} class to the traversable paths in
         * its {@link #hierarchies hierarchy}.
         */
        private Map<Class<? extends Record>, Set<String>> pathsByClassHierarchy;

        /**
         * A mapping from each {@link Record} class to itself and all of its
         * descendants. This facilitates querying across hierarchies.
         */
        private final Multimap<Class<? extends Record>, Class<?>> hierarchies;

        /**
         * A mapping from each {@link Record} class to each its non-internal
         * keys, each of which is mapped to the associated {@link Field} object.
         */
        private final Map<Class<? extends Record>, Map<String, Field>> fieldsByClass;

        /**
         * A mapping from each {@link Record} class to each of its non-internal
         * keys, each of which is mapped to a collection of type arguments
         * associated with that corresponding {@link Field} object.
         */
        private final Map<Class<? extends Record>, Map<String, Collection<Class<?>>>> fieldTypeArgumentsByClass;

        /**
         * A collection containing each {@link Record} class that has at least
         * one field whose type is a subclass of {@link Record}.
         */
        private final Set<Class<? extends Record>> hasRecordFieldTypeByClass;

        /**
         * A collection containing each {@link Record} class that itself or is
         * the ancestors of a {@link Record} class that has at least one field
         * whose type is a subclass of {@link Record}.
         */
        private final Set<Class<? extends Record>> hasRecordFieldTypeByClassHierarchy;

        /**
         * Reflection handler
         */
        private final Reflections reflection = new Reflections(
                new SubTypesScanner());

        /**
         * A collection containing each {@link Record} class that has fields
         * with annotations.
         */
        private final Map<Class<? extends Record>, Map<Class<? extends Annotation>, Set<Field>>> fieldAnnotationsByClass;

        /**
         * A mapping from each {@link Record} class to each of its related
         * {@link Record} classes, each mapped to a set of field names that
         * trigger deletion of records linked to it.
         * <p>
         * This structure enables efficient lookup of fields marked with the
         * {@link JoinDelete} annotation, helping identify fields in linked
         * records that should be deleted when the primary {@link Record} is
         * deleted.
         * </p>
         */
        private final Map<Class<? extends Record>, Map<Class<? extends Record>, Set<String>>> joinDeleteFieldsByClass;

        /**
         * A mapping from each {@link Record} class to each of its related
         * {@link Record} classes, each mapped to a set of field names that
         * trigger automatic reference nullification for records linked to it.
         * <p>
         * This structure enables efficient lookup of fields marked with the
         * {@link CaptureDelete} annotation, helping identify fields in linked
         * records that should be nullified when the primary {@link Record} is
         * deleted.
         * </p>
         */
        private final Map<Class<? extends Record>, Map<Class<? extends Record>, Set<String>>> captureDeleteFieldsByClass;

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
            this.fieldAnnotationsByClass = new HashMap<>();
            this.joinDeleteFieldsByClass = new HashMap<>();
            this.captureDeleteFieldsByClass = new HashMap<>();
            Set<String> internalFieldNames = INTERNAL_FIELDS.keySet();
            reflection.getSubTypesOf(Record.class).forEach(type -> {
                // Build class hierarchy
                hierarchies.put(type, type);
                reflection.getSubTypesOf(type)
                        .forEach(subType -> hierarchies.put(type, subType));

                // Get non-internal fields and associated metadata
                Stream<Field> nonInternalFields = Arrays
                        .asList(Reflection.getAllDeclaredFields(type)).stream()
                        .filter(field -> !internalFieldNames
                                .contains(field.getName()));
                nonInternalFields.forEach(field -> {
                    Map<String, Field> fields = fieldsByClass
                            .computeIfAbsent(type, $ -> new HashMap<>());
                    Map<String, Collection<Class<?>>> fieldTypeArguments = fieldTypeArgumentsByClass
                            .computeIfAbsent(type, $ -> new HashMap<>());
                    String key = field.getName();
                    fields.put(key, field);
                    fieldTypeArguments.put(key,
                            Reflection.getTypeArguments(field));
                    if(Record.class.isAssignableFrom(field.getType())) {
                        hasRecordFieldTypeByClass.add(type);
                    }

                    // Get annotations associated with each field
                    for (Annotation annotation : field.getAnnotations()) {
                        fieldAnnotationsByClass
                                .computeIfAbsent(type, $ -> new HashMap<>())
                                .computeIfAbsent(annotation.annotationType(),
                                        $ -> new HashSet<>())
                                .add(field);
                    }
                });
            });

            hierarchies.forEach((type, relative) -> {
                if(hasRecordFieldTypeByClass.contains(relative)) {
                    hasRecordFieldTypeByClassHierarchy.add(type);
                }
            });

            // For each type, determine the types that have Link fields with the
            // deletion hook annotations.
            Map<Class<? extends Annotation>, Map<Class<? extends Record>, Map<Class<? extends Record>, Set<String>>>> hooks = ImmutableMap
                    .of(JoinDelete.class, joinDeleteFieldsByClass,
                            CaptureDelete.class, captureDeleteFieldsByClass);
            hierarchies.keySet().forEach(type -> {
                for (Class<? extends Record> incoming : hierarchies.keySet()) {
                    hooks.forEach((annotation, data) -> {
                        Set<Field> fields = getAnnotatedFields(incoming,
                                annotation);
                        for (Field field : fields) {
                            if(isTypeCompatibleWithClassField(type, incoming,
                                    field)) {
                                data.computeIfAbsent(type, $ -> new HashMap<>())
                                        .computeIfAbsent(incoming,
                                                $$ -> new HashSet<>())
                                        .add(field.getName());
                            }
                        }
                    });
                }
            });

            // Now that the hierarchies and fields for each Record type have
            // been documented, go through again and compute the paths for each
            // one
            computeAllPossiblePaths();
        }

        /**
         * Return a set of fields from a {@link Record} class that are annotated
         * with a specific annotation.
         * 
         * @param <T> the type of record
         * @param clazz the class of the record
         * @param annotation the class of the annotation to look for
         * @return a set of fields annotated with the specified annotation, or
         *         an
         *         empty set if none are found
         * @throws IllegalArgumentException if the provided class is unknown
         */
        public <T extends Record> Set<Field> getAnnotatedFields(Class<T> clazz,
                Class<? extends Annotation> annotation) {
            try {
                return fieldAnnotationsByClass
                        .getOrDefault(clazz, ImmutableMap.of())
                        .getOrDefault(annotation, ImmutableSet.of());
            }
            catch (NullPointerException e) {
                throw new IllegalArgumentException(
                        "Unknown Record type: " + clazz);
            }
        }

        /**
         * Return a set of fields from an instance of a {@link Record} that are
         * annotated with a specific annotation.
         * 
         * @param <T> the type of record
         * @param record the record instance
         * @param annotation the class of the annotation to look for
         * @return a set of fields annotated with the specified annotation, or
         *         an
         *         empty set if none are found
         * @throws IllegalArgumentException if the provided record type is
         *             unknown
         */
        public <T extends Record> Set<Field> getAnnotatedFields(T record,
                Class<? extends Annotation> annotation) {
            return getAnnotatedFields(record.getClass(), annotation);
        }

        /**
         * Return a {@link Criteria} instance that defines the condition to
         * locate records linked to the specified {@link Record} that should
         * have fields nullified upon deletion of the specified record.
         * <p>
         * This condition is constructed by identifying fields in other
         * {@link Record} classes that are annotated with {@link CaptureDelete}
         * and linked to the specified {@code record}. These fields are grouped
         * by related classes, and then a combined criteria is formed to match
         * against the links found.
         * </p>
         *
         * @param record the {@link Record} instance for which the nullification
         *            condition is being determined
         * @return a {@link Criteria} object representing the lookup condition
         *         for records linked to the provided {@code record} that should
         *         have references nullified, or {@code null} if no such links
         *         are found
         */
        public <T extends Record> Criteria getCaptureDeleteLookupCondition(
                Record record) {
            return getDeletionHookLookupCondition(record,
                    captureDeleteFieldsByClass);
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
                throw new IllegalArgumentException(
                        "Unknown Record type: " + clazz);
            }
        }

        /**
         * Return the {@link Field} object for {@code key} in the {@link Class}
         * of {@code record}.
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
                throw new IllegalArgumentException(
                        "Unknown Record type: " + clazz);
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
         * Return a {@link Criteria} instance that defines the condition to
         * locate records linked to the specified {@link Record} for deletion.
         * <p>
         * This condition is constructed by identifying fields in other
         * {@link Record} classes that are annotated with {@link JoinDelete} and
         * linked to the specified {@code record}. These fields are grouped by
         * related classes, and then a combined criteria is formed to match
         * against the links found.
         * </p>
         *
         * @param record the {@link Record} instance for which the delete
         *            condition is being determined
         * @return a {@link Criteria} object representing the delete lookup
         *         condition for records linked to the provided {@code record};
         *         or {@code null} if no such links are found
         */
        @Nullable
        public <T extends Record> Criteria getJoinDeleteLookupCondition(
                Record record) {
            return getDeletionHookLookupCondition(record,
                    joinDeleteFieldsByClass);
        }

        /**
         * Return the names of the non-internal fields in {@code clazz}.
         * 
         * @param clazz
         * @return the keys
         */
        public <T extends Record> Set<String> getKeys(Class<T> clazz) {
            try {
                return fieldsByClass.get(clazz).keySet();
            }
            catch (NullPointerException e) {
                throw new IllegalArgumentException(
                        "Unknown Record type: " + clazz);
            }
        }

        /**
         * Return all the paths (e.g., navigable keys based on fields with
         * linked
         * {@link Record} types) for {@code clazz}.
         * 
         * @param clazz
         * @return the paths
         */
        public <T extends Record> Set<String> getPaths(Class<T> clazz) {
            return pathsByClass.get(clazz);
        }

        /**
         * Return all the paths (e.g., navigable keys based on fields with
         * linked
         * {@link Record} types) for {@code clazz} and all of its descendents.
         * 
         * @param clazz
         * @return the paths
         */
        public Set<String> getPathsHierarchy(Class<? extends Record> clazz) {
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
                throw new IllegalArgumentException(
                        "Unknown Record type: " + clazz);
            }
        }

        /**
         * Return any defined type arguments for the field named {@code key} in
         * the
         * {@link Class} of {@code record}.
         * 
         * @param clazz
         * @param key
         * @return the type arguments
         */
        public <T extends Record> Collection<Class<?>> getTypeArguments(
                T record, String key) {
            return getTypeArguments(record.getClass(), key);
        }

        /**
         * Return {@code true} if {@code clazz} has any fields whose type is a
         * subclass of {@link Record}.
         * 
         * @param clazz
         * @return {@code true} if {@code clazz} has any {@link Record} type
         *         fields
         */
        public boolean hasFieldOfTypeRecordInClass(
                Class<? extends Record> clazz) {
            return hasRecordFieldTypeByClass.contains(clazz);
        }

        /**
         * Return {@code true} if {@code clazz}, or any of its descendants, have
         * any
         * fields whose type is a subclass of {@link Record}.
         * 
         * @param clazz
         * @return {@code true} if {@code clazz}, or any of its children, have
         *         any
         *         {@link Record} type fields
         */
        public boolean hasFieldOfTypeRecordInClassHierarchy(
                Class<? extends Record> clazz) {
            return hasRecordFieldTypeByClassHierarchy.contains(clazz);
        }

        /**
         * Do {@link #computePaths(Class, Multimap, Map)} for every
         * {@link Record} type and store.
         */
        // Visible for Testing
        void computeAllPossiblePaths() {
            reflection.getSubTypesOf(Record.class).forEach(type -> {
                pathsByClass.put(type,
                        computePaths(type, hierarchies, fieldsByClass));
                pathsByClassHierarchy.put(type, computePathsHierarchy(type,
                        hierarchies, fieldsByClass));
            });
        }

        /**
         * Return a {@link Criteria} instance that defines the condition to
         * locate records linked to the specified {@link Record} based on a
         * deletion hook.
         * This method is designed to support various deletion hooks, such as
         * {@link JoinDelete} and {@link CaptureDelete}, by using the provided
         * data map to determine which fields in other records should be
         * affected when the specified {@code record} is deleted.
         * <p>
         * This condition is constructed by gathering all fields in related
         * {@link Record} classes that are marked with the specified deletion
         * annotation and link to the specified {@code record}. Each associated
         * class and its fields are combined into a single {@link Criteria}
         * expression that matches records with fields needing to be either
         * deleted or nullified, depending on the deletion hook type.
         * </p>
         *
         * @param record the {@link Record} instance for which the deletion
         *            condition is being constructed
         * @param data a mapping that defines the deletion hooks for each
         *            {@link Record} class, where each key is a {@link Record}
         *            type linked by the hook, and each value is a set of field
         *            names annotated with the deletion hook
         * @return a {@link Criteria} instance representing the lookup condition
         *         for records linked to the provided {@code record}, or
         *         {@code null} if no linked records with the deletion hook are
         *         found
         */
        @Nullable
        private <T extends Record> Criteria getDeletionHookLookupCondition(
                Record record,
                Map<Class<? extends Record>, Map<Class<? extends Record>, Set<String>>> data) {
            Criteria condition = null;
            Map<Class<? extends Record>, Set<String>> components = data
                    .getOrDefault(record.getClass(), ImmutableMap.of());
            for (Entry<Class<? extends Record>, Set<String>> entry : components
                    .entrySet()) {
                Class<? extends Record> type = entry.getKey();
                Set<String> keys = entry.getValue();
                Criteria typePart = Criteria.where().key(SECTION_KEY)
                        .operator(Operator.EQUALS).value(type.getName());
                Criteria linkPart = null;
                for (String key : keys) {
                    Criteria $ = Criteria.where().key(key)
                            .operator(Operator.LINKS_TO).value(record.id);
                    if(linkPart == null) {
                        linkPart = $;
                    }
                    else {
                        linkPart = Criteria.where().group(linkPart).or()
                                .group($);
                    }
                }
                Criteria part = Criteria.where().group(typePart).and()
                        .group(linkPart);
                if(condition == null) {
                    condition = part;
                }
                else {
                    condition = Criteria.where().group(condition).or()
                            .group(part);
                }
            }
            return condition;
        }

        /**
         * Determine whether the specified {@code type} is compatible with or
         * assignable to the given {@code field} within {@code clazz}. This
         * check includes both direct assignment compatibility as well as
         * compatibility with the field's type arguments.
         * 
         * @param type the {@link Record} type to check for compatibility with
         *            the field
         * @param clazz the class that contains the {@code field}
         * @param field the {@link Field} in {@code clazz} to check against
         * @return {@code true} if {@code type} is compatible with the field, or
         *         {@code false} otherwise
         */
        private boolean isTypeCompatibleWithClassField(
                Class<? extends Record> type, Class<? extends Record> clazz,
                Field field) {
            if(field.getType().isAssignableFrom(type)) {
                return true;
            }
            else {
                for (Class<?> typeArg : getTypeArguments(clazz,
                        field.getName())) {
                    if(typeArg.isAssignableFrom(type)) {
                        return true;
                    }
                }
            }
            return false;
        }

    }

    /**
     * A read-only {@link Map} that ensures that the values of another
     * {@link Map} are wrapped in a {@link Collection}.
     * <p>
     * If the input {@link Map} associates a key to a scalar value, that value
     * is added to a {@link Collection}. Otherwise, if the associated value is a
     * {@link Sequence}, the items within it are represented using a
     * {@link Collection}.
     * </p>
     *
     * @author Jeff Nelson
     */
    private static class CollectionValueWrapperMap<K>
            extends AbstractMap<K, Collection<Object>> {

        private final Map<K, Object> data;

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        public CollectionValueWrapperMap(Map<K, Object> data) {
            this.data = data;
        }

        @Override
        public boolean containsKey(Object key) {
            return data.containsKey(key);
        }

        @Override
        public Set<Entry<K, Collection<Object>>> entrySet() {
            return keySet().stream()
                    .map(key -> new AbstractMap.SimpleImmutableEntry<>(key,
                            get(key)))
                    .collect(Collectors.toCollection(
                            () -> new LinkedHashSet<>(data.size())));
        }

        @SuppressWarnings("unchecked")
        @Override
        public Collection<Object> get(Object key) {
            Object value = data.get(key);
            if(value instanceof Collection) {
                return (Collection<Object>) value;
            }
            else if(Sequences.isSequence(value)) {
                return Sequences.stream(value).collect(Collectors.toList());
            }
            else if(value == null) {
                return ImmutableList.of();
            }
            else {
                return ImmutableList.of(value);
            }
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public Set<K> keySet() {
            return data.keySet();
        }

        @Override
        public Collection<Object> put(K key, Collection<Object> value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Object> remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return data.size();
        }

    };

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
                Order order, Page page, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria, order, page,
                        realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
                Order order, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria, order, realms);
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
        public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
                Page page, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria, page, realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> find(Class<T> clazz, Criteria criteria,
                Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.find(clazz, criteria, realms);
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
                Criteria criteria, Order order, Page page, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria, order, page,
                        realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> findAny(Class<T> clazz,
                Criteria criteria, Order order, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria, order, realms);
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
        public <T extends Record> Set<T> findAny(Class<T> clazz,
                Criteria criteria, Page page, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria, page, realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> findAny(Class<T> clazz,
                Criteria criteria, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.findAny(clazz, criteria, realms);
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
        public <T extends Record> T findAnyUnique(Class<T> clazz,
                Criteria criteria, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.findAnyUnique(clazz, criteria, realms);
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
        public <T extends Record> T findUnique(Class<T> clazz,
                Criteria criteria, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.findUnique(clazz, criteria, realms);
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
        public <T extends Record> T load(Class<T> clazz, long id,
                Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, id, realms);
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
        public <T extends Record> Set<T> load(Class<T> clazz, Order order,
                Page page, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, order, page, realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> load(Class<T> clazz, Order order,
                Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, order, realms);
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
        public <T extends Record> Set<T> load(Class<T> clazz, Page page,
                Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, page, realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> load(Class<T> clazz, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.load(clazz, realms);
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
        public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
                Page page, Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz, order, page, realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> loadAny(Class<T> clazz, Order order,
                Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz, order, realms);
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

        @Override
        public <T extends Record> Set<T> loadAny(Class<T> clazz, Page page,
                Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz, page, realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

        @Override
        public <T extends Record> Set<T> loadAny(Class<T> clazz,
                Realms realms) {
            if(tracked.runway != null) {
                return tracked.runway.loadAny(clazz, realms);
            }
            else {
                throw new UnsupportedOperationException(
                        "No database interface has been assigned to this Record");
            }
        }

    }

    /**
     * A read-only {@link Multimap} interface for a {@link Map} where each key
     * is associated with a {@link Collection} of values.
     * <p>
     * A {@link SyntheticMultimap} allows for treating a {@link Map} with
     * {@link Collection} values as a {@link Multimap} without explicitly
     * copying the data over.
     * </p>
     *
     * @author Jeff Nelson
     */
    private static class SyntheticMultimap<K, V> implements Multimap<K, V> {

        /**
         * The map that this interface treats like a {@link Multimap}.
         */
        private final Map<K, Collection<V>> data;

        /**
         * Construct a new instance.
         * 
         * @param data
         */
        public SyntheticMultimap(Map<K, Collection<V>> data) {
            this.data = data;
        }

        @Override
        public Map<K, Collection<V>> asMap() {
            return data;
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsEntry(Object key, Object value) {
            Collection<V> values = data.getOrDefault(key, ImmutableSet.of());
            return values.contains(value);
        }

        @Override
        public boolean containsKey(Object key) {
            return data.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            for (Entry<K, Collection<V>> entry : data.entrySet()) {
                for (V v : entry.getValue()) {
                    if(v.equals(value)) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public Collection<Entry<K, V>> entries() {
            List<Entry<K, V>> entries = new ArrayList<>();
            for (Entry<K, Collection<V>> entry : data.entrySet()) {
                K key = entry.getKey();
                for (V value : entry.getValue()) {
                    entries.add(
                            new AbstractMap.SimpleImmutableEntry<>(key, value));
                }
            }
            return entries;
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Multimap) {
                Multimap<?, ?> mmap = (Multimap<?, ?>) obj;
                return mmap.asMap().equals(asMap());
            }
            else {
                return false;
            }
        }

        @Override
        public Collection<V> get(K key) {
            return data.get(key);
        }

        @Override
        public int hashCode() {
            return data.hashCode();
        }

        @Override
        public boolean isEmpty() {
            return data.isEmpty();
        }

        @Override
        public Multiset<K> keys() {
            Multiset<K> keys = LinkedHashMultiset.create();
            for (Entry<K, Collection<V>> entry : data.entrySet()) {
                keys.add(entry.getKey(), entry.getValue().size());
            }
            return keys;
        }

        @Override
        public Set<K> keySet() {
            return data.keySet();
        }

        @Override
        public boolean put(K key, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean putAll(K key, Iterable<? extends V> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean putAll(Multimap<? extends K, ? extends V> multimap) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<V> removeAll(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<V> replaceValues(K key,
                Iterable<? extends V> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return data.size();
        }

        @Override
        public String toString() {
            return data.toString();
        }

        @Override
        public Collection<V> values() {
            return data.values().stream().flatMap(values -> values.stream())
                    .collect(Collectors.toList());
        }

    }

}
