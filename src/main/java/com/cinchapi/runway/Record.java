package com.cinchapi.runway;

import gnu.trove.map.TLongObjectMap;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Verify;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.ConnectionPool;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.server.io.Serializables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.concourse.util.TypeAdapters;
import com.cinchapi.runway.json.JsonTypeWriter;
import com.cinchapi.runway.validation.Validator;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
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
@SuppressWarnings("restriction")
public abstract class Record {

    /**
     * Instance of {@link sun.misc.Unsafe} to use for hacky operations.
     */
    private static final sun.misc.Unsafe unsafe = Reflection
            .getStatic("theUnsafe", sun.misc.Unsafe.class);

    /* package */ static Runway PINNED_RUNWAY_INSTANCE = null;

    /**
     * The key used to hold the section metadata.
     */
    /* package */ static final String SECTION_KEY = "_"; // just want a
                                                         // simple/short key
                                                         // name that is likely
                                                         // to avoid collisions

    /**
     * The description of a record that is considered to be in "zombie" state.
     */
    private static final Set<String> ZOMBIE_DESCRIPTION = Sets
            .newHashSet(SECTION_KEY);

    private static long NULL_ID = -1;

    /**
     * The {@link Field fields} that are defined in the base class.
     */
    private static Set<Field> INTERNAL_FIELDS = Sets.newHashSet(
            Arrays.asList(Reflection.getAllDeclaredFields(Record.class)));

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
            TLongObjectMap<Record> existing, ConnectionPool connections) {
        Concourse concourse = connections.request();
        try {
            return load(clazz, id, existing, connections, concourse);
        }
        finally {
            connections.release(concourse);
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
    private static boolean inZombieState(long id, Concourse concourse) {
        return concourse.describe(id).equals(ZOMBIE_DESCRIPTION);
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
            Concourse concourse) {
        T record = (T) newDefaultInstance(clazz, connections);
        Reflection.set("id", id, record); /* (authorized) */
        record.load(concourse, existing);
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
            return instance;
        }
        catch (InstantiationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * The variable that holds the name of the section in the database where
     * this record is stored.
     */
    private transient String __ = getClass().getName();

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
     * A log of any suppressed errors related to this Record. The descriptions
     * of these errors can be thrown at any point from the
     * {@link #throwSupressedExceptions()} method.
     */
    /* package */ transient List<String> errors = Lists.newArrayList();

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
     * The {@link Concourse} database in which this {@link Record} is stored.
     */
    private ConnectionPool connections = null;

    /**
     * Construct a new instance.
     * 
     * @param concourse
     */
    public Record() {
        this.id = Time.now();
        if(PINNED_RUNWAY_INSTANCE != null) {
            this.connections = PINNED_RUNWAY_INSTANCE.connections;
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
        connections = runway.connections;
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
        Object value = dynamicData.get(key);
        if(value == null) {
            try {
                Field field = Reflection.getDeclaredField(key, this);
                if(isReadableField(field)) {
                    return (T) field.get(this);
                }
            }
            catch (Exception e) {/* ignore */}
        }
        if(value == null) {
            value = tempData().get(key);
        }
        return (T) value;
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
     */
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
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data.
     * 
     * @return json string
     */
    public String json() {
        return json(Sets.newHashSet());
    }

    /**
     * Return a JSON string containing this {@link Record}'s readable and
     * temporary data from the specified {@code keys}.
     * 
     * @return json string
     */
    public String json(String... keys) {
        return json(Sets.newHashSet(), keys);
    }

    /**
     * Return a map that contains all of the readable data in this record.
     * <p>
     * If you only want to return specific fields, use the
     * {@link #get(String...)} method.
     * </p>
     * 
     * @return the data in this record
     */
    public Map<String, Object> map() {
        Map<String, Object> data = Maps.newHashMap(tempData());
        fields().forEach(field -> {
            try {
                Object value;
                if(isReadableField(field)
                        && (value = field.get(this)) != null) {
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
     * Save any changes made to this {@link Record}.
     * <p>
     * <strong>NOTE:</strong> This method recursively saves any linked
     * {@link Records}.
     * </p>
     */
    public boolean save() {
        Verify.that(connections != null,
                "Cannot perform an implicit save because this Record isn't pinned to a Concourse instance");
        Concourse concourse = connections.request();
        try {
            return save(concourse);
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
                dynamicData.put(key, value);
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
            StringBuilder sb = new StringBuilder();
            for (String error : errors) {
                sb.append(error);
                sb.append(System.getProperty("line.separator"));
            }
            throw new RuntimeException(sb.toString());
        }
    }

    @Override
    public final String toString() {
        return json();
    }

    /**
     * Load an existing record from the database and add all of it to this
     * instance in memory.
     * 
     * @param runway
     * @param existing
     */
    @SuppressWarnings({ "unchecked", "rawtypes", })
    /* package */ final void load(Concourse concourse,
            TLongObjectMap<Record> existing) {
        Preconditions.checkState(id != NULL_ID);
        existing.put(id, this); // add the current object so we don't
                                // recurse infinitely
        checkConstraints(concourse);
        if(inZombieState(id, concourse)) {
            concourse.clear(id);
            throw new ZombieException();
        }
        // TODO: do a large select and populate the fields instead of
        // doing individual gets
        fields().forEach(field -> {
            try {
                if(!Modifier.isTransient(field.getModifiers())) {
                    String key = field.getName();
                    if(Record.class.isAssignableFrom(field.getType())) {
                        Link link = ((Link) concourse.get(key, id));
                        if(link != null) {
                            long linkedId = link.longValue();
                            Record record = load(field.getType(), linkedId,
                                    existing, connections, concourse);
                            field.set(this, record);
                        }
                    }
                    else if(Collection.class
                            .isAssignableFrom(field.getType())) {
                        Collection collection = null;
                        if(Modifier.isAbstract(field.getType().getModifiers())
                                || Modifier.isInterface(
                                        field.getType().getModifiers())) {
                            if(field.getType() == Set.class) {
                                collection = Sets.newLinkedHashSet();
                            }
                            else { // assume list
                                collection = Lists.newArrayList();
                            }
                        }
                        else {
                            collection = (Collection) field.getType()
                                    .newInstance();
                        }
                        Set<?> values = concourse.select(key, id);
                        for (Object item : values) {
                            if(item instanceof Link) {
                                long link = ((Link) item).longValue();
                                Record obj = existing.get(link);
                                if(obj == null) {
                                    String section = concourse.get(SECTION_KEY,
                                            link);
                                    if(Strings.isNullOrEmpty(section)) {
                                        concourse.remove(key, item, id);
                                        continue;
                                    }
                                    else {
                                        Class<? extends Record> linkClass = (Class<? extends Record>) Class
                                                .forName(section.toString());
                                        item = load(linkClass, link, existing,
                                                connections, concourse);
                                    }
                                }
                                else {
                                    item = obj;
                                }
                            }
                            collection.add(item);
                        }
                        field.set(this, collection);
                    }
                    else if(field.getType().isArray()) {
                        List list = new ArrayList();
                        Set<?> values = concourse.select(key, id);
                        for (Object item : values) {
                            list.add(item);
                        }
                        field.set(this, list.toArray());
                    }
                    else if(field.getType().isPrimitive()
                            || field.getType() == String.class
                            || field.getType() == Integer.class
                            || field.getType() == Long.class
                            || field.getType() == Float.class
                            || field.getType() == Double.class
                            || field.getType() == Boolean.class) {
                        Object value = concourse.get(key, id);
                        if(value != null) { // Java doesn't allow primitive
                                            // types to hold nulls
                            field.set(this, concourse.get(key, id));
                        }
                    }
                    else if(field.getType() == Tag.class) {
                        Object object = concourse.get(key, id);
                        if(object != null) {
                            field.set(this, Tag.create((String) object));
                        }
                    }
                    else if(field.getType().isEnum()) {
                        String stored = concourse.get(key, id);
                        if(stored != null) {
                            field.set(this,
                                    Enum.valueOf((Class<Enum>) field.getType(),
                                            stored.toString()));
                        }
                    }
                    else if(Serializable.class
                            .isAssignableFrom(field.getType())) {
                        String base64 = concourse.get(key, id);
                        if(base64 != null) {
                            ByteBuffer bytes = ByteBuffer.wrap(
                                    BaseEncoding.base64Url().decode(base64));
                            field.set(this, Serializables.read(bytes,
                                    (Class<Serializable>) field.getType()));
                        }
                    }
                    else {
                        Gson gson = new Gson();
                        Object object = gson.fromJson(
                                (String) concourse.get(key, id),
                                field.getType());
                        field.set(this, object);
                    }
                }
            }
            catch (ReflectiveOperationException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        });

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
    /* package */ final boolean save(Concourse concourse) {
        try {
            Preconditions.checkState(!inViolation);
            errors.clear();
            concourse.stage();
            if(deleted) {
                delete(concourse);
            }
            else {
                saveWithinTransaction(concourse);
            }
            return concourse.commit();
        }
        catch (Throwable t) {
            concourse.abort();
            if(inZombieState(concourse)) {
                concourse.clear(id);
            }
            errors.add(Throwables.getStackTraceAsString(t));
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
    /* package */ void saveWithinTransaction(final Concourse concourse) {
        concourse.verifyOrSet(SECTION_KEY, __, id);
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
                        Preconditions.checkState(
                                isUnique(concourse, key, value),
                                field.getName() + " must be unique");
                    }
                    if(field.isAnnotationPresent(Required.class)) {
                        Preconditions.checkState(
                                !AnyObjects.isNullOrEmpty(value),
                                field.getName() + " is required");
                    }
                    if(value != null) {
                        store(key, value, concourse, false);
                    }
                }
            }
            catch (ReflectiveOperationException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        });

    }

    /**
     * Return additional {@link JsonTypeWriter JsonTypeWriters} that should be
     * use when generating the {@link #json()} for this {@link Record}.
     * 
     * @return a mapping from a {@link Class} to a corresponding
     *         {@link JsonTypeWriter}.
     */
    protected Map<Class<?>, JsonTypeWriter<?>> jsonTypeHierarchyWriters() {
        return Maps.newHashMap();
    }

    /**
     * Return additional {@link JsonTypeWriter JsonTypeWriters} that should be
     * use when generating the {@link #json()} for this {@link Record}.
     * 
     * @return a mapping from a {@link Class} to a corresponding
     *         {@link JsonTypeWriter}.
     */
    protected Map<Class<?>, JsonTypeWriter<?>> jsonTypeWriters() {
        return Maps.newHashMap();
    }

    /**
     * Provide additional data about this Record that might not be encapsulated
     * in its fields. For example, this is a good way to provide template
     * specific information that isn't persisted to the database.
     * 
     * @return the additional data
     */
    protected Map<String, Object> tempData() {
        return Maps.newHashMap();
    }

    /**
     * Check to ensure that this Record does not violate any constraints. If so,
     * throw an {@link IllegalStateException}.
     * 
     * @param concourse
     * @throws IllegalStateException
     */
    private void checkConstraints(Concourse concourse) {
        try {
            String section = concourse.get(SECTION_KEY, id);
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
            throw Throwables.propagate(e);
        }
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
        return Arrays.asList(Reflection.getAllDeclaredFields(this)).stream()
                .filter(field -> !INTERNAL_FIELDS.contains(field))
                .collect(Collectors.toSet());
    }

    /**
     * Return {@code true} if this record is in a "zombie" state meaning it
     * exists in the database without any actual data.
     * 
     * @param concourse
     * @return {@code true} if this record is a zombie
     */
    private final boolean inZombieState(Concourse concourse) {
        return inZombieState(id, concourse);
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
        if(value instanceof Iterable<?> || value.getClass().isArray()) {
            for (Object obj : (Iterable<?>) value) {
                if(!isUnique(concourse, key, obj)) {
                    return false;
                }
            }
            return true;
        }
        else {
            Criteria criteria = Criteria.where().key(SECTION_KEY)
                    .operator(Operator.EQUALS).value(getClass().getName()).and()
                    .key(key).operator(Operator.EQUALS).value(value).build();
            Set<Long> records = concourse.find(criteria);
            return records.isEmpty()
                    || (records.contains(id) && records.size() == 1);
        }
    }

    private String json(Set<Record> seen, String... keys) {
        Map<String, Object> data = keys.length == 0 ? map() : get(keys);

        // Create a dynamic type adapter that will detect recursive links and
        // prevent infinite recursion when trying to generate the json.
        TypeAdapter<Record> recordTypeAdapter = new TypeAdapter<Record>() {

            @Override
            public Record read(JsonReader in) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void write(JsonWriter out, Record value) throws IOException {
                if(!seen.contains(value)) {
                    seen.add(value);
                    out.jsonValue(value.json(seen));
                }
                else {
                    out.value(value.id() + " (recursive link)");
                }
            }

        };
        // TODO: write custom type adapters...
        GsonBuilder builder = new GsonBuilder()
                .registerTypeAdapter(Object.class,
                        TypeAdapters.forGenericObject().nullSafe())
                .registerTypeAdapter(TObject.class,
                        TypeAdapters.forTObject().nullSafe())
                .registerTypeHierarchyAdapter(Record.class,
                        recordTypeAdapter.nullSafe())
                .disableHtmlEscaping();
        jsonTypeWriters().forEach((clazz, writer) -> {
            builder.registerTypeAdapter(clazz, writer.typeAdapter().nullSafe());
        });
        jsonTypeHierarchyWriters().forEach((clazz, writer) -> {
            builder.registerTypeHierarchyAdapter(clazz,
                    writer.typeAdapter().nullSafe());
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
            boolean append) {
        // TODO: dirty field detection!
        if(value instanceof Record) {
            Record record = (Record) value;
            record.saveWithinTransaction(concourse);
            concourse.link(key, record.id, id);
        }
        else if(value instanceof Collection || value.getClass().isArray()) {
            // TODO use reconcile() function once 0.5.0 comes out...
            concourse.clear(key, id); // TODO this is extreme...move to a diff
                                      // based approach to delete only values
                                      // that should be deleted
            for (Object item : (Iterable<?>) value) {
                store(key, item, concourse, true);
            }
        }
        else if(value.getClass().isPrimitive() || value instanceof String
                || value instanceof Tag || value instanceof Link
                || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double
                || value instanceof Boolean) {
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
            store(key, base64, concourse, append);
        }
        else {
            Gson gson = new Gson();
            Tag json = Tag.create(gson.toJson(value));
            store(key, json, concourse, append);
        }
    }

}
