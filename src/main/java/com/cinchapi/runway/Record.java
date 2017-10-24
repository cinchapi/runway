package com.cinchapi.runway;

import gnu.trove.map.TLongObjectMap;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

import com.cinchapi.common.base.AnyObjects;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.server.io.Serializables;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;
import com.cinchapi.concourse.util.ByteBuffers;
import com.cinchapi.runway.validation.Validator;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

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
 * And, private fields are never included in a JSON {@link #dump()} or
 * {@link #toString()} output.
 * </p>
 * <h2>Creating a new Instance</h2>
 * <p>
 * Records are created by calling the {@link Record#create(Class)} method and
 * supplying the desired class for the instance. Internally, the create routine
 * calls the no-arg constructor so subclasses should never provide their own
 * constructors.
 * </p>
 * 
 * @author jnelson
 * 
 */
public abstract class Record {

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
     * Convert a generic {@code object} to the appropriate {@link JsonElement}.
     * <p>
     * <em>This method accepts {@link Iterable} collections and recursively
     * transforms them to JSON arrays.</em>
     * </p>
     * 
     * @param object
     * @return the appropriate JsonElement
     */
    private static JsonElement jsonify(Object object, Set<Record> seen) {
        if(object instanceof Iterable
                && Iterables.size((Iterable<?>) object) == 1) {
            return jsonify(Iterables.getOnlyElement((Iterable<?>) object),
                    seen);
        }
        else if(object instanceof Iterable) {
            JsonArray array = new JsonArray();
            for (Object element : (Iterable<?>) object) {
                array.add(jsonify(element, seen));
            }
            return array;
        }
        else if(object instanceof Number) {
            return new JsonPrimitive((Number) object);
        }
        else if(object instanceof Boolean) {
            return new JsonPrimitive((Boolean) object);
        }
        else if(object instanceof String) {
            return new JsonPrimitive((String) object);
        }
        else if(object instanceof Tag) {
            return new JsonPrimitive((String) object.toString());
        }
        else if(object instanceof Record) {
            if(!seen.contains(object)) {
                seen.add((Record) object);
                return ((Record) object).toJsonElement(seen);
            }
            else {
                return jsonify(((Record) object).id() + " (recursive link)",
                        seen);
            }
        }
        else {
            Gson gson = new Gson();
            return gson.toJsonTree(object);
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

    public Record() {
        this.id = Time.now();
    }

    /**
     * Delete this {@link Record} from Concourse when the {@link #save()} method
     * is called.
     */
    public void deleteOnSave() {
        deleted = true;
    }

    /**
     * Dump the non private data in this {@link Record} as a JSON string.
     * 
     * @return the JSON string
     */
    public String dump() {
        return toJsonElement().toString();
    }

    /**
     * Dump the non private specified {@code} keys in this {@link Record} as a
     * JSON string.
     * 
     * @param keys
     * @return the json string
     */
    public String dump(String... keys) {
        return toJsonElement(keys).toString();
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
                value = Reflection.get(key, this);
            }
            catch (Exception e) {/* ignore */}
        }
        return (T) value;
    }

    /**
     * Return a map that contains all of the non-private data in this record.
     * For example, this method can be used to return data that can be sent to a
     * template processor to map values to front-end variables. You can use the
     * {@link #getMoreData()} method to define additional data values.
     * 
     * @return the data in this record
     */
    public Map<String, Object> getData() {
        try {
            Map<String, Object> data = getMoreData();
            Field[] fields = Reflection.getAllDeclaredFields(this);
            data.put("id", id);
            for (Field field : fields) {
                Object value;
                if(!Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && (value = field.get(this)) != null) {
                    data.put(field.getName(), value);
                }
            }
            data.putAll(dynamicData);
            return data;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return a map that contains all of the non-private data in this record
     * based on the specified {@code keys}. For example, this method can be used
     * to return data that can be sent to a template processor to map values to
     * front-end variables. You can use the {@link #getMoreData()} method to
     * define
     * additional data values, and the keys that map to those values will only
     * be returned if they are included in {@code keys}.
     * 
     * @return the data in this record
     */
    public Map<String, Object> getData(String... keys) {
        try {
            Map<String, Object> data = getMoreData();
            Set<String> _keys = Sets.newHashSet(keys);
            for (String key : data.keySet()) {
                if(!_keys.contains(key)) {
                    data.remove(key);
                }
            }
            Field[] fields = Reflection.getAllDeclaredFields(this);
            data.put("id", id);
            for (Field field : fields) {
                Object value;
                if(_keys.contains(field.getName())
                        && !Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && (value = field.get(this)) != null) {
                    data.put(field.getName(), value);
                }
            }
            data.putAll(dynamicData);
            return data;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
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

    public final String section() {
        return this.getClass().getName();
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

    /**
     * Returns a {@link JsonElement} representation of this record which
     * includes all of its non private fields.
     * 
     * @return the JsonElement representation
     */
    public JsonElement toJsonElement() {
        return toJsonElement(Sets.<Record> newHashSet());
    }

    /**
     * Returns a {@link JsonElement} representation of this record which
     * includes all of the non private {@code keys} that are specified.
     * 
     * @param keys
     * @return the JsonElement representation
     */
    public JsonElement toJsonElement(String... keys) {
        return toJsonElement(Sets.<Record> newHashSet(), keys);
    }

    @Override
    public final String toString() {
        return dump();
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
        try {
            if(inZombieState(id, concourse)) {
                concourse.clear(id);
                throw new ZombieException();
            }
            // TODO: do a large select and populate the fields instead of
            // doing individual gets
            Field[] fields = Reflection.getAllDeclaredFields(this);
            for (Field field : fields) {
                if(!Modifier.isTransient(field.getModifiers())) {
                    String key = field.getName();
                    if(Record.class.isAssignableFrom(field.getType())) {
                        Link link = ((Link) concourse.get(key, id));
                        if(link != null) {
                            long linkedId = link.longValue();
                            Record record = (Record) Reflection.newInstance(
                                    field.getType(), linkedId, existing);
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
                                        item = Reflection.newInstance(linkClass,
                                                link, existing);
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
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }

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
                saveUnsafe(concourse);
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
    /* package */ void saveUnsafe(final Concourse concourse) {
        try {
            concourse.verifyOrSet(SECTION_KEY, section(), id);
            Field[] fields = Reflection.getAllDeclaredFields(this);
            for (Field field : fields) {
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
            dynamicData.forEach((key, value) -> {
                store(key, value, concourse, false);
            });
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Provide additional data about this Record that might not be encapsulated
     * in its fields. For example, this is a good way to provide template
     * specific information that isn't persisted to the database.
     * 
     * @return the additional data
     */
    protected Map<String, Object> getMoreData() {
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
            checkState(section != null);
            checkState(
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

    /**
     * Returns a {@link JsonElement} representation of this record which
     * includes all of its non private fields.
     * 
     * @param seen - the records that have been previously serialized, so we
     *            don't recurse infinitely
     * 
     * @return the JsonElement representation
     */
    private JsonElement toJsonElement(Set<Record> seen) {
        try {
            Field[] fields = Reflection.getAllDeclaredFields(this);
            JsonObject json = new JsonObject();
            json.addProperty("id", id);
            Map<String, Object> more = getMoreData();
            for (String key : more.keySet()) {
                json.add(key, jsonify(more.get(key), seen));
            }
            for (Field field : fields) {
                Object value = field.get(this);
                if(!Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && value != null) {
                    json.add(field.getName(), jsonify(value, seen));
                }
            }
            dynamicData.forEach((key, value) -> {
                json.add(key, jsonify(value, seen));
            });
            return json;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Returns a {@link JsonElement} representation of this record which
     * includes all of the non private {@code keys} that are specified.
     * 
     * @param keys
     * @param seen - the records that have been previously serialized, so we
     *            don't recurse infinitely
     * @return the JsonElement representation
     */
    private JsonElement toJsonElement(Set<Record> seen, String... keys) {
        try {
            Set<String> _keys = Sets.newHashSet(keys);
            Field[] fields = Reflection.getAllDeclaredFields(this);
            JsonObject json = new JsonObject();
            json.addProperty("id", id);
            Map<String, Object> more = getMoreData();
            for (String key : more.keySet()) {
                if(_keys.contains(key)) {
                    json.add(key, jsonify(more.get(key), seen));
                }
            }
            for (Field field : fields) {
                Object value;
                if(_keys.contains(field.getName())
                        && !Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && (value = field.get(this)) != null) {
                    json.add(field.getName(), jsonify(value, seen));
                }
            }
            dynamicData.forEach((key, value) -> {
                json.add(key, jsonify(value, seen));
            });
            return json;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }
}
