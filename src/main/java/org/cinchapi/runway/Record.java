package org.cinchapi.runway;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.ConnectionPool;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.server.io.Serializables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.util.ByteBuffers;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * A {@link Record} is a a wrapper around a record in {@link Concourse} that
 * facilitates object-oriented interaction while preserving transactional
 * security.
 * <p>
 * Each subclass should define its schema through {@code non-transient} member
 * variables. When a Record is loaded from Concourse, the member variables will
 * be automatically populated with the information in the database. And, when a
 * Record is persisted using the {@link #save()} method, the values of those
 * variables will automatically be stored/updated in Concourse.
 * </p>
 * <h2>Variable Modifiers</h2>
 * <p>
 * Records will respect the native java modifiers placed on variables. This
 * means that transient fields are never stored or loaded from the database.
 * Additionally, private fields are never included in a JSON {@link #dump()} or
 * {@link #toString()} output.
 * </p>
 * 
 * @author jnelson
 * 
 */
public abstract class Record {

    /**
     * Create a new {@link Record} that is contained within the specified
     * {@code clazz}.
     * 
     * @param clazz
     * @return the new Record
     */
    public static <T extends Record> T create(Class<T> clazz) {
        T record = getNewDefaultInstance(clazz);
        record.init();
        return record;
    }

    /**
     * Find any return any records in {@code clazz} that match {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the records that match {@code criteria}
     */
    public static <T extends Record> Set<T> find(Class<T> clazz,
            Criteria criteria) {
        Concourse concourse = connections().request();
        try {
            Set<T> records = Sets.newLinkedHashSet();
            Set<Long> ids = concourse.find(criteria);
            for (long id : ids) {
                if(inClass(id, clazz, concourse)) {
                    records.add(load(clazz, id));
                }
            }
            return records;
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Find and return all records that match {@code criteria}, regardless of
     * what class the Record is contained within.
     * 
     * @param criteria
     * @return the records that match {@code criteria}
     */
    @SuppressWarnings("unchecked")
    public static Set<Record> findAll(Criteria criteria) {
        Concourse concourse = connections().request();
        try {
            Set<Record> records = Sets.newLinkedHashSet();
            Set<Long> ids = concourse.find(criteria);
            for (long id : ids) {
                Class<? extends Record> clazz = (Class<? extends Record>) Class
                        .forName((String) concourse.get(SECTION_KEY, id));
                records.add(load(clazz, id));
            }
            return records;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Find and return every record in {@code clazz} or any of its children that
     * match {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the records that match {@code criteria}
     */
    @SuppressWarnings("unchecked")
    public static <T extends Record> Set<? extends T> findEvery(Class<T> clazz,
            Criteria criteria) {
        Concourse concourse = connections().request();
        try {
            Set<T> records = Sets.newLinkedHashSet();
            Set<Long> ids = concourse.find(criteria);
            for (long id : ids) {
                Class<? extends T> c = (Class<? extends T>) Class
                        .forName((String) concourse.get(SECTION_KEY, id));
                if(clazz.isAssignableFrom(c)) {
                    records.add(load(c, id));
                }
            }
            return records;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Search and return every records in {@code clazz} or any of its children
     * that match the search {@code query} for {@code key}.
     * 
     * @param clazz
     * @param key
     * @param query
     * @return the records that match the search
     */
    @SuppressWarnings("unchecked")
    public static <T extends Record> Set<? extends T> findEvery(Class<T> clazz,
            String key, String query) {
        Concourse concourse = connections().request();
        try {
            Set<T> records = Sets.newLinkedHashSet();
            Set<Long> ids = concourse.search(key, query);
            for (long id : ids) {
                Class<? extends T> c = (Class<? extends T>) Class
                        .forName((String) concourse.get(SECTION_KEY, id));
                if(clazz.isAssignableFrom(c)) {
                    records.add(load(c, id));
                }
            }
            return records;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Load the Record that is contained within the specified {@code clazz} and
     * has the specified {@code id}.
     * <p>
     * Multiple calls to this method with the same parameters will return
     * <strong>different</strong> instances (e.g. the instances are not cached).
     * This is done deliberately so different threads/clients can make changes
     * to a Record in isolation.
     * </p>
     * 
     * @param clazz
     * @param id
     * @return the existing Record
     */
    public static <T extends Record> T load(Class<T> clazz, long id) {
        T record = getNewDefaultInstance(clazz);
        record.load(id);
        return record;
    }

    /**
     * Save all the changes in all of the {@code records} using a single ACID
     * transaction.
     * 
     * @param records
     * @return {@code true} if all the changes are atomically saved.
     */
    public static boolean saveAll(Record... records) {
        Concourse concourse = connections().request();
        try {
            concourse.stage();
            for (Record record : records) {
                record.save(concourse);
            }
            return concourse.commit();
        }
        catch (Throwable t) {
            concourse.abort();
            return false;
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Search and return any records in {@code clazz} that match the search
     * {@code query} for {@code key}.
     * 
     * @param clazz
     * @param key
     * @param query
     * @return the records that match the search
     */
    public static <T extends Record> Set<T> search(Class<T> clazz, String key,
            String query) {
        Concourse concourse = connections().request();
        try {
            Set<T> records = Sets.newLinkedHashSet();
            Set<Long> ids = concourse.search(key, query);
            for (long id : ids) {
                if(inClass(id, clazz, concourse)) {
                    records.add(load(clazz, id));
                }
            }
            return records;
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Search and return all records that match the search {@code query} for
     * {@code key}, regardless of what class the Record is contained within.
     * 
     * @param key
     * @param query
     * @return the records that match the search
     */
    @SuppressWarnings("unchecked")
    public static Set<Record> searchAll(String key, String query) {
        Concourse concourse = connections().request();
        try {
            Set<Record> records = Sets.newLinkedHashSet();
            Set<Long> ids = concourse.search(key, query);
            for (long id : ids) {
                Class<? extends Record> clazz = (Class<? extends Record>) Class
                        .forName((String) concourse.get(SECTION_KEY, id));
                records.add(load(clazz, id));
            }
            return records;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Set the connection information that is used for Concourse.
     * 
     * @param host
     * @param port
     * @param username
     * @param password
     */
    protected static void setConnectionInformation(String host, int port,
            String username, String password) { // visible for testing
        connections = ConnectionPool.newCachedConnectionPool(host, port,
                username, password);
    }

    /**
     * Return a handler to the connection pool that is in use.
     * 
     * @return the connection pool handler
     */
    private static ConnectionPool connections() {
        if(connections == null) {
            connections = ConnectionPool
                    .newCachedConnectionPool("concourse_client.prefs");
        }
        return connections;
    }

    /**
     * Get a new instance of {@code clazz} by calling the default (zero-arg)
     * constructor, if it exists. This method attempts to correctly invoke
     * constructors for nester inner classes.
     * 
     * @param clazz
     * @return the instance of the {@code clazz}.
     */
    @SuppressWarnings("unchecked")
    private static <T> T getNewDefaultInstance(Class<T> clazz) {
        try {
            Class<?> enclosingClass = clazz.getEnclosingClass();
            if(enclosingClass != null) {
                Object enclosingInstance = getNewDefaultInstance(enclosingClass);
                Constructor<?> constructor = clazz
                        .getDeclaredConstructor(enclosingClass);
                return (T) constructor.newInstance(enclosingInstance);

            }
            else {
                return clazz.newInstance();
            }
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return {@code true} if the Record identified by {@code id} is a member of
     * {@code clazz}.
     * 
     * @param id
     * @param clazz
     * @param concourse
     * @return {@code true} if the record is in the class
     */
    private static boolean inClass(long id, Class<? extends Record> clazz,
            @Nullable Concourse concourse) {
        if(concourse == null) {
            concourse = connections().request();
            try {
                return inClass(id, clazz, concourse);
            }
            finally {
                connections().release(concourse);
            }
        }
        else {
            return ((String) concourse.get(SECTION_KEY, id)).equals(clazz
                    .getName());
        }
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
    private static JsonElement jsonify(Object object) {
        if(object instanceof Iterable
                && Iterables.size((Iterable<?>) object) == 1) {
            return jsonify(Iterables.getOnlyElement((Iterable<?>) object));
        }
        else if(object instanceof Iterable) {
            JsonArray array = new JsonArray();
            for (Object element : (Iterable<?>) object) {
                array.add(jsonify(element));
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
        else {
            Gson gson = new Gson();
            return gson.toJsonTree(object);
        }
    }

    /**
     * The connection pool. Access using the {@link #connections()} method.
     */
    private static ConnectionPool connections;

    /**
     * The key used to hold the section metadata.
     */
    private static final String SECTION_KEY = "_"; // just want a simple/short
                                                   // key name that is likely to
                                                   // avoid collisions

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    connections.close();
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }

    /**
     * The variable that holds the name of the section in the database where
     * this record is stored.
     */
    private transient String _ = getClass().getName();

    /**
     * A cache of all the fields in this class and all of its parents.
     */
    private transient Field[] fields0;

    /**
     * The primary key that is used to identify this Record in the database.
     */
    private transient long id;

    /**
     * A flag that indicates this Record is in violation of some constraint and
     * therefore cannot be used without ruining the integrity of the database.
     */
    private transient boolean inViolation = false;

    /**
     * A flag that indicates that the Record object has either been initialized
     * as a new record in the database or has loaded an existing record and is
     * therefore usable.
     */
    private transient boolean usable = false;

    /**
     * Create a new Record instance. In order for this to be operable, a call
     * must be made to either {@link #init()} or {@link #load(long)}.
     */
    protected Record() {/* noop */}

    /**
     * Dump the non private data in this {@link Record} as a JSON document.
     * 
     * @return the json dump
     */
    public String dump() {
        try {
            Field[] fields = getAllDeclaredFields();
            JsonObject json = new JsonObject();
            json.addProperty("id", id);
            for (Field field : fields) {
                Object value;
                if(!Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && (value = field.get(this)) != null) {
                    json.add(field.getName(), jsonify(value));
                }
            }
            return json.toString();
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Dump the specified {@code} keys in this {@link Record} as a JSON
     * Document.
     * 
     * @param keys
     * @return the json dump
     */
    public String dump(String... keys) {
        try {
            Set<String> _keys = Sets.newHashSet(keys);
            Field[] fields = getAllDeclaredFields();
            JsonObject json = new JsonObject();
            json.addProperty("id", id);
            for (Field field : fields) {
                Object value;
                if(_keys.contains(field.getName())
                        && !Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && (value = field.get(this)) != null) {
                    json.add(field.getName(), jsonify(value));
                }
            }
            return json.toString();
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return id == ((Record) obj).id;
    }

    /**
     * Return the {@link #id} that uniquely identifies this record.
     * 
     * @return the id
     */
    public final long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Save all changes that have been made to this record using an ACID
     * transaction.
     * <p>
     * Use {@link Record#saveAll(Record...)} to save changes in multiple records
     * in a single ACID transaction.
     * </p>
     * 
     * @return {@code true} if all the changes have been atomically saved.
     */
    public final boolean save() {
        Preconditions.checkState(!inViolation);
        Concourse concourse = connections().request();
        try {
            concourse.stage();
            save(concourse);
            return concourse.commit();
        }
        catch (Throwable t) {
            concourse.abort();
            return false;
        }
        finally {
            connections().release(concourse);
        }
    }

    @Override
    public final String toString() {
        return dump();
    }

    /**
     * Initialize the new record with all the core data.
     */
    protected final void init() { // visible for access from static #init()
                                  // method
        if(!usable) {
            Concourse concourse = connections().request();
            try {
                this.id = concourse.insert(initData());
                usable = true;
                // TODO set initial state
            }
            finally {
                connections().release(concourse);
            }
        }
    }

    /**
     * Load an existing record from the database and add all of it to this
     * instance in memory.
     * 
     * @param id
     */
    @SuppressWarnings({ "unchecked", "rawtypes", })
    protected final void load(long id) { // visible for access from static #load
                                         // method
        if(!usable) {
            this.id = id;
            Concourse concourse = connections().request();
            checkConstraints(concourse);
            try {
                Field[] fields = getAllDeclaredFields();
                for (Field field : fields) {
                    if(!Modifier.isTransient(field.getModifiers())) {
                        String key = field.getName();
                        if(field.getType().isAssignableFrom(Record.class)) {
                            Constructor<? extends Record> constructor = (Constructor<? extends Record>) field
                                    .getType().getConstructor(long.class);
                            Record record = constructor.newInstance(concourse
                                    .get(key, id));
                            field.set(this, record);
                        }
                        else if(field.getType().isAssignableFrom(
                                Collection.class)) {
                            Collection collection = (Collection) field
                                    .getType().newInstance();
                            Set<?> values = concourse.fetch(key, id);
                            for (Object item : values) {
                                collection.add(item);
                            }
                            field.set(this, collection);
                        }
                        else if(field.getType().isArray()) {
                            List list = new ArrayList();
                            Set<?> values = concourse.fetch(key, id);
                            for (Object item : values) {
                                list.add(item);
                            }
                            field.set(this, list.toArray());
                        }
                        else if(field.getType().isPrimitive()
                                || field.getType() == String.class
                                || field.getType() == Tag.class
                                || field.getType() == Integer.class
                                || field.getType() == Long.class
                                || field.getType() == Float.class
                                || field.getType() == Double.class) {
                            field.set(this, concourse.get(key, id));
                        }
                        else if(field.getType().isAssignableFrom(
                                Serializable.class)) {
                            String base64 = concourse.get(key, id);
                            ByteBuffer bytes = ByteBuffer.wrap(BaseEncoding
                                    .base64Url().decode(base64));
                            field.set(this, Serializables.read(bytes,
                                    (Class<Serializable>) field.getType()));
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
                usable = true;
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
            finally {
                connections().release(concourse);
            }
        }
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
            checkState(section.equals(_),
                    "Cannot load a record from section %s "
                            + "into a Record of type %s", section, _);
        }
        catch (IllegalStateException e) {
            inViolation = true;
            throw e;
        }
    }

    /**
     * Get all the fields that are declared in this class and any of its
     * parents.
     * 
     * @return the declared fields
     */
    private final Field[] getAllDeclaredFields() {
        if(fields0 == null) {
            List<Field> fields = Lists.newArrayList();
            Class<?> clazz = this.getClass();
            while (clazz != Object.class) {
                for (Field field : clazz.getDeclaredFields()) {
                    if(!field.getName().equalsIgnoreCase("fields0")
                            && !field.isSynthetic()
                            && !Modifier.isStatic(field.getModifiers())) {
                        field.setAccessible(true);
                        fields.add(field);
                    }
                }
                clazz = clazz.getSuperclass();
            }
            fields0 = fields.toArray(new Field[] {});
        }
        return fields0;
    }

    /**
     * Return a JSON string that contains all the data which initially populates
     * each record upon creation.
     * 
     * @return the initial data
     */
    private final String initData() {
        JsonObject object = new JsonObject();
        object.addProperty(SECTION_KEY, "`" + _ + "`"); // Wrap the
                                                        // #section with
                                                        // `` so that it
                                                        // is stored in
                                                        // Concourse as
                                                        // a Tag.
        return object.toString();
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
                    .operator(Operator.EQUALS).value(_).and().key(key)
                    .operator(Operator.EQUALS).value(value).build();
            return concourse.find(criteria).isEmpty();
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
    private void save(Concourse concourse) {
        try {
            Field[] fields = getAllDeclaredFields();
            for (Field field : fields) {
                if(!Modifier.isTransient(field.getModifiers())) {
                    field.setAccessible(true);
                    String key = field.getName();
                    Object value = field.get(this);
                    if(field.isAnnotationPresent(Unique.class)) {
                        Preconditions
                                .checkState(isUnique(concourse, key, value));
                    }
                    if(value != null) {
                        store(key, value, concourse, false);
                    }
                }
            }
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
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
    private void store(String key, Object value, Concourse concourse,
            boolean append) {
        // TODO: dirty field detection!
        if(value instanceof Record) {
            concourse.link(key, id, ((Record) value).id);
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
                || value instanceof Float || value instanceof Double) {
            if(append) {
                concourse.add(key, value, id);
            }
            else {
                concourse.set(key, value, id); // TODO use verifyOrSet when
                                               // it becomes available
            }
        }
        else if(value instanceof Serializable) {
            ByteBuffer bytes = Serializables.getBytes((Serializable) value);
            Tag base64 = Tag.create(BaseEncoding.base64Url().encode(
                    ByteBuffers.toByteArray(bytes)));
            store(key, base64, concourse, append);
        }
        else {
            Gson gson = new Gson();
            String json = gson.toJson(value);
            store(key, json, concourse, append);
        }
    }
}
