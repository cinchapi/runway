package org.cinchapi.concourse.oop;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.ConnectionPool;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.server.io.Serializables;
import org.cinchapi.concourse.util.ByteBuffers;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

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
    protected static <T extends Record> T create(Class<T> clazz) {
        try {
            T record = clazz.newInstance();
            cache.put(record.getId(), record);
            return record;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
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
     * Find and return all the records that are in {@code clazz} or any of its
     * subclasses that match {@code criteria}.
     * 
     * @param clazz
     * @param criteria
     * @return the records that match {@code criteria}
     */
    @SuppressWarnings("unchecked")
    public static <T extends Record> Set<? extends T> findAll(Class<T> clazz,
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
     * Find and return any record that matches {@code criteria}, regardless of
     * what class the Record is contained within.
     * 
     * @param criteria
     * @return the records that match {@code criteria}
     */
    @SuppressWarnings("unchecked")
    public static Set<Record> findAny(Criteria criteria) {
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
     * Load the Record that is contained within the specified {@code clazz} and
     * has the specified {@code id}.
     * 
     * @param clazz
     * @param id
     * @return the existing Record
     */
    @SuppressWarnings("unchecked")
    public static <T extends Record> T load(Class<T> clazz, long id) {
        T record = (T) cache.get(id);
        if(record == null) {
            try {
                Constructor<T> constructor = clazz.getConstructor(long.class);
                record = constructor.newInstance(id);
                cache.put(id, record);
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }
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
            return ((String) concourse.get(SECTION_KEY, id)).replace("`", "")
                    .equals(clazz.getName()); // TODO change when 0.4.1 comes
                                              // out
        }
    }

    /**
     * The cache that holds all the record instances that have been loaded from
     * the database. This is used to ensure that we don't make unnecessary read
     * calls and also that all interaction with a particular Record goes through
     * the same instance so we ensure that everything remains consistent.
     */
    private static final TLongObjectMap<Record> cache = new TLongObjectHashMap<Record>();

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
     * Create a new Record
     */
    protected Record() {
        Concourse concourse = connections().request();
        try {
            this.id = concourse.insert(initData());
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Load an existing record from
     * 
     * @param primaryKey
     */
    @SuppressWarnings({ "unchecked", "rawtypes", "unused" })
    private Record(long id) {
        this.id = id;
        Concourse concourse = connections().request();
        checkConstraints(concourse);
        try {
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                String key = field.getName();
                if(field.getType().isAssignableFrom(Record.class)) {
                    Constructor<? extends Record> constructor = (Constructor<? extends Record>) field
                            .getType().getConstructor(long.class);
                    Record record = constructor.newInstance(concourse.get(key,
                            id));
                    field.set(this, record);
                }
                else if(field.getType().isAssignableFrom(Collection.class)) {
                    Collection collection = (Collection) field.getType()
                            .newInstance();
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
                else if(field.getType().isAssignableFrom(Serializable.class)) {
                    String base64 = concourse.get(key, id);
                    ByteBuffer bytes = ByteBuffer.wrap(BaseEncoding.base64Url()
                            .decode(base64));
                    field.set(this, Serializables.read(bytes,
                            (Class<Serializable>) field.getType()));
                }
                else {
                    Gson gson = new Gson();
                    Object object = gson.fromJson(
                            (String) concourse.get(key, id), field.getType());
                    field.set(this, object);
                }
            }
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        finally {
            connections().release(concourse);
        }
    }

    /**
     * The primary key that is used to identify this Record in the database.
     */
    private final long id;

    /**
     * A flag that indicates this Record is in violation of some constraint and
     * therefore cannot be used without ruining the integrity of the database.
     */
    private boolean inViolation = false;

    /**
     * Return the {@link #id} that uniquely identifies this record.
     * 
     * @return the id
     */
    public final long getId() {
        return id;
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
        finally {
            connections().release(concourse);
        }
    }

    /**
     * Return the name of the section where this Record is stored in the
     * database.
     * 
     * @return the section name
     */
    private final String _() {
        return this.getClass().getName();
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
            checkState(concourse.get(SECTION_KEY, id).equals(_()));
        }
        catch (IllegalStateException e) {
            inViolation = true;
            throw e;
        }
    }

    /**
     * Return a JSON string that contains all the data which initially populates
     * each record upon creation.
     * 
     * @return the initial data
     */
    private final String initData() {
        JsonObject object = new JsonObject();
        object.addProperty(SECTION_KEY, "`" + _() + "`"); // Wrap the
                                                          // #section with
                                                          // `` so that it
                                                          // is stored in
                                                          // Concourse as
                                                          // a Tag.
        return object.toString();
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
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                if(!Modifier.isTransient(field.getModifiers())) {
                    field.setAccessible(true);
                    String key = field.getName();
                    Object value = field.get(this);
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
        //TODO: dirty field detection!
        if(value instanceof Record) {
            concourse.link(key, id, ((Record) value).id);
        }
        else if(value instanceof Collection || value.getClass().isArray()) {
            //TODO use reconcile() function once 0.5.0 comes out...
            concourse.clear(key, id); // TODO this is extreme...move to a diff
                                      // based approach to delete on values that
                                      // should be deleted
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
