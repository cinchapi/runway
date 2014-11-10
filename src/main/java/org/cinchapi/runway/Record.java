package org.cinchapi.runway;

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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.cinchapi.concourse.Concourse;
import org.cinchapi.concourse.ConnectionPool;
import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.Tag;
import org.cinchapi.concourse.TransactionException;
import org.cinchapi.concourse.lang.BuildableState;
import org.cinchapi.concourse.lang.Criteria;
import org.cinchapi.concourse.server.io.Serializables;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.time.Time;
import org.cinchapi.concourse.util.ByteBuffers;
import org.cinchapi.runway.util.AnyObject;
import org.cinchapi.runway.validation.Validator;

import static com.google.common.base.Preconditions.checkState;

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
     * Atomically clear all of the records in {@code clazz} from the database.
     * 
     * @param clazz
     */
    public static <T extends Record> void clear(Class<T> clazz) {
        Concourse concourse = connections().request();
        try {
            concourse.stage();
            Set<Long> records = concourse.find(Criteria.where()
                    .key(SECTION_KEY).operator(Operator.EQUALS)
                    .value(clazz.getName()));
            for (long record : records) {
                concourse.clear(record);
            }
            concourse.commit();
        }
        catch (TransactionException e) {
            concourse.abort();
            clear(clazz);
        }
        finally {
            connections().release(concourse);
        }
    }

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
            BuildableState criteria) {
        return find(clazz, criteria.build());
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
    public static Set<Record> findAll(BuildableState criteria) {
        return findAll(criteria.build());
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
    public static <T extends Record> Set<? extends T> findEvery(Class<T> clazz,
            BuildableState criteria) {
        return findEvery(clazz, criteria.build());
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
     * Return the single instance of {@code clazz} that matches the
     * {@code criteria} or {@code null} if it doesn't exist or multiple objects
     * match.
     * 
     * @param clazz
     * @param criteria
     * @return the single matching result
     */
    @Nullable
    public static <T extends Record> T findSingleInstance(Class<T> clazz,
            BuildableState criteria) {
        return findSingleInstance(clazz, criteria.build());
    }

    /**
     * Return the single instance of {@code clazz} that matches the
     * {@code criteria} or {@code null} if it doesn't exist or multiple objects
     * match.
     * 
     * @param clazz
     * @param criteria
     * @return the single matching result
     */
    @Nullable
    public static <T extends Record> T findSingleInstance(Class<T> clazz,
            Criteria criteria) {
        Set<T> results = find(clazz, criteria);
        try {
            return Iterables.getOnlyElement(results);
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Return a list of all the ids for every record in {@code clazz}. This
     * method does to load up the actual records.
     * 
     * @param clazz
     * @return the ids of all the records in the class
     */
    public static <T extends Record> Set<Long> getEveryId(Class<T> clazz) {
        Concourse concourse = connections().request();
        try {
            return concourse.find(Criteria.where().key(SECTION_KEY)
                    .operator(Operator.EQUALS).value(clazz.getName()));
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
        return load(clazz, id, new TLongObjectHashMap<Record>());
    }

    /**
     * Load every record in {@code clazz}.
     * 
     * @param clazz
     * @return all the records in the class
     */
    public static <T extends Record> Set<T> loadEvery(Class<T> clazz) {
        Concourse concourse = connections().request();
        try {
            Set<T> records = Sets.newLinkedHashSet();
            Set<Long> ids = concourse.find(Criteria.where().key(SECTION_KEY)
                    .operator(Operator.EQUALS).value(clazz.getName()));
            for (long id : ids) {
                try {
                    records.add(load(clazz, id));
                }
                catch (ZombieException e) {
                    continue;
                }
            }
            return records;
        }
        finally {
            connections().release(concourse);
        }
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
        long transactionId = Time.now();
        Record current = null;
        try {
            concourse.stage();
            concourse.set("transaction_id", transactionId, METADATA_RECORD);
            Set<Record> waiting = Sets.newHashSet(records);
            waitingToBeSaved.put(transactionId, waiting);
            for (Record record : records) {
                current = record;
                record.save(concourse);
            }
            concourse.clear("transaction_id", METADATA_RECORD);
            return concourse.commit();
        }
        catch (Throwable t) {
            concourse.abort();
            if(current != null) {
                current.errors.add(Throwables.getStackTraceAsString(t));
            }
            return false;
        }
        finally {
            waitingToBeSaved.remove(transactionId);
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
     * Search and return every records in {@code clazz} or any of its children
     * that match the search {@code query} for {@code key}.
     * 
     * @param clazz
     * @param key
     * @param query
     * @return the records that match the search
     */
    @SuppressWarnings("unchecked")
    public static <T extends Record> Set<? extends T> searchEvery(
            Class<T> clazz, String key, String query) {
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
     * Search for the single instance of {@code clazz} that matches the search
     * {@code criteria} or return {@code null} if it doesn't exist or multiple
     * objects match.
     * 
     * @param clazz
     * @param key
     * @param value
     * @return the single matching search result
     */
    @Nullable
    public static <T extends Record> T searchSingleInstance(Class<T> clazz,
            String key, String value) {
        Set<T> results = search(clazz, key, value);
        try {
            return Iterables.getOnlyElement(results);
        }
        catch (Exception e) {
            return null;
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
    public static void setConnectionInformation(String host, int port,
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
     * constructors for nested inner classes.
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
            try {
                return ((String) concourse.get(SECTION_KEY, id)).equals(clazz
                        .getName());
            }
            catch (NullPointerException e) { // NPE indicates the record does
                                             // not have a SECTION_KEY
                return false;
            }
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
     * Return {@code true} if {@code record} is part of a single transaction
     * within {@code concourse} and is waiting to be saved.
     * 
     * @param concourse
     * @param record
     * @return {@code true} if the record is waiting to be saved
     */
    private static boolean isWaitingToBeSaved(Concourse concourse, Record record) {
        try {
            long transactionId = concourse.get("transaction_id",
                    METADATA_RECORD);
            return waitingToBeSaved.get(transactionId).contains(record);
        }
        catch (NullPointerException e) {
            return false;
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
        else if(object instanceof Tag) {
            return new JsonPrimitive((String) object.toString());
        }
        else if(object instanceof Record) {
            return ((Record) object).toJsonElement();
        }
        else {
            Gson gson = new Gson();
            return gson.toJsonTree(object);
        }
    }

    /**
     * Internal method to help recursively load records by keeping tracking of
     * which ones currently exist. Ultimately this method will load the Record
     * that is contained within the specified {@code clazz} and
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
     * @param existing
     * @return
     */
    private static <T extends Record> T load(Class<T> clazz, long id,
            TLongObjectMap<Record> existing) {
        T record = getNewDefaultInstance(clazz);
        record.load(id, existing);
        return record;
    }

    /**
     * The connection pool. Access using the {@link #connections()} method.
     */
    private static ConnectionPool connections;

    /**
     * The record where metadata is stored. We typically store some transient
     * metadata for transaction routing within this record (so its only visible
     * within the specific transaction) and we clear it before commit time.
     */
    private static long METADATA_RECORD = -1;

    /**
     * The key used to hold the section metadata.
     */
    private static final String SECTION_KEY = "_"; // just want a simple/short
                                                   // key name that is likely to
                                                   // avoid collisions

    /**
     * A mapping from a transaction id to the set of records that are waiting to
     * be saved within that transaction. We use this collection to ensure that a
     * record being saved only links to an existing record in the database or a
     * record that will later exist (e.g. waiting to be saved).
     */
    private static final TLongObjectMap<Set<Record>> waitingToBeSaved = new TLongObjectHashMap<Set<Record>>();

    /**
     * The description of a record that is considered to be in "zombie" state.
     */
    private static final Set<String> ZOMBIE_DESCRIPTION = Sets
            .newHashSet(SECTION_KEY);

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
     * A flag that indicates if the record has been deleted using the
     * {@link #deleteOnSave()} method.
     */
    private transient boolean deleted = false;

    /**
     * A log of any suppressed errors related to this Record. The descriptions
     * of these errors can be thrown at any point from the
     * {@link #throwSupressedExceptions()} method.
     */
    private transient List<String> errors = Lists.newArrayList();

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

    /*
     * (non-Javadoc)
     * Create a new Record instance. In order for this to be operable, a call
     * must be made to either {@link #init()} or {@link #load(long)}. A caller
     * should never invoke this constructor directly because it merely creates a
     * hallow shell object that is useless until a call is made to either #init
     * or #load.
     */
    /**
     * DO NOT CALL and DO NOT OVERRIDE!!! Please read the documentation for this
     * class for appropriate instructions on instantiating Record instances.
     */
    protected Record() {/* noop */}

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
            Field[] fields = getAllDeclaredFields();
            data.put("id", id);
            for (Field field : fields) {
                Object value;
                if(!Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && (value = field.get(this)) != null) {
                    data.put(field.getName(), value);
                }
            }
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
            Field[] fields = getAllDeclaredFields();
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
            return data;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
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
        Concourse concourse = connections().request();
        try {
            Preconditions.checkState(!inViolation);
            errors.clear();
            concourse.stage();
            if(deleted) {
                delete(concourse);
            }
            else {
                save(concourse);
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
        finally {
            connections().release(concourse);
        }

    }

    /**
     * Thrown an exception that describes any exceptions that were previously
     * suppressed. If none occured, then this method does nothing. This is a
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
        try {
            Field[] fields = getAllDeclaredFields();
            JsonObject json = new JsonObject();
            json.addProperty("id", id);
            Map<String, Object> more = getMoreData();
            for (String key : more.keySet()) {
                json.add(key, jsonify(more.get(key)));
            }
            for (Field field : fields) {
                Object value = field.get(this);
                if(!Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && value != null) {
                    json.add(field.getName(), jsonify(value));
                }
            }
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
     * @return the JsonElement representation
     */
    public JsonElement toJsonElement(String... keys) {
        try {
            Set<String> _keys = Sets.newHashSet(keys);
            Field[] fields = getAllDeclaredFields();
            JsonObject json = new JsonObject();
            json.addProperty("id", id);
            Map<String, Object> more = getMoreData();
            for (String key : more.keySet()) {
                if(_keys.contains(key)) {
                    json.add(key, jsonify(more.get(key)));
                }
            }
            for (Field field : fields) {
                Object value;
                if(_keys.contains(field.getName())
                        && !Modifier.isPrivate(field.getModifiers())
                        && !Modifier.isTransient(field.getModifiers())
                        && (value = field.get(this)) != null) {
                    json.add(field.getName(), jsonify(value));
                }
            }
            return json;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public final String toString() {
        return dump();
    }

    /**
     * Initialize the new record with all the core data.
     */
    final void init() { // visible for access from static #init()
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
     * @param existing
     */
    @SuppressWarnings({ "unchecked", "rawtypes", })
    final void load(long id, TLongObjectMap<Record> existing) { // visible for
                                                                // access
                                                                // from static
                                                                // #load
                                                                // method
        if(!usable) {
            this.id = id;
            existing.put(id, this); // add the current object so we don't
                                    // recurse infinitely
            Concourse concourse = connections().request();
            checkConstraints(concourse);
            try {
                if(inZombieState(id, concourse)) {
                    concourse.clear(id);
                    throw new ZombieException();
                }
                Field[] fields = getAllDeclaredFields();
                for (Field field : fields) {
                    if(!Modifier.isTransient(field.getModifiers())) {
                        String key = field.getName();
                        if(Record.class.isAssignableFrom(field.getType())) {
                            Record record = (Record) getNewDefaultInstance(field
                                    .getType());
                            record.load(
                                    ((Link) concourse.get(key, id)).longValue(),
                                    existing);
                            field.set(this, record);
                        }
                        else if(Collection.class.isAssignableFrom(field
                                .getType())) {
                            Collection collection = null;
                            if(Modifier.isAbstract(field.getType()
                                    .getModifiers())
                                    || Modifier.isInterface(field.getType()
                                            .getModifiers())) {
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
                            Set<?> values = concourse.fetch(key, id);
                            for (Object item : values) {
                                if(item instanceof Link) {
                                    long link = ((Link) item).longValue();
                                    Object obj = existing.get(link);
                                    if(obj == null) {
                                        String section = concourse.get("_",
                                                link);
                                        if(Strings.isNullOrEmpty(section)) {
                                            concourse.remove(key, item, id);
                                            continue;
                                        }
                                        else {
                                            Class<? extends Record> linkClass = (Class<? extends Record>) Class
                                                    .forName(section.toString());
                                            item = load(linkClass, link,
                                                    existing);
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
                            Set<?> values = concourse.fetch(key, id);
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
                                || field.getType() == Double.class) {
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
                                field.set(this, Enum.valueOf(
                                        (Class<Enum>) field.getType(),
                                        stored.toString()));
                            }
                        }
                        else if(Serializable.class.isAssignableFrom(field
                                .getType())) {
                            String base64 = concourse.get(key, id);
                            if(base64 != null) {
                                ByteBuffer bytes = ByteBuffer.wrap(BaseEncoding
                                        .base64Url().decode(base64));
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
                    section.equals(_)
                            || Class.forName(_).isAssignableFrom(
                                    Class.forName(section)),
                    "Cannot load a record from section %s "
                            + "into a Record of type %s", section, _);
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
                    .operator(Operator.EQUALS).value(_).and().key(key)
                    .operator(Operator.EQUALS).value(value).build();
            Set<Long> records = concourse.find(criteria);
            return records.isEmpty()
                    || (records.contains(id) && records.size() == 1);
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
    private void save(final Concourse concourse) {
        try {
            Field[] fields = getAllDeclaredFields();
            for (Field field : fields) {
                if(!Modifier.isTransient(field.getModifiers())) {
                    field.setAccessible(true);
                    final String key = field.getName();
                    final Object value = field.get(this);
                    if(field.isAnnotationPresent(ValidatedBy.class)) {
                        Class<? extends Validator> validatorClass = field
                                .getAnnotation(ValidatedBy.class).value();
                        Validator validator = getNewDefaultInstance(validatorClass);
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
                                !AnyObject.isNullOrEmpty(value),
                                field.getName() + " is required");
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
    @SuppressWarnings("rawtypes")
    private void store(String key, Object value, Concourse concourse,
            boolean append) {
        // TODO: dirty field detection!
        if(value instanceof Record) {
            Record record = (Record) value;
            Preconditions.checkState(!record.inZombieState(concourse)
                    || isWaitingToBeSaved(concourse, record),
                    "Cannot link to an empty record! "
                            + "You must save the record to "
                            + "which you're linking before "
                            + "saving this record, or save "
                            + "them both within an atomic transaction "
                            + "using the Record#saveAll() method");
            concourse.link(key, id, record.id);
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
                concourse.set(key, value, id); // TODO use verifyOrSet when
                                               // it becomes available
            }
        }
        else if(value instanceof Enum) {
            concourse.set(key, Tag.create(((Enum) value).name()), id);
        }
        else if(value instanceof Serializable) {
            ByteBuffer bytes = Serializables.getBytes((Serializable) value);
            Tag base64 = Tag.create(BaseEncoding.base64Url().encode(
                    ByteBuffers.toByteArray(bytes)));
            store(key, base64, concourse, append);
        }
        else {
            Gson gson = new Gson();
            Tag json = Tag.create(gson.toJson(value));
            store(key, json, concourse, append);
        }
    }
}
