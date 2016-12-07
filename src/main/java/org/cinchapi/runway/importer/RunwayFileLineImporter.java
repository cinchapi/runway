package org.cinchapi.runway.importer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.cinchapi.runway.Record;
import org.cinchapi.runway.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.importer.Importer;
import com.google.common.base.Throwables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * An {@link Importer} that imports data through Runway models instead of adding
 * them to Concourse directly.
 * 
 * @author jnelson
 * 
 * @param <T> - the Record type to import the data into
 */
public abstract class RunwayFileLineImporter<T extends Record> extends
        Importer {

    /**
     * Construct a new instance.
     * @param concourse
     */
    protected RunwayFileLineImporter(Concourse concourse) {
        super(concourse);
    }

    /**
     * A Logger that is available for the subclass to log helpful messages.
     */
    protected Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public Set<Long> importFile(String file) {
        return importFile(file, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Long> importFile(String file, String resolveKey) {
        List<ImportResult> results = Lists.newArrayList();
        String[] keys = headers();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] toks = Strings.splitStringByDelimterAndRespectQuotes(
                        line, delimiter());
                if(keys == null) {
                    keys = toks;
                    log.info("Processed header: " + line);
                }
                else {
                    Multimap<String, Object> data = LinkedHashMultimap.create();
                    int limit = keys.length > toks.length ? toks.length
                            : keys.length; // make sure we don't get any
                                           // ArrayIndexOutOfBounds exceptions
                    for (int i = 0; i < limit; i++) {
                        data.put(keys[i], Convert.stringToJava(toks[i]));
                    }
                    Set<T> records = Sets.newHashSet();
                    Class<T> clazz = (Class<T>) ((ParameterizedType) getClass()
                            .getGenericSuperclass()).getActualTypeArguments()[0];

                    // Check to see if there are any resolved records
                    for (Object resolveValue : data.get(resolveKey)) {
                        records = Sets.union(
                                records,
                                Record.find(clazz,
                                        Criteria.where().key(resolveKey)
                                                .operator(Operator.EQUALS)
                                                .value(resolveValue).build()));
                        records = Sets.newHashSet(records); // must make copy
                                                            // because previous
                                                            // method returns
                                                            // immutable view
                    }
                    if(records.isEmpty()) {
                        records.add(Record.create(clazz));
                    }
                    StringBuilder msg = new StringBuilder();
                    for (T record : records) {
                        boolean success = true;
                        int errors = 0;
                        try {
                            importData(record, data);
                            success = record.save();
                        }
                        catch (Throwable t) {
                            String message = t.getMessage();
                            message = message == null ? Throwables
                                    .getStackTraceAsString(t) : message;
                            msg.append(message);
                            success = false;
                        }

                        // Figure out what to log
                        if(success) {
                            errors = 0;
                        }
                        else {
                            if(msg.toString().isEmpty()) {
                                try {
                                    record.throwSupressedExceptions();
                                }
                                catch (Exception e) {
                                    msg.append(e.getMessage());
                                }
                            }
                            errors = 1;
                        }
                        log.info(MessageFormat
                                .format("Imported {0} into record(s) {1} with {2} error(s): {3}",
                                        line, record.getId(), errors,
                                        msg.toString()));
                    }
                }
            }
            reader.close();
            return results;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Import the {@code data} into the {@code record}. The subclass should
     * assign the values associated with the keys in the multimap to the correct
     * fields within the record instance.
     * <p>
     * You <strong>SHOULD NOT</strong> save the record instance in this method.
     * The framework will take care of making sure that the record instance is
     * properly saved in the import result does not have any errors.
     * </p>
     * <p>
     * <em>NOTE: Any exceptions thrown from this method are caught by the framework
     * and logged as errors for the import.</em>
     * </p>
     * 
     * @param record
     * @param data
     */
    protected abstract void importData(T record, Multimap<String, Object> data);

    /**
     * Specify a delimiter to use for splitting the file.
     * 
     * @return the delimiter
     */
    protected abstract String delimiter();

    /**
     * Return the split headers from the file, if necessary. If this method
     * returns {@code null} then the framework will use the first line in the
     * file to determine the headers. Therefore it is only necessary to override
     * this method if there is no header row in the file to be imported.
     * 
     * @return the headers
     */
    @Nullable
    protected String[] headers() {
        return null;
    }

}
