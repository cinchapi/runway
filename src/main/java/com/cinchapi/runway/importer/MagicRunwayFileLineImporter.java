package com.cinchapi.runway.importer;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.runway.Record;
import com.google.common.collect.Multimap;

// TODO test me please

/**
 * An {@link Importer} that uses reflection to populate a Runway model.
 * 
 * @author jnelson
 * 
 * @param <T> the record type
 */
public abstract class MagicRunwayFileLineImporter<T extends Record> extends
        RunwayFileLineImporter<T> {

    /**
     * Construct a new instance.
     * @param concourse
     */
    protected MagicRunwayFileLineImporter(Concourse concourse) {
        super(concourse);
    }

    @Override
    protected void importData(T record, Multimap<String, Object> data) {
        data.asMap().forEach((key, values) ->{
            values.forEach((value) -> {
                record.set(key, value);
            });
        });
    }

}
