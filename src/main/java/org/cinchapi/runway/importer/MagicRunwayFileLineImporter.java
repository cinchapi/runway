package org.cinchapi.runway.importer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.cinchapi.runway.Record;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

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

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected void importData(T record, Multimap<String, Object> data) {
        try {
            Method method = Record.class
                    .getDeclaredMethod("getAllDeclaredFields");
            method.setAccessible(true);
            Field[] fields = (Field[]) method.invoke(record);
            for (Field field : fields) {
                try {
                    Collection collection = null;
                    if(Collection.class.isAssignableFrom(field.getType())) {
                        if(Modifier.isAbstract(field.getType().getModifiers())
                                || Modifier.isInterface(field.getType()
                                        .getModifiers())) {
                            if(field.getType() == Set.class){
                                collection = Sets.newLinkedHashSet();
                            }
                            else { //assume list
                                collection = Lists.newArrayList();
                            }
                        }
                        else {
                            collection = (Collection) field.getType()
                                    .newInstance();
                        }
                        for (Object obj : Imports.getAll(field.getName(), data)) {
                            collection.add(obj);
                        }
                        field.set(record, collection);
                    }
                    else if(field.getType().isArray()) {
                        List list = new ArrayList();
                        for (Object obj : Imports.getAll(field.getName(), data)) {
                            list.add(obj);
                        }
                        field.set(record, list.toArray());
                    }
                    else if(field.getType().isEnum()) {
                        field.set(record, Enum.valueOf(
                                (Class<Enum>) field.getType(),
                                Imports.<String> get(field.getName(), data)));
                    }
                    else {
                        field.set(record, Imports.get(field.getName(), data));
                    }
                }
                catch (Exception e) {
                    continue;
                }
            }
        }
        catch (ReflectiveOperationException e) {
            Throwables.propagate(e);
        }
    }

}
