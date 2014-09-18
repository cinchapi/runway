package org.cinchapi.runway;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A marker to indicate that the value for a field in a {@link Record} should
 * never be null. The ORM framework will ensure that any field with this
 * annotation is not null before saving the data to the database.
 * 
 * @author jnelson
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Required {

}
