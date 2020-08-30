package com.cinchapi.runway;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A marker to indicate that the value for a field in a {@link Record} should be
 * unique. The ORM framework will ensure that any field with this annotation is
 * unique before saving the data to the database.
 * <p>
 * By default a {@link Unique} constraint is applied to a single element. You
 * can simultaneously apply the same {@link Unique} constraint to multiple
 * fields to simulate a compound index by providing the same {@link #name()} to
 * the {@link Unique} annotation on all the desired fields.
 * </p>
 * 
 * @author jnelson
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Unique {

    /**
     * The name of {@link Unique} constraint. Use the same name for
     * {@link Unique} constraints on multiple fields to enforce combined
     * uniqueness.
     * 
     * @return the name of the {@link Unique} constraint
     */
    String name() default "";

    // TODO: In future, add a field String[] names to allow a field to be a part
    // of multiple unique constraints

}
