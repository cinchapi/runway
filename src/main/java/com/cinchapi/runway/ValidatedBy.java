package com.cinchapi.runway;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.cinchapi.runway.validation.Validator;

/**
 * An annotation that specifies the {@link Validator} class to use when
 * validating any values associated with the field covered by this annotation.
 * 
 * @author jnelson
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidatedBy {

    /**
     * Return the {@link Validator} class that is used to process the field
     * covered by this annotation.
     * 
     * @return the Validator class
     */
    Class<? extends Validator> value();
}
