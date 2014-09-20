package org.cinchapi.runway.validation;

/**
 * A rule processor that is used by the {@link ValidatedBy} annotation and ORM
 * engine for determining whether certain values are considered valid for
 * particular fields.
 * 
 * @author jnelson
 */
public interface Validator {

    /**
     * Return {@code true} if {@code object} is considered <em>valid</em>
     * according to the rules of this {@link Validator}.
     * 
     * @param object
     * @return {@code true} if the object is valid
     */
    public boolean validate(Object object);

    /**
     * Return the error message that should be thrown to the caller if an
     * attempt to validate fails.
     * 
     * @return the error message
     */
    public String getErrorMessage();

}
