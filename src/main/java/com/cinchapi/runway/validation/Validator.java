/*
 * Copyright (c) 2013-2026 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cinchapi.runway.validation;

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
