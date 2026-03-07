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
