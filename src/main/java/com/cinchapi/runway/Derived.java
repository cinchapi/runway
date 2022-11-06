/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A marker to indicate that a method provides a <strong>derived</strong>
 * property.
 * <p>
 * A derived property is additional information that is not directly stored in
 * the database, but can be inferred based on {@link Record#intrinsic()
 * intrinsic} properties.
 * </p>
 * 
 * @author Jeff Nelson
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Derived {

    /**
     * The name of derived property. By default, the name of the annotated
     * method is used.
     * 
     * @return the name of the derived property
     */
    String value() default "";
}