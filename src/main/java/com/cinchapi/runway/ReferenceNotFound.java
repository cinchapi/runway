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
 * Declare the {@link ReferenceNotFoundPolicy} for the annotated field, which
 * overrides the policy that the {@link Runway} applies.
 * <p>
 * Annotate a field only where a stale reference on it needs different handling
 * than the database default. The declaration travels with the field, so every
 * load of the field behaves the same way, no matter which record the load
 * started from.
 * </p>
 * <p>
 * This annotation decides only what a load does with a stale reference. It does
 * not decide which {@link Record Records} a deletion reaches, which
 * {@link CascadeDelete}, {@link JoinDelete} and {@link CaptureDelete} govern. A
 * field annotated with {@link CaptureDelete} never carries a stale reference,
 * so no policy applies to it.
 * </p>
 *
 * @author Jeff Nelson
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ReferenceNotFound {

    /**
     * The {@link ReferenceNotFoundPolicy} that governs the annotated field.
     *
     * @return the {@link ReferenceNotFoundPolicy}
     */
    ReferenceNotFoundPolicy value();

}
