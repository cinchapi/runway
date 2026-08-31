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
 * Declare the {@link ReferenceNotFoundPolicy} that a load applies to the
 * annotated field when a stored reference it holds has no data behind it.
 * <p>
 * A field without this annotation follows the policy that the loading
 * {@link Runway} applies, so annotate a field only where its reference calls
 * for something other than the default. The declaration travels with the field,
 * so every load of the field behaves the same way regardless of how it was
 * reached.
 * </p>
 * <p>
 * {@link ReferenceNotFound} governs what a load reports for a reference that
 * resolves to nothing. It does not decide which {@link Record Records} a
 * deletion reaches, which {@link CascadeDelete}, {@link JoinDelete} and
 * {@link CaptureDelete} govern. A field that captures its deletions never
 * carries a dead reference to begin with, so the policy has nothing to apply
 * to.
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
