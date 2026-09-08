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
 * Annotation to ensure the {@link Record Records} to which a field points are
 * excluded from the <strong>save graph</strong> of the {@link Record} that
 * declares it.
 * <p>
 * When this annotation is applied, the source {@link Record} will still ensure
 * that the target is stored within the database as a link (so loads will still
 * resolve it). But the content of the excluded {@link Record Records}
 * themselves are neither read nor written by that save, so the save does not
 * check them for staleness or existence.
 * </p>
 * <p>
 * Nevertheless, a save that reaches one of those {@link Record Records} through
 * an unannotated field writes it there in normal course.
 * </p>
 * <p>
 * <strong>NOTE:</strong> The exclusion governs the save graph only. Loading,
 * {@link CascadeDelete}, {@link JoinDelete}, {@link CaptureDelete}, reference
 * repair and the binding that scopes a {@link Record} to a {@link Runway} or
 * {@link Transaction} reach an excluded {@link Record} as they do an ordinary
 * one. A save through a {@link Transaction} therefore binds the excluded
 * {@link Record} to that {@link Transaction}, even though it does not write it.
 * </p>
 * <p>
 * <strong>NOTE:</strong> A save does not create a {@link Record} it reaches
 * only through an annotated field. An unsaved {@link Record} in such a field
 * becomes a link to a {@link Record} that does not exist unless the caller
 * saves it.
 * </p>
 *
 * @author Jeff Nelson
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface ExcludeFromSaveGraph {}
