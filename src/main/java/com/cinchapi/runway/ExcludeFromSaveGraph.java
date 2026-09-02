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
 * Annotation to exclude the {@link Record Records} a field points at from the
 * save graph of the {@link Record} that declares it.
 * <p>
 * A {@link Record#save() save} writes the field's link values, so the declaring
 * {@link Record} still records which {@link Record Records} the field points
 * at, and a load still resolves them. The excluded {@link Record Records}
 * themselves are neither read nor written by that save, and they do not join
 * its conflict footprint.
 * </p>
 * <h2>Scope</h2>
 * <p>
 * The exclusion belongs to the field, not to the {@link Record Records} the
 * field points at. A save that reaches one of those {@link Record Records}
 * through an unannotated field writes it there.
 * </p>
 * <p>
 * The exclusion governs the save graph only. Loading, {@link CascadeDelete},
 * {@link JoinDelete}, {@link CaptureDelete} and reference repair reach an
 * excluded {@link Record} as they do an ordinary one.
 * </p>
 * <h2>Scope binding</h2>
 * <p>
 * An excluded {@link Record} does not join the scope that saves or
 * {@link DatabaseInterface#create(Class, Object...) creates} the declaring
 * {@link Record}, so it keeps the binding it holds and an open
 * {@link Transaction} that saves the declaring {@link Record} takes no
 * ownership of it. Another scope may save or delete it while that
 * {@link Transaction} is open.
 * </p>
 * <p>
 * <strong>NOTE:</strong> A {@link Record} passed as a
 * {@link DatabaseInterface#create(Class, Object...) create} argument that lands
 * in an annotated field is bound to the scope it already had, so a later
 * {@link Record#save() save} of it commits on its own rather than with the
 * creating {@link Transaction}.
 * </p>
 * <p>
 * <strong>NOTE:</strong> A load reaches through an annotated field, so a
 * {@link Record} that a {@link Transaction} loads is bound to it and joins its
 * conflict footprint, however the load reached it. The exclusion bounds what a
 * save takes on its own; it does not bound what the caller reads.
 * </p>
 * <h2>Precondition</h2>
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
