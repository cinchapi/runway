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
 * Declare the {@link Strategy} that a save uses to merge a field's in-memory
 * state into the state the database stores.
 * <p>
 * A field without this annotation uses {@link Strategy#MERGE MERGE}, which
 * stages only the changes the instance made, so values that other writers
 * changed concurrently survive. Annotate a field with {@link Strategy#OVERWRITE
 * OVERWRITE} to instead write the field's full current state on every save that
 * stages changes.
 * </p>
 * <p>
 * {@link MergeStrategy} governs how a field stages, not when a {@link Record}
 * is dirty: a record whose fields all match their last loaded or saved state
 * still has no {@link Record#hasUnsavedChanges() unsaved changes} and a save of
 * it writes nothing.
 * </p>
 *
 * @author Jeff Nelson
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MergeStrategy {

    /**
     * The {@link Strategy} that governs how the annotated field saves.
     *
     * @return the {@link Strategy}
     */
    Strategy value();

    /**
     * The ways a save can merge a field's in-memory state into the state the
     * database stores.
     *
     * @author Jeff Nelson
     */
    enum Strategy {

        /**
         * Stage only the difference between the field's current state and the
         * state the {@link Record} last loaded or saved: an add for every
         * element the instance introduced, a remove for every element it
         * dropped, and a write of a scalar only when its value changed. Values
         * that other writers changed concurrently survive the save.
         * <p>
         * This is the default for every field without a {@link MergeStrategy}
         * annotation.
         * </p>
         */
        MERGE,

        /**
         * Write the field's full current state whenever the {@link Record}
         * saves staged changes: after the save, the stored state for the field
         * exactly matches the instance, and any concurrent change to the field
         * is overwritten.
         * <p>
         * This is the correct semantics for a field whose value set is one
         * logical whole that is always produced in its entirety (e.g., an
         * ordered sequence that is rebuilt in place), where merging two
         * versions would interleave them into a state that no writer intended.
         * </p>
         * <p>
         * <strong>WARNING:</strong> {@link #OVERWRITE} restores
         * last-writer-wins over the whole field. Never use it on a collection
         * that multiple writers extend independently (e.g., a membership set),
         * because a save from an instance with a stale view erases the elements
         * that other writers added after the instance loaded.
         * </p>
         */
        OVERWRITE
    }

}
