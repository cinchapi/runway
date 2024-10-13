/*
 * Copyright (c) 2013-2024 Cinchapi Inc.
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
 * Annotation to mark a field for automatic reference removal upon deletion.
 * Fields marked with this annotation will cause the containing record to
 * remove its reference to the deleted record when the linked record is
 * removed.
 * 
 * <p>This is different from {@link JoinDelete}, where the containing record
 * is also deleted if the linked record is deleted. With {@link CaptureDelete},
 * the containing record remains intact, but the reference to the deleted
 * record is nullified automatically.</p>
 *
 * <p>For example, if a parent record has a field annotated with
 * {@link @CaptureDelete} that links to a child record, deleting the child
 * record will not delete the parent record, but it will set the field to
 * {@code null} in the parent.</p>
 *
 * <p>Example Usage:</p>
 * @formatter:off
 * <pre>
 * {@code
 * public class ParentRecord extends Record {
 *     @CaptureDelete
 *     private ChildRecord child;
 * }
 * }
 * </pre>
 * @formatter:off
 * 
 * <p>In this example, deleting the {@code child} will not delete the
 * {@code ParentRecord}, but will set {@code child} to {@code null} in
 * the {@code ParentRecord} instance.</p>
 *
 * @author Jeff Nelson
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface CaptureDelete {}
