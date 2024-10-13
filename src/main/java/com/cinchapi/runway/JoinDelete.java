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
 * Annotation to mark a field for reverse cascading deletion. Fields marked
 * with this annotation will trigger the deletion of the containing record
 * when the linked record is deleted. This annotation is commonly used to
 * ensure referential integrity by deleting records that depend on other
 * records.
 *
 * <p>
 * For example, if a parent record has a field annotated with
 * {@link JoinDelete} that links to a child record, deleting the child
 * record will also delete the parent record.
 * </p>
 *
 * <p>
 * This annotation works in the reverse manner of {@link CascadeDelete},
 * which deletes linked records when the containing record is removed.
 * </p>
 *
 * <p>
 * Example Usage:
 * </p>
 * 
 * @formatter:off
 * <pre>
 * {@code
 * public class ParentRecord extends Record { 
 * 
 *     @ReceiveDelete
 *     private ChildRecord child;
 * }
 * }
 * </pre>
 * @formatter:on
 *
 * <p>
 * In this example, deleting the {@code child} will also delete the
 * {@code ParentRecord} instance that contains it.
 * </p>
 *
 * @author Jeff Nelson
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface JoinDelete {}
