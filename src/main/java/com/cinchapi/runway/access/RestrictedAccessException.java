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
package com.cinchapi.runway.access;

/**
 * Thrown when an {@link Audience} attempts to perform an operation on an
 * {@link AccessControl access controlled} {@link Record} that violates the
 * defined access rules.
 *
 * @author Jeff Nelson
 */
public class RestrictedAccessException extends RuntimeException {

    private static final long serialVersionUID = -8174960449599230104L;

}