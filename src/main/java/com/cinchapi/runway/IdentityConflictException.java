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

import com.cinchapi.concourse.TransactionException;

/**
 * A {@link TransactionException} that signals that a concurrent writer claimed
 * a {@link Record Record's} unique identity between an
 * {@link TransactionInterface#intern(Record) intern's} lookup and its save, so
 * the attempt should abort and run again to adopt the winner.
 *
 * @author Jeff Nelson
 */
public final class IdentityConflictException extends TransactionException {

    private static final long serialVersionUID = 1L;

    /**
     * The detail from the underlying uniqueness refusal.
     */
    private final String message;

    /**
     * Construct a new instance.
     *
     * @param message the detail from the uniqueness refusal
     */
    IdentityConflictException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }

}
