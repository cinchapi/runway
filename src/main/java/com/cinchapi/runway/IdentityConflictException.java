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
import com.cinchapi.runway.Record.ConstraintViolationException;

/**
 * Signal that an {@link TransactionInterface#intern(Record) intern} lookup
 * observed a {@link Record Record's} unique identity as unclaimed, but the
 * save's {@link Unique} enforcement observed a claim.
 * <p>
 * A managed operation retries so it can adopt a full same-class winner or
 * report a claim that cannot be adopted as a terminal uniqueness refusal.
 * Within a caller-owned {@link Transaction}, this exception propagates and the
 * failed save poisons the transaction. The uniqueness refusal is the cause.
 * </p>
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
     * @param refusal the uniqueness refusal that the save observed
     */
    IdentityConflictException(ConstraintViolationException refusal) {
        this.message = refusal.getMessage();
        initCause(refusal);
    }

    @Override
    public String getMessage() {
        return message;
    }

}
