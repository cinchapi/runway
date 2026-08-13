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

import java.util.function.UnaryOperator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.cinchapi.common.base.Verify;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.sort.Order;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Selection;
import com.cinchapi.runway.Selections;
import com.cinchapi.runway.Transaction;
import com.cinchapi.runway.TransactionInterface;

/**
 * A {@link Transaction} that behaves like its {@link Audience}.
 * <p>
 * Every database operation delegates to the {@link Audience}, so reads observe
 * the {@link Audience Audience's} visibility and writes require its
 * permissions, and each operation resolves within the {@link Transaction}
 * because the {@link Audience} is bound to it. The lifecycle methods drive the
 * {@link Transaction} directly.
 * </p>
 * <p>
 * The delegation is only valid while the {@link Audience} operates in the
 * {@link Transaction Transaction's} scope: while the transaction is open, and
 * after it ends until the {@link Audience} joins a different
 * {@link Transaction}. From then on, a database operation on this view is
 * refused; the fall-through to the enclosing {@link com.cinchapi.runway.Runway
 * Runway} must not follow the {@link Audience} into a scope this view never
 * represented.
 * </p>
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
final class AudienceTransaction implements Transaction {

    /**
     * Return the raw {@link TransactionInterface} behind {@code transaction} so
     * a framework-internal operation that already performed its access checks
     * can proceed without repeating them.
     *
     * @param transaction the possibly {@link Audience}-scoped view
     * @return the raw {@link TransactionInterface}
     */
    static TransactionInterface raw(TransactionInterface transaction) {
        return transaction instanceof AudienceTransaction
                ? ((AudienceTransaction) transaction).transaction
                : transaction;
    }

    /**
     * The {@link Audience} whose visibility and permissions govern every
     * database operation on this view.
     */
    private final Audience audience;

    /**
     * The {@link Transaction} that scopes every operation and receives the
     * lifecycle calls.
     */
    private final Transaction transaction;

    /**
     * Construct a new instance.
     *
     * @param audience the {@link Audience} that governs database operations
     * @param transaction the {@link Transaction} that scopes them
     */
    AudienceTransaction(Audience audience, Transaction transaction) {
        this.audience = audience;
        this.transaction = transaction;
    }

    @Override
    public void abort() {
        transaction.abort();
    }

    @Override
    public void afterAbort(Runnable hook) {
        transaction.afterAbort(hook);
    }

    @Override
    public void afterCommit(Runnable hook) {
        transaction.afterCommit(hook);
    }

    @Override
    public void close() {
        transaction.close();
    }

    @Override
    public boolean commit() {
        return transaction.commit();
    }

    @Override
    public <T extends Record> T create(Class<T> clazz, Object... args) {
        verifyAudienceScope();
        return audience.create(clazz, args);
    }

    @Nullable
    @Override
    public <T extends Record, V> T findAnyFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        verifyAudienceScope();
        return audience.findAnyFirstAndUpdate(clazz, criteria, order, key,
                update);
    }

    @Nullable
    @Override
    public <T extends Record, V> T findAnyUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        verifyAudienceScope();
        return audience.findAnyUniqueAndUpdate(clazz, criteria, key, update);
    }

    @Nullable
    @Override
    public <T extends Record, V> T findFirstAndUpdate(Class<T> clazz,
            Criteria criteria, Order order, String key,
            UnaryOperator<V> update) {
        verifyAudienceScope();
        return audience.findFirstAndUpdate(clazz, criteria, order, key, update);
    }

    @Nullable
    @Override
    public <T extends Record, V> T findUniqueAndUpdate(Class<T> clazz,
            Criteria criteria, String key, UnaryOperator<V> update) {
        verifyAudienceScope();
        return audience.findUniqueAndUpdate(clazz, criteria, key, update);
    }

    @Override
    public <T extends Record> T intern(T record) {
        verifyAudienceScope();
        return audience.intern(record);
    }

    @Override
    public boolean save(boolean preventStaleWrites, Record... records) {
        return transaction.save(preventStaleWrites, records);
    }

    @Override
    public Selections select(Selection<?>... selections) {
        verifyAudienceScope();
        return audience.select(selections);
    }

    /**
     * Verify that the {@link #audience} still operates in the
     * {@link #transaction transaction's} scope, so a delegated operation cannot
     * resolve in a different scope that the {@link Audience} later joined.
     */
    private void verifyAudienceScope() {
        Verify.that(Reflection.get("binding", audience) == transaction,
                "The Audience behind this view no longer operates in this"
                        + " Transaction's scope");
    }

}
