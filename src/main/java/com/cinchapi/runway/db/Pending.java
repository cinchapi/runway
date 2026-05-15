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
package com.cinchapi.runway.db;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

/**
 * A {@link Pending} is a value that is eventually produced by a {@link Reader}.
 * <p>
 * Composition operators ({@link #map}, {@link #then}) build new {@link Pending
 * Pendings} from existing ones, and {@link #onResolve} delivers the value to a
 * {@link Consumer}.
 * </p>
 * <p>
 * A {@link Pending} value is <strong>not thread-safe</strong>: it is associated
 * with a single {@link Reader} and must be touched only from that {@link Reader
 * Reader's} owning thread.
 * </p>
 *
 * @param <T> the value type
 *
 * @author Jeff Nelson
 */
@NotThreadSafe
public abstract class Pending<T> {

    /**
     * Return a {@link Pending} whose value is {@code value}.
     *
     * @param value the value (may be {@code null})
     * @param <T> the value type
     * @return a {@link Pending} of {@code value}
     */
    public static <T> Pending<T> of(T value) {
        return new Resolved<>(value);
    }

    /**
     * Return a {@link Pending} whose value is the result of
     * {@code resolver.get()} when invoked by {@code reader}.
     *
     * @param reader the {@link AbstractReader} that owns the underlying read
     * @param resolver yields the value of this {@link Pending}
     * @param <T> the value type
     * @return a {@link Pending} of the supplied value
     */
    static <T> Pending<T> deferred(AbstractReader reader,
            Supplier<T> resolver) {
        return new Deferred<>(reader, resolver);
    }

    /**
     * Return a {@link Pending} whose value is {@code mapper} applied to this
     * {@link Pending Pending's} value.
     *
     * @param mapper the transform to apply
     * @param <U> the transformed value type
     * @return a {@link Pending} of the transformed value
     */
    public abstract <U> Pending<U> map(Function<? super T, ? extends U> mapper);

    /**
     * Return a {@link Pending} whose value is the value of the {@link Pending}
     * that {@code next} returns when given this {@link Pending Pending's}
     * value.
     *
     * @param next given this {@link Pending Pending's} value, returns a
     *            follow-on {@link Pending}
     * @param <U> the follow-on {@link Pending Pending's} value type
     * @return a {@link Pending} of the follow-on value
     */
    public abstract <U> Pending<U> then(
            Function<? super T, ? extends Pending<U>> next);

    /**
     * Deliver this {@link Pending Pending's} value to {@code sink}.
     *
     * @param sink the {@link Consumer} that receives the value
     */
    public abstract void onResolve(Consumer<? super T> sink);

    /**
     * A {@link Pending} backed by a value supplied at construction.
     *
     * @param <T> the value type
     *
     * @author Jeff Nelson
     */
    private static final class Resolved<T> extends Pending<T> {

        /**
         * The value of this {@link Pending}.
         */
        private final T value;

        /**
         * Construct a new {@link Resolved}.
         *
         * @param value the value of this {@link Pending}
         */
        Resolved(T value) {
            this.value = value;
        }

        @Override
        public <U> Pending<U> map(Function<? super T, ? extends U> mapper) {
            return new Resolved<>(mapper.apply(value));
        }

        @Override
        public <U> Pending<U> then(
                Function<? super T, ? extends Pending<U>> next) {
            return next.apply(value);
        }

        @Override
        public void onResolve(Consumer<? super T> sink) {
            sink.accept(value);
        }

    }

    /**
     * A {@link Pending} whose value is supplied later: either by an
     * {@link AbstractReader} that owns its underlying read, or by a chained
     * {@link #then} continuation that supplies the value from a follow-on
     * {@link Pending}.
     *
     * @param <T> the value type
     *
     * @author Jeff Nelson
     */
    private static final class Deferred<T> extends Pending<T> {

        /**
         * Sinks awaiting the value of this {@link Pending}.
         */
        private final List<Consumer<? super T>> sinks = new ArrayList<>();

        /**
         * Whether {@link #value} has been set.
         */
        private boolean resolved = false;

        /**
         * The value of this {@link Pending}; meaningful only when
         * {@link #resolved} is {@code true}.
         */
        private T value;

        /**
         * Construct a {@link Deferred} whose value is the result of
         * {@code resolver.get()} when invoked by {@code reader}.
         *
         * @param reader the {@link AbstractReader} that owns the underlying
         *            read
         * @param resolver yields the value of this {@link Pending}
         */
        Deferred(AbstractReader reader, Supplier<T> resolver) {
            reader.register(() -> complete(resolver.get()));
        }

        /**
         * Construct a {@link Deferred} whose value is supplied via
         * {@link #complete(Object)}.
         */
        Deferred() {/* completed via complete() */}

        @Override
        public <U> Pending<U> map(Function<? super T, ? extends U> mapper) {
            Deferred<U> chained = new Deferred<>();
            onResolve(value -> chained.complete(mapper.apply(value)));
            return chained;
        }

        @Override
        public <U> Pending<U> then(
                Function<? super T, ? extends Pending<U>> next) {
            Deferred<U> chained = new Deferred<>();
            onResolve(value -> next.apply(value).onResolve(chained::complete));
            return chained;
        }

        @Override
        public void onResolve(Consumer<? super T> sink) {
            if(resolved) {
                sink.accept(value);
            }
            else {
                sinks.add(Preconditions.checkNotNull(sink));
            }
        }

        /**
         * Set this {@link Pending Pending's} value to {@code value} and deliver
         * it to every registered sink.
         *
         * @param value the value of this {@link Pending}
         */
        private void complete(T value) {
            Preconditions.checkState(!resolved, "Pending has already resolved");
            this.resolved = true;
            this.value = value;
            // Snapshot-and-clear so a sink that registers another sink onto
            // this Pending (now already resolved) does not concurrently mutate
            // the iteration.
            List<Consumer<? super T>> snapshot = new ArrayList<>(sinks);
            sinks.clear();
            for (Consumer<? super T> sink : snapshot) {
                sink.accept(value);
            }
        }

    }

}
