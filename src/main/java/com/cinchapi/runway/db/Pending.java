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
 * A {@link Pending} is a value that resolves at some point in the future.
 *
 * <h2>Push, not pull</h2>
 * <p>
 * A {@link java.util.concurrent.Future Future} is <em>pull</em>-based: its
 * holder decides when to read the value, calling {@code get()} and blocking
 * until it is available. A {@link Pending} inverts that model. It is
 * <em>push</em>-based: the holder registers a {@link Consumer} with
 * {@link #onResolve}, and the value is delivered to that {@link Consumer} when
 * it resolves. The caller never asks for the value and never blocks.
 * </p>
 * <p>
 * The push model lets whatever produces the value choose when resolution
 * happens &mdash; for example, deferring work until many outstanding
 * {@link Pending Pendings} can be resolved together &mdash; without callers
 * having to coordinate the timing themselves. Callers describe what should
 * happen with the value and move on.
 * </p>
 *
 * <h2>Composition</h2>
 * <p>
 * Because the work that depends on the value is described rather than executed
 * inline, it can be expressed <em>before</em> the value is known. Two operators
 * build new {@link Pending Pendings} from existing ones:
 * </p>
 * <ul>
 * <li>{@link #map} transforms the value with a function and yields a
 * {@link Pending} of the result. Use it for a synchronous step that produces a
 * plain value.</li>
 * <li>{@link #then} transforms the value with a function that itself returns a
 * {@link Pending}, and yields a {@link Pending} of that follow-on value. Use it
 * for a step that is itself asynchronous.</li>
 * </ul>
 * <p>
 * Operators can be chained: the {@link Pending} returned by one is the input to
 * the next. A {@link Consumer} registered on the final {@link Pending} in a
 * chain receives the fully transformed value.
 * </p>
 *
 * <h2>Examples</h2>
 *
 * <p>
 * Transform a value with {@link #map}:
 * </p>
 *
 * <pre>
 * {@code
 * fetchName(userId)
 *         .map(String::toUpperCase)
 *         .onResolve(name -> System.out.println(name));
 * }
 * </pre>
 *
 * <p>
 * Chain a follow-on step with {@link #then}. The second step cannot be
 * expressed until the first value is known, so {@link #then} captures the
 * dependency:
 * </p>
 *
 * <pre>
 * {@code
 * fetchUserId(email)
 *         .then(id -> fetchProfile(id))
 *         .map(profile -> profile.name())
 *         .onResolve(name -> System.out.println(name));
 * }
 * </pre>
 *
 * <p>
 * A {@link Pending} is <strong>not thread-safe</strong>: composition,
 * {@link #onResolve} registration, and resolution itself must all happen on the
 * same thread.
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
