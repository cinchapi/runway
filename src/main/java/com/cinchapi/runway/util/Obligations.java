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
package com.cinchapi.runway.util;

import java.util.ArrayList;
import java.util.List;

import com.cinchapi.common.base.CheckedExceptions;

/**
 * {@link Obligations} provides utilities for executing a group of actions with
 * the guarantee that every action runs, even if earlier actions throw
 * exceptions.
 * <p>
 * This is useful for cleanup and teardown logic where multiple independent
 * steps must all execute regardless of individual failures. Any exceptions
 * thrown during execution are collected and rethrown as a single exception with
 * the remaining failures attached as {@link Throwable#addSuppressed(Throwable)
 * suppressed} exceptions.
 * </p>
 *
 * @author Jeff Nelson
 */
public final class Obligations {

    /**
     * An action that may throw a checked {@link Exception}.
     */
    @FunctionalInterface
    public interface Action {

        /**
         * Execute the action.
         *
         * @throws Exception if the action fails
         */
        void run() throws Exception;
    }

    /**
     * Execute every {@code action}, guaranteeing that all of them run even if
     * some throw exceptions.
     * <p>
     * Each action is executed in order. If any action throws, the exception is
     * captured and execution continues with the next action. After all actions
     * have been attempted, if any exceptions were captured, the first is thrown
     * with all subsequent exceptions attached as
     * {@link Throwable#addSuppressed(Throwable) suppressed} exceptions.
     * </p>
     *
     * @param actions the actions to execute
     * @throws Exception if any action throws
     */
    public static void runAll(Action... actions) throws Exception {
        List<Throwable> errors = null;
        for (Action action : actions) {
            try {
                action.run();
            }
            catch (Throwable t) {
                if(errors == null) {
                    errors = new ArrayList<>();
                }
                errors.add(t);
            }
        }
        if(errors != null) {
            Throwable primary = errors.get(0);
            for (int i = 1; i < errors.size(); ++i) {
                primary.addSuppressed(errors.get(i));
            }
            throw primary instanceof Exception ? (Exception) primary
                    : CheckedExceptions.throwAsRuntimeException(primary);
        }
    }

    private Obligations() {/* no-init */}

}
