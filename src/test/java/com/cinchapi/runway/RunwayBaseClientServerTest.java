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

import java.util.Map;
import java.util.function.Supplier;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.concourse.test.ClientServerTest;
import com.google.common.collect.ImmutableMap;

/**
 * Base test class for {@link Runway} tests that use the
 * {@ClientServerTest} framework.
 *
 * @author Jeff Nelson
 */
public abstract class RunwayBaseClientServerTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

    @Override
    protected boolean reuseServerAcrossTests() {
        return true;
    }

    @Override
    protected SharedServerFailurePolicy onSharedServerFailure() {
        return SharedServerFailurePolicy.REFRESH;
    }

    protected Runway runway;

    @Override
    protected void beforeTestRun() {
        runway = runwayBuilder().build();
    }

    @Override
    protected void afterTestRun() {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return a {@link Runway.Builder} bound to the test {@link #server} and the
     * current test's {@link #environment}.
     * <p>
     * Tests must build every {@link Runway} from this method so that each
     * connects to the per-test {@link #environment}.
     *
     * @return a {@link Runway.Builder} for this test's {@link #server} and
     *         {@link #environment}
     */
    protected Runway.Builder runwayBuilder() {
        return Runway.builder().port(server.getClientPort())
                .environment(environment);
    }

    class Player extends Record {
        String name;
        int score;

        public Player(String name, int score) {
            this.name = name;
            this.score = score;
        }

        @Override
        protected Map<String, Object> derived() {
            return ImmutableMap.of("isAllstar", score > 20);
        }

        @Override
        protected Map<String, Supplier<Object>> computed() {
            return ImmutableMap.of("isAboveAverage", () -> {
                double average = db.load(Player.class).stream()
                        .mapToInt(player -> player.score).summaryStatistics()
                        .getAverage();
                return score > average;
            }, "isBelowAverage", () -> {
                double average = db.load(Player.class).stream()
                        .mapToInt(player -> player.score).summaryStatistics()
                        .getAverage();
                return score < average;
            });
        }

    }

    class PointGuard extends Player {

        int assists;

        /**
         * Construct a new instance.
         * 
         * @param name
         * @param score
         */
        public PointGuard(String name, int score, int assists) {
            super(name, score);
            this.assists = assists;
        }

    }

}
