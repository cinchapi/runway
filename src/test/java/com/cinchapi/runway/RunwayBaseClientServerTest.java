/*
 * Copyright (c) 2013-2021 Cinchapi Inc.
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
abstract class RunwayBaseClientServerTest extends ClientServerTest {

    @Override
    protected String getServerVersion() {
        return Testing.CONCOURSE_VERSION;
    }

    protected Runway runway;

    @Override
    public void beforeEachTest() {
        runway = Runway.builder().port(server.getClientPort()).build();
    }

    @Override
    public void afterEachTest() {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
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

}
