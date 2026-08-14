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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.time.Time;

/**
 * Unit tests for {@link IncrementalSaver} that combine the shared {@link Saver}
 * contract with implementation-specific behavior.
 *
 * @author Jeff Nelson
 */
public class IncrementalSaverTest extends SaverTest {

    @Override
    protected Saver instantiateSaver(Concourse connection) {
        return new IncrementalSaver(connection);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link IncrementalSaver} runs the
     * difference {@link java.util.function.Consumer validator} at the moment
     * {@code diff} is called &mdash; not later during commit &mdash; so a
     * throwing validator stops further recording.
     * <p>
     * <strong>Start state:</strong> A record exists with one value.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage the {@link IncrementalSaver}.</li>
     * <li>Record a {@code diff} whose validator throws.</li>
     * <li>Catch the exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exception arrives from the {@code diff}
     * call itself, not from a later {@code commit}.
     */
    @Test
    public void testDiffValidatorRunsInline() {
        long id = client.add("a", 1);

        Saver saver = newSaver();
        saver.stage();
        AtomicBoolean threwOnDiff = new AtomicBoolean(false);
        try {
            saver.diff(id, Timestamp.fromMicros(Time.now()), null, diff -> {
                throw new IllegalStateException("nope");
            });
        }
        catch (IllegalStateException e) {
            threwOnDiff.set(true);
        }
        saver.abort();

        Assert.assertTrue(threwOnDiff.get());
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link IncrementalSaver}
     * {@code find} validator runs inline.
     * <p>
     * <strong>Start state:</strong> A record matching {@code flag = true}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Stage.</li>
     * <li>Record a {@code find} whose validator throws.</li>
     * <li>Catch the exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The exception arrives from the {@code find}
     * call.
     */
    @Test
    public void testFindValidatorRunsInline() {
        client.add("flag", true);

        Saver saver = newSaver();
        saver.stage();
        AtomicBoolean threwOnFind = new AtomicBoolean(false);
        try {
            saver.find(Criteria.where().key("flag").operator(Operator.EQUALS)
                    .value(true), ids -> {
                        throw new IllegalStateException("nope");
                    });
        }
        catch (IllegalStateException e) {
            threwOnFind.set(true);
        }
        saver.abort();

        Assert.assertTrue(threwOnFind.get());
    }

}
