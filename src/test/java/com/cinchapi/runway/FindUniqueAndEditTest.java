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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.DuplicateEntryException;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests for
 * {@link Runway#findUniqueAndEdit(Class, Criteria, java.util.function.Consumer)
 * findUniqueAndEdit}. Each test runs under both Command-API modes (bulk enabled
 * and disabled); the atomic edit itself always uses the incremental,
 * synchronously-staged transaction path, so the matrix additionally guards the
 * surrounding save and load operations under both modes.
 *
 * @author Javier Lores
 */
@RunWith(Parameterized.class)
public class FindUniqueAndEditTest extends RunwayBaseClientServerTest {

    /**
     * Return the parameter matrix that drives each test once per Command-API
     * capability.
     *
     * @return one row with bulk commands enabled and one with it disabled
     */
    @Parameters(name = "bulkCommands={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to exercise the bulk Command-API read
     *            path; {@code false} for the incremental path
     */
    public FindUniqueAndEditTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndEdit} edits the
     * sole matching record and durably persists the edit.
     * <p>
     * <strong>Start state:</strong> Three {@link Item Items} with distinct
     * codes, exactly one of which matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Item Items} with codes 1, 2, and 3.</li>
     * <li>Call {@code findUniqueAndEdit} with {@code code == 2} and a consumer
     * that sets {@code owner} to {@code "worker"}.</li>
     * <li>Re-load the returned {@link Item} by id from the database.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Item} has code 2 and
     * {@code owner == "worker"}, and the re-loaded {@link Item} shows the same
     * persisted {@code owner}.
     */
    @Test
    public void testFindUniqueAndEditEditsSoleMatchAndPersists() {
        runway.save(new Item(1), new Item(2), new Item(3));
        Item item = runway.findUniqueAndEdit(Item.class, code(2),
                i -> i.owner = "worker");
        Assert.assertNotNull(item);
        Assert.assertEquals(2, item.code);
        Assert.assertEquals("worker", item.owner);
        Item reloaded = runway.load(Item.class, item.id());
        Assert.assertEquals("worker", reloaded.owner);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndEdit} returns
     * {@code null} and never invokes the consumer when no record matches.
     * <p>
     * <strong>Start state:</strong> Three {@link Item Items} none of which
     * matches the criteria.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save {@link Item Items} with codes 1, 2, and 3.</li>
     * <li>Call {@code findUniqueAndEdit} with {@code code == 99} and a consumer
     * that flips an {@link AtomicBoolean}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result is {@code null} and the consumer
     * never ran.
     */
    @Test
    public void testFindUniqueAndEditReturnsNullAndSkipsConsumerWhenNoMatch() {
        runway.save(new Item(1), new Item(2), new Item(3));
        AtomicBoolean consumerRan = new AtomicBoolean(false);
        Item item = runway.findUniqueAndEdit(Item.class, code(99),
                i -> consumerRan.set(true));
        Assert.assertNull(item);
        Assert.assertFalse(consumerRan.get());
    }

    /**
     * <strong>Goal:</strong> Verify that {@code findUniqueAndEdit} throws
     * {@link DuplicateEntryException} when more than one record matches, and
     * that neither the mutation nor a commit occurs.
     * <p>
     * <strong>Start state:</strong> Two {@link Item Items} that share the same
     * code.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Item Items} both with code 7.</li>
     * <li>Call {@code findUniqueAndEdit} with {@code code == 7} and a consumer
     * that sets {@code owner}.</li>
     * <li>Catch the expected exception, then re-load both {@link Item
     * Items}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link DuplicateEntryException} is thrown
     * and neither re-loaded {@link Item} has an {@code owner} set.
     */
    @Test
    public void testFindUniqueAndEditThrowsOnDuplicateWithoutEditing() {
        Item one = new Item(7);
        Item two = new Item(7);
        runway.save(one, two);
        boolean threw = false;
        try {
            runway.findUniqueAndEdit(Item.class, code(7),
                    i -> i.owner = "worker");
        }
        catch (DuplicateEntryException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertNull(runway.load(Item.class, one.id()).owner);
        Assert.assertNull(runway.load(Item.class, two.id()).owner);
    }

    /**
     * Return a {@link Criteria} matching every {@link Item} whose {@code code}
     * equals the given {@code value}.
     *
     * @param value the code to match
     * @return the {@code code == value} {@link Criteria}
     */
    private static Criteria code(int value) {
        return Criteria.where().key("code").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * A {@link Record} with a queryable {@code code} and an editable
     * {@code owner}.
     *
     * @author Javier Lores
     */
    public static class Item extends Record {

        /**
         * The queryable code.
         */
        int code;

        /**
         * The editable owner, or {@code null} when unset.
         */
        String owner;

        /**
         * Construct a new instance.
         *
         * @param code the queryable code
         */
        public Item(int code) {
            this.code = code;
        }
    }

}
