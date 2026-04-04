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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Tests that verify {@link Selection} state and result propagation back to the
 * caller's original reference after execution.
 * <p>
 * When a {@link Selection} (or {@link Selection.Builder}) is passed to
 * {@link Runway#select(Selection...)}, the framework may resolve, duplicate, or
 * wrap it internally. These tests ensure that the original reference reflects
 * the execution outcome (state and result) regardless of how many layers of
 * wrapping occur.
 * </p>
 *
 * @author Jeff Nelson
 */
public class SelectionStatePropagationTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection.Builder} passed to
     * {@link Runway#select(Selection...)} reflects the execution state and
     * result after the call completes.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Player Players}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Player Players}.</li>
     * <li>Create a {@link Selection.Builder} for all {@link Player
     * Players}.</li>
     * <li>Pass the builder to {@link Runway#select(Selection...)}.</li>
     * <li>Check the builder's {@code state()} and {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The builder's {@code state()} returns
     * {@link Selection.State#FINISHED} and {@code get()} returns the set of
     * loaded {@link Player Players}.
     */
    @Test
    public void testBuilderStateReflectsExecutionAfterSelect() {
        new Player("Alice", 30).save();
        new Player("Bob", 10).save();

        Selection.InitialBuilder<Player> builder = Selection.of(Player.class);
        runway.select(builder);

        Assert.assertEquals(Selection.State.FINISHED, builder.state());
        Set<Player> result = builder.get();
        Assert.assertEquals(2, result.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection.QueryBuilder}
     * (chained from an {@link Selection.InitialBuilder}) propagates state and
     * result after execution.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Player Players} with
     * different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Player Players} with scores 10, 25, and 5.</li>
     * <li>Create a {@link Selection.QueryBuilder} by chaining {@code where()}
     * on an {@link Selection.InitialBuilder}.</li>
     * <li>Pass the query builder to {@link Runway#select(Selection...)}.</li>
     * <li>Check the query builder's {@code state()} and {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The query builder's {@code state()} returns
     * {@link Selection.State#FINISHED} and {@code get()} returns the matching
     * {@link Player Players}.
     */
    @Test
    public void testQueryBuilderPropagatesStateAfterSelect() {
        new Player("Alice", 10).save();
        new Player("Bob", 25).save();
        new Player("Charlie", 5).save();

        Criteria criteria = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(8).build();
        Selection.QueryBuilder<Player> builder = Selection.of(Player.class)
                .where(criteria);
        runway.select(builder);

        Assert.assertEquals(Selection.State.FINISHED, builder.state());
        Set<Player> result = builder.get();
        Assert.assertEquals(2, result.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a count {@link Selection.Builder}
     * propagates the count result back after execution.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Player Players}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Player Players}.</li>
     * <li>Create a count {@link Selection.CountBuilder}.</li>
     * <li>Pass it to {@link Runway#select(Selection...)}.</li>
     * <li>Check the builder's {@code state()} and {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The builder's {@code state()} returns
     * {@link Selection.State#FINISHED} and {@code get()} returns {@code 3}.
     */
    @Test
    public void testCountBuilderPropagatesResultAfterSelect() {
        new Player("Alice", 10).save();
        new Player("Bob", 25).save();
        new Player("Charlie", 5).save();

        Selection.CountBuilder<Player> builder = Selection.of(Player.class)
                .count();
        runway.select(builder);

        Assert.assertEquals(Selection.State.FINISHED, builder.state());
        int count = builder.get();
        Assert.assertEquals(3, count);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Selection#withInjectedFilter(Selection, java.util.function.Predicate)}
     * propagates state and result back to the original {@link Selection}.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Player Players}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Player Players}.</li>
     * <li>Build a {@link Selection} for all {@link Player Players}.</li>
     * <li>Wrap it via
     * {@link Selection#withInjectedFilter(Selection, java.util.function.Predicate)}.</li>
     * <li>Execute only the wrapped copy via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Check the original's {@code state()} and {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The original {@link Selection}'s
     * {@code state()} returns {@link Selection.State#FINISHED} and
     * {@code get()} returns the filtered results.
     */
    @Test
    public void testWithInjectedFilterPropagatesBackToOriginal() {
        new Player("Alice", 30).save();
        new Player("Bob", 10).save();

        Selection<Player> original = Selection.of(Player.class).build();
        Selection<Player> wrapped = Selection.withInjectedFilter(original,
                player -> player.score > 20);
        runway.select(wrapped);

        Assert.assertEquals(Selection.State.FINISHED, original.state());
        Set<Player> result = original.get();
        // NOTE: The original gets the wrapped (filtered) result
        // propagated back
        Assert.assertEquals(1, result.size());
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link Selection#withInjectedCriteria(Selection, Criteria)} propagates
     * state and result back to the original {@link Selection}.
     * <p>
     * <strong>Start state:</strong> Three saved {@link Player Players} with
     * different scores.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save three {@link Player Players}.</li>
     * <li>Build a {@link Selection} for {@link Player Players} matching
     * {@code score > 5}.</li>
     * <li>Wrap it via
     * {@link Selection#withInjectedCriteria(Selection, Criteria)} with an
     * additional criteria {@code score < 30}.</li>
     * <li>Execute only the wrapped copy.</li>
     * <li>Check the original's {@code state()} and {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The original {@link Selection}'s
     * {@code state()} returns {@link Selection.State#FINISHED} and
     * {@code get()} returns the result filtered by both criteria.
     */
    @Test
    public void testWithInjectedCriteriaPropagatesBackToOriginal() {
        new Player("Alice", 30).save();
        new Player("Bob", 15).save();
        new Player("Charlie", 5).save();

        Criteria base = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(5).build();
        Selection<Player> original = Selection.of(Player.class).where(base)
                .build();

        Criteria injected = Criteria.where().key("score")
                .operator(Operator.LESS_THAN).value(30).build();
        Selection<Player> wrapped = Selection.withInjectedCriteria(original,
                injected);
        runway.select(wrapped);

        Assert.assertEquals(Selection.State.FINISHED, original.state());
        Set<Player> result = original.get();
        Assert.assertEquals(1, result.size());
    }

    /**
     * <strong>Goal:</strong> Verify that an already-executed
     * {@link Selection.Builder} cannot be resubmitted.
     * <p>
     * <strong>Start state:</strong> One saved {@link Player}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save one {@link Player}.</li>
     * <li>Create and execute a {@link Selection.Builder} via
     * {@link Runway#select(Selection...)}.</li>
     * <li>Attempt to pass the same builder to
     * {@link Runway#select(Selection...)} again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The second call throws
     * {@link IllegalStateException}.
     */
    @Test(expected = IllegalStateException.class)
    public void testResubmittingExecutedBuilderThrows() {
        new Player("Alice", 10).save();

        Selection.InitialBuilder<Player> builder = Selection.of(Player.class);
        runway.select(builder);
        Assert.assertEquals(Selection.State.FINISHED, builder.state());

        // Second submission should throw
        runway.select(builder);
    }

    /**
     * <strong>Goal:</strong> Verify that state propagates through multiple
     * layers of wrapping (criteria injection followed by filter injection).
     * <p>
     * <strong>Start state:</strong> Two saved {@link Player Players}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Player Players}.</li>
     * <li>Create an original {@link Selection.Builder}.</li>
     * <li>Wrap it via
     * {@link Selection#withInjectedFilter(Selection, java.util.function.Predicate)}.</li>
     * <li>Wrap the result again via
     * {@link Selection#withInjectedFilter(Selection, java.util.function.Predicate)}.</li>
     * <li>Execute only the doubly-wrapped copy.</li>
     * <li>Check the original builder's {@code state()} and {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The original builder's {@code state()} returns
     * {@link Selection.State#FINISHED} and {@code get()} returns results.
     */
    @Test
    public void testMultiDepthWrappingPropagatesBackToOriginal() {
        new Player("Alice", 30).save();
        new Player("Bob", 10).save();

        Selection.InitialBuilder<Player> builder = Selection.of(Player.class);
        Selection<Player> wrapped1 = Selection.withInjectedFilter(builder,
                player -> true);
        Selection<Player> wrapped2 = Selection.withInjectedFilter(wrapped1,
                player -> true);
        runway.select(wrapped2);

        Assert.assertEquals(Selection.State.FINISHED, builder.state());
        Set<Player> result = builder.get();
        Assert.assertEquals(2, result.size());
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Selection.Builder} used with
     * {@link DatabaseInterface#fetch(Selection)} reflects the execution state
     * and result after the call completes.
     * <p>
     * <strong>Start state:</strong> Two saved {@link Player Players}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two {@link Player Players}.</li>
     * <li>Create a {@link Selection.Builder} for all {@link Player
     * Players}.</li>
     * <li>Pass the builder to {@link DatabaseInterface#fetch(Selection)}.</li>
     * <li>Check the builder's {@code state()} and {@code get()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The builder's {@code state()} returns
     * {@link Selection.State#FINISHED} and {@code get()} returns the same
     * result as {@code fetch()}.
     */
    @Test
    public void testFetchPropagatesStateToBuilder() {
        new Player("Alice", 30).save();
        new Player("Bob", 10).save();

        Selection.InitialBuilder<Player> builder = Selection.of(Player.class);
        Set<Player> fetched = runway.fetch(builder);

        Assert.assertEquals(Selection.State.FINISHED, builder.state());
        Set<Player> fromBuilder = builder.get();
        Assert.assertEquals(fetched.size(), fromBuilder.size());
    }

}
