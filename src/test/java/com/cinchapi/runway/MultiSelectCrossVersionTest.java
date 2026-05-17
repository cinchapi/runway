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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.profile.Benchmark;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.test.CrossVersionTest;
import com.cinchapi.concourse.test.runners.CrossVersionTestRunner.Versions;
import com.cinchapi.concourse.thrift.Operator;

/**
 * Cross-version benchmarks for {@link Runway#select(Selection...)}
 * multi-selection dispatch and {@link Runway#save(Record...)} bulk persist.
 * <p>
 * The {@code 0.12.8} server lacks the Concourse Command API, so
 * {@code select(Selection...)} dispatches through the combinable/isolated path
 * and {@code save(Record...)} dispatches per-record. The latest server supports
 * {@code prepare()}/{@code submit()}, so {@code select(Selection...)} batches
 * every {@link Selection} through a single
 * {@link com.cinchapi.runway.db.BatchReader} and {@code save(Record...)}
 * batches every {@link Record} through a single-round-trip
 * {@link com.cinchapi.runway.db.BatchSaver}. Each {@link Test} records a
 * latency stat that surfaces in the cross-version comparison table at the end
 * of the run, exposing how the new paths perform relative to the legacy ones.
 * Correctness for the dispatch and bulk-save paths is covered by the
 * single-version integration tests; the methods here exist solely to produce
 * timings.
 * <p>
 * Every benchmark uses the {@link Benchmark} builder with
 * {@link Benchmark.ConfigStage#warmups(int) warmups} so the JIT (and the
 * server-side reflective dispatch path on Command-API-capable versions) is warm
 * before measurement begins.
 *
 * @author Jeff Nelson
 */
@Versions({ "0.12.8", Testing.CONCOURSE_VERSION })
public class MultiSelectCrossVersionTest extends CrossVersionTest {

    /**
     * The number of warmup iterations executed before each timed run. Chosen to
     * push every code path past HotSpot's reflective-dispatch inflation
     * threshold (~15 invocations per call site) even for the smallest benchmark
     * (two commands per iteration produces 100 calls warmed).
     */
    private static final int WARMUPS = 50;

    /**
     * The number of timed iterations averaged into each recorded stat.
     */
    private static final int ITERATIONS = 10;

    private Runway runway;

    @Override
    public void afterStartedTest() {
        try {
            runway.close();
        }
        catch (Exception e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    @Override
    public void beforeEachTest() {
        runway = Runway.builder().port(server.getClientPort()).build();
    }

    /**
     * <strong>Goal:</strong> Measure the average end-to-end latency of a
     * multi-selection dispatch mixing every {@link DatabaseSelection} subtype
     * &mdash; load-by-id, find, load-class, count, and unique &mdash; so the
     * combinable/isolated path on older servers and the batched
     * {@link com.cinchapi.runway.db.BatchReader} path on newer servers can be
     * compared head-to-head.
     * <p>
     * <strong>Start state:</strong> 100 {@link Widget Widgets} with unique
     * scores {@code 0..99} and one {@link Gadget} named {@code "target"} are
     * persisted.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Persist the workload.</li>
     * <li>Build a {@link Benchmark} whose action constructs five fresh
     * {@link Selection Selections} (one of each subtype) and submits them in a
     * single {@link Runway#select(Selection...)} call.</li>
     * <li>Run {@value #WARMUPS} warmups, then average the action over
     * {@value #ITERATIONS} timed iterations.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The average per-call latency is recorded
     * against the running server version for the cross-version comparison
     * table.
     */
    @Test
    public void testMixedSubtypesBenchmark() {
        for (int i = 0; i < 100; ++i) {
            new Widget("w" + i, i).save();
        }
        Gadget target = new Gadget("target", "red");
        target.save();
        long targetId = target.id();

        Criteria scoreGt25 = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(25).build();
        Criteria scoreEq50 = Criteria.where().key("score")
                .operator(Operator.EQUALS).value(50).build();

        double avg = Benchmark.measure(() -> {
            Selection<Gadget> byId = Selection.of(Gadget.class).id(targetId)
                    .build();
            Selection<Widget> byCriteria = Selection.of(Widget.class)
                    .criteria(scoreGt25).build();
            Selection<Widget> byClass = Selection.of(Widget.class).build();
            Selection<Widget> byCount = Selection.of(Widget.class)
                    .criteria(scoreGt25).count().build();
            Selection<Widget> byUnique = Selection.of(Widget.class)
                    .criteria(scoreEq50).unique().build();
            runway.select(byId, byCriteria, byClass, byCount, byUnique);
        }).in(TimeUnit.MILLISECONDS).warmups(WARMUPS).average(ITERATIONS)
                .join();
        record("Multi-Select (Mixed Subtypes)", avg);
    }

    /**
     * <strong>Goal:</strong> Measure the average end-to-end latency of a
     * multi-selection dispatch carrying two same-class {@link Selection
     * Selections} with divergent criteria &mdash; the case the
     * combinable/isolated dispatch isolates to avoid {@code demux}
     * cross-contamination &mdash; so the isolation cost on older servers and
     * the batched cost on newer servers can be compared head-to-head.
     * <p>
     * <strong>Start state:</strong> 100 {@link Widget Widgets} with unique
     * scores {@code 0..99} are persisted.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Persist the workload.</li>
     * <li>Build a {@link Benchmark} whose action constructs two fresh
     * {@link Widget} finds with non-overlapping criteria and submits them in a
     * single {@link Runway#select(Selection...)} call.</li>
     * <li>Run {@value #WARMUPS} warmups, then average the action over
     * {@value #ITERATIONS} timed iterations.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The average per-call latency is recorded
     * against the running server version for the cross-version comparison
     * table.
     */
    @Test
    public void testSameClassDivergentCriteriaBenchmark() {
        for (int i = 0; i < 100; ++i) {
            new Widget("w" + i, i).save();
        }

        Criteria lt25 = Criteria.where().key("score")
                .operator(Operator.LESS_THAN).value(25).build();
        Criteria gt75 = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(75).build();

        double avg = Benchmark.measure(() -> {
            Selection<Widget> low = Selection.of(Widget.class).criteria(lt25)
                    .build();
            Selection<Widget> high = Selection.of(Widget.class).criteria(gt75)
                    .build();
            runway.select(low, high);
        }).in(TimeUnit.MILLISECONDS).warmups(WARMUPS).average(ITERATIONS)
                .join();
        record("Multi-Select (Divergent Criteria)", avg);
    }

    /**
     * <strong>Goal:</strong> Measure the average end-to-end latency of a
     * multi-selection dispatch in which one {@link Selection} is already in the
     * thread-local reservation cache and short-circuits inside the dispatch
     * &mdash; so the cost of a mixed cached/uncached batch on older and newer
     * servers can be compared head-to-head.
     * <p>
     * <strong>Start state:</strong> 50 {@link Widget Widgets} with unique
     * scores {@code 0..49} are persisted, a reservation is held, and the
     * reservation cache is pre-warmed with a load-class for {@link Widget}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Persist the workload, {@code reserve()}, and pre-warm the cache.</li>
     * <li>Build a {@link Benchmark} whose action constructs a fresh load-class
     * {@link Selection} for {@link Widget} (cache hit) and a fresh find
     * {@link Selection} for one named record (cache miss), then submits both in
     * a single {@link Runway#select(Selection...)} call.</li>
     * <li>Run {@value #WARMUPS} warmups, then average the action over
     * {@value #ITERATIONS} timed iterations.</li>
     * <li>{@code unreserve()} in a {@code finally} block.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The average per-call latency is recorded
     * against the running server version for the cross-version comparison
     * table.
     */
    @Test
    public void testCachedShortCircuitBenchmark() {
        for (int i = 0; i < 50; ++i) {
            new Widget("w" + i, i).save();
        }

        Criteria nameW0 = Criteria.where().key("name").operator(Operator.EQUALS)
                .value("w0").build();

        runway.reserve();
        try {
            runway.select(Selection.of(Widget.class).build());

            double avg = Benchmark.measure(() -> {
                Selection<Widget> all = Selection.of(Widget.class).build();
                Selection<Widget> find = Selection.of(Widget.class)
                        .criteria(nameW0).build();
                runway.select(all, find);
            }).in(TimeUnit.MILLISECONDS).warmups(WARMUPS).average(ITERATIONS)
                    .join();
            record("Multi-Select (Cached Short-Circuit)", avg);
        }
        finally {
            runway.unreserve();
        }
    }

    /**
     * <strong>Goal:</strong> Measure the average end-to-end latency of a
     * multi-selection dispatch carrying three duplicate {@link Selection
     * Selections} (distinct objects, identical query parameters) so the dedup
     * cost on older and newer servers can be compared head-to-head.
     * <p>
     * <strong>Start state:</strong> 100 {@link Widget Widgets} with unique
     * scores {@code 0..99} are persisted.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Persist the workload.</li>
     * <li>Build a {@link Benchmark} whose action constructs three fresh
     * {@link Widget} finds with identical criteria and submits them in a single
     * {@link Runway#select(Selection...)} call.</li>
     * <li>Run {@value #WARMUPS} warmups, then average the action over
     * {@value #ITERATIONS} timed iterations.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The average per-call latency is recorded
     * against the running server version for the cross-version comparison
     * table.
     */
    @Test
    public void testDuplicateSelectionsBenchmark() {
        for (int i = 0; i < 100; ++i) {
            new Widget("w" + i, i).save();
        }

        Criteria gt50 = Criteria.where().key("score")
                .operator(Operator.GREATER_THAN).value(50).build();

        double avg = Benchmark.measure(() -> {
            Selection<Widget> a = Selection.of(Widget.class).criteria(gt50)
                    .build();
            Selection<Widget> b = Selection.of(Widget.class).criteria(gt50)
                    .build();
            Selection<Widget> c = Selection.of(Widget.class).criteria(gt50)
                    .build();
            runway.select(a, b, c);
        }).in(TimeUnit.MILLISECONDS).warmups(WARMUPS).average(ITERATIONS)
                .join();
        record("Multi-Select (Duplicates)", avg);
    }

    /**
     * <strong>Goal:</strong> Measure the average end-to-end latency of a
     * multi-selection dispatch carrying load-by-id {@link Selection Selections}
     * against a {@link Record} class with descendants &mdash; forcing the
     * section-lookup branch &mdash; and scoped by {@link Realms}, so the
     * hierarchy + realms dispatch cost on older and newer servers can be
     * compared head-to-head.
     * <p>
     * <strong>Start state:</strong> 50 {@link SuperWidget} records under realm
     * {@code "alpha"} and 50 {@link SubWidget} records under realm
     * {@code "beta"} are persisted.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Persist the workload, capturing one {@link SuperWidget} id from realm
     * {@code "alpha"} and one {@link SubWidget} id from realm
     * {@code "beta"}.</li>
     * <li>Build a {@link Benchmark} whose action constructs two fresh
     * load-by-id {@link SuperWidget} {@link Selection Selections} scoped to
     * {@code Realms.only("alpha")} &mdash; one in-realm, one out-of-realm
     * &mdash; plus a load-class {@link Selection} that includes descendants,
     * and submits all three in a single {@link Runway#select(Selection...)}
     * call.</li>
     * <li>Run {@value #WARMUPS} warmups, then average the action over
     * {@value #ITERATIONS} timed iterations.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The average per-call latency is recorded
     * against the running server version for the cross-version comparison
     * table.
     */
    @Test
    public void testHierarchyAndRealmsBenchmark() {
        long alphaId = 0;
        long betaId = 0;
        for (int i = 0; i < 50; ++i) {
            SuperWidget alpha = new SuperWidget("alpha" + i);
            alpha.addRealm("alpha");
            alpha.save();
            if(i == 0) {
                alphaId = alpha.id();
            }
            SubWidget beta = new SubWidget("beta" + i, "extra" + i);
            beta.addRealm("beta");
            beta.save();
            if(i == 0) {
                betaId = beta.id();
            }
        }
        long inRealmId = alphaId;
        long outOfRealmId = betaId;

        double avg = Benchmark.measure(() -> {
            Selection<SuperWidget> inRealm = Selection.of(SuperWidget.class)
                    .id(inRealmId).realms(Realms.only("alpha")).build();
            Selection<SuperWidget> outOfRealm = Selection.of(SuperWidget.class)
                    .id(outOfRealmId).realms(Realms.only("alpha")).build();
            Selection<SuperWidget> all = Selection.of(SuperWidget.class).any()
                    .build();
            runway.select(inRealm, outOfRealm, all);
        }).in(TimeUnit.MILLISECONDS).warmups(WARMUPS).average(ITERATIONS)
                .join();
        record("Multi-Select (Hierarchy + Realms)", avg);
    }

    /**
     * <strong>Goal:</strong> Measure the average end-to-end latency of a
     * bulk-save call so the per-record dispatch on older servers and the
     * single-round-trip {@link com.cinchapi.runway.db.BatchSaver} path on newer
     * servers can be compared head-to-head.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Benchmark} whose action constructs a fresh array of
     * fifty {@link Widget Widgets} and persists them in a single
     * {@link Runway#save(Record...)} call.</li>
     * <li>Run {@value #WARMUPS} warmups, then average the action over
     * {@value #ITERATIONS} timed iterations.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The average per-call latency is recorded
     * against the running server version for the cross-version comparison
     * table.
     */
    @Test
    public void testBulkSaveBenchmark() {
        double avg = Benchmark.measure(() -> {
            Widget[] batch = new Widget[50];
            for (int i = 0; i < batch.length; ++i) {
                batch[i] = new Widget("w" + i, i);
            }
            runway.save(batch);
        }).in(TimeUnit.MILLISECONDS).warmups(WARMUPS).average(ITERATIONS)
                .join();
        record("Bulk Save", avg);
    }

    /**
     * A {@link Record} class with at least one descendant so that loading by id
     * triggers the section-lookup branch in {@code $selectRecord}.
     */
    class SuperWidget extends Record {

        String name;

        SuperWidget(String name) {
            this.name = name;
        }
    }

    /**
     * A subclass of {@link SuperWidget} that gives the hierarchy more than one
     * section.
     */
    class SubWidget extends SuperWidget {

        String extra;

        SubWidget(String name, String extra) {
            super(name);
            this.extra = extra;
        }
    }

    /**
     * A simple test {@link Record} with a name and score.
     */
    class Widget extends Record {

        String name;

        int score;

        Widget(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }

    /**
     * A second test {@link Record} type used to vary the dispatch by class.
     */
    class Gadget extends Record {

        String name;

        String color;

        Gadget(String name, String color) {
            this.name = name;
            this.color = color;
        }
    }

}
