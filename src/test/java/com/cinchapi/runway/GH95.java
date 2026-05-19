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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Repro GH-95 https://github.com/cinchapi/runway/issues/95
 * <p>
 * When a {@link Record} is loaded with navigate pre-fetch,
 * {@code Record#convert(...)} reads the destination data for each
 * {@link com.cinchapi.concourse.Link Link} from the pre-fetched {@code targets}
 * map. The convert branch unconditionally dereferences the result of
 * {@code targets.get(target)} on the next line, so any pre-fetch miss (e.g.,
 * the target is a brand-new id, the target's record was cleared, or the
 * pre-fetch result set was scoped narrower than the actual link set) triggers a
 * {@link NullPointerException} that aborts the entire load. Pre-fetching is an
 * optimization, not a contract &mdash; a miss should degrade to a single record
 * fetch, not crash.
 *
 * @author Jeff Nelson
 */
public class GH95 extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that loading a {@link Stone} succeeds when
     * its pre-fetched {@code targets} map is missing an entry for one of the
     * {@link com.cinchapi.concourse.Link Links} on {@link Stone#pebbles}.
     * Without the fix, the convert branch dereferences a {@code null} value
     * from {@code targets.get(target)}, throwing {@link NullPointerException}
     * and aborting the entire load. With the fix, the missing entry triggers a
     * single-record fallback fetch and the referenced {@link Pebble} is
     * materialized normally.
     * <p>
     * <strong>Start state:</strong> A {@link Stone} with three {@link Pebble
     * Pebbles} saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Stone} with three {@link Pebble Pebbles}.</li>
     * <li>Build a {@code targets} map containing entries for two of the three
     * {@link Pebble Pebbles}, intentionally omitting the third &mdash;
     * simulating a pre-fetch result set that diverged from the actual
     * {@link Link} set on the loaded {@link Stone}.</li>
     * <li>Call the package-private static {@code Record.load(...)} with the
     * incomplete {@code targets} map.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reload completes without throwing,
     * {@code stone.pebbles} contains all three {@link Pebble Pebbles}, and the
     * omitted {@link Pebble} is fetched directly from Concourse rather than
     * read from the {@code targets} map.
     */
    @Test
    public void testLoadFallsBackWhenTargetMissingFromPrefetchMap() {
        Pebble p1 = new Pebble();
        p1.label = "alpha";
        Pebble p2 = new Pebble();
        p2.label = "beta";
        Pebble p3 = new Pebble();
        p3.label = "gamma";
        Stone stone = new Stone();
        stone.label = "s";
        stone.pebbles.add(p1);
        stone.pebbles.add(p2);
        stone.pebbles.add(p3);
        runway.save(stone, p1, p2, p3);

        Map<Long, Map<String, Set<Object>>> targets = Maps.newHashMap();
        targets.put(p1.id(), client.select(p1.id()));
        targets.put(p3.id(), client.select(p3.id()));

        Stone loaded = Record.load(Stone.class, stone.id(),
                new ConcurrentHashMap<>(), runway.connections, runway,
                client.time().getMicros(), null, targets);

        Assert.assertNotNull(loaded);
        Assert.assertEquals(3, loaded.pebbles.size());
        List<String> labels = new ArrayList<>();
        for (Pebble p : loaded.pebbles) {
            labels.add(p.label);
        }
        Assert.assertTrue(labels.contains("alpha"));
        Assert.assertTrue(labels.contains("beta"));
        Assert.assertTrue(labels.contains("gamma"));
    }

    /**
     * <strong>Goal:</strong> Verify that when {@code targets} contains an entry
     * for a {@link com.cinchapi.concourse.Link Link} target, the pre-fetched
     * data is used directly and no redundant single-record fetch is issued
     * &mdash; preserving the navigate pre-fetch optimization. Without this
     * guard, the missing-entry fallback could regress into an always-fall-back
     * path that silently doubles the round trips of every pre-selected load.
     * <p>
     * <strong>Start state:</strong> A {@link Stone} with one {@link Pebble}
     * saved.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Stone} with one {@link Pebble} whose stored
     * {@code label} is {@code "alpha"}.</li>
     * <li>Build a {@code targets} map whose entry for the {@link Pebble}
     * mirrors the actual stored data, except that {@code label} is overridden
     * to a sentinel value that does not exist in Concourse.</li>
     * <li>Call the package-private static {@code Record.load(...)} with the
     * doctored {@code targets} map.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded {@link Pebble Pebble's}
     * {@code label} is the sentinel from the {@code targets} map &mdash;
     * proving the fast path was used. If the fast path were bypassed, the
     * {@code label} would be {@code "alpha"} from Concourse.
     */
    @Test
    public void testLoadUsesPrefetchedDataWhenTargetIsPresent() {
        Pebble p1 = new Pebble();
        p1.label = "alpha";
        Stone stone = new Stone();
        stone.label = "s";
        stone.pebbles.add(p1);
        runway.save(stone, p1);

        Map<Long, Map<String, Set<Object>>> targets = Maps.newHashMap();
        Map<String, Set<Object>> overridden = Maps
                .newHashMap(client.select(p1.id()));
        overridden.put("label", Sets.newHashSet("PREFETCH_SENTINEL"));
        targets.put(p1.id(), overridden);

        Stone loaded = Record.load(Stone.class, stone.id(),
                new ConcurrentHashMap<>(), runway.connections, runway,
                client.time().getMicros(), null, targets);

        Assert.assertNotNull(loaded);
        Assert.assertEquals(1, loaded.pebbles.size());
        Assert.assertEquals("PREFETCH_SENTINEL", loaded.pebbles.get(0).label);
    }

    /**
     * <strong>Goal:</strong> Verify that loading a {@link Stone} with a
     * dangling {@link com.cinchapi.concourse.Link Link} in its
     * {@link Stone#pebbles} collection succeeds. This is a regression test for
     * the interaction between the navigate pre-fetch path and the dangling-link
     * cleanup logic in {@code Record#convert(...)}.
     * <p>
     * <strong>Start state:</strong> A {@link Stone} with three {@link Pebble
     * Pebbles} saved, one of which is then cleared. The
     * {@link com.cinchapi.concourse.Link Link} on {@link Stone#pebbles}
     * pointing at the cleared {@link Pebble} survives because Concourse permits
     * {@link com.cinchapi.concourse.Link Links} to empty records.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Stone} with three {@link Pebble Pebbles}.</li>
     * <li>Call {@code client.clear(...)} on the second {@link Pebble Pebble's}
     * id to leave its outgoing {@link com.cinchapi.concourse.Link Link}
     * dangling.</li>
     * <li>Reload the {@link Stone} via
     * {@code runway.load(Stone.class, id)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reload completes without throwing,
     * {@code stone.pebbles} contains exactly the two surviving {@link Pebble
     * Pebbles}, and the dangling {@link com.cinchapi.concourse.Link Link} has
     * been removed from {@link Stone Stone's} stored {@code pebbles} data.
     */
    @Test
    public void testLoadDoesNotNpeOnDanglingCollectionLink() {
        Pebble p1 = new Pebble();
        p1.label = "alpha";
        Pebble p2 = new Pebble();
        p2.label = "beta";
        Pebble p3 = new Pebble();
        p3.label = "gamma";
        Stone stone = new Stone();
        stone.label = "s";
        stone.pebbles.add(p1);
        stone.pebbles.add(p2);
        stone.pebbles.add(p3);
        runway.save(stone, p1, p2, p3);

        client.clear(p2.id());

        Stone loaded = runway.load(Stone.class, stone.id());
        Assert.assertNotNull(loaded);
        Assert.assertEquals(2, loaded.pebbles.size());
        List<String> labels = new ArrayList<>();
        for (Pebble p : loaded.pebbles) {
            labels.add(p.label);
        }
        Assert.assertTrue(labels.contains("alpha"));
        Assert.assertTrue(labels.contains("gamma"));
        Assert.assertEquals(2, client.select("pebbles", stone.id()).size());
    }

    /**
     * Leaf-level {@link Record} used as the {@link com.cinchapi.concourse.Link
     * Link} target for {@link Stone#pebbles}.
     */
    class Pebble extends Record {

        /**
         * A simple label for identification in assertions.
         */
        String label;
    }

    /**
     * {@link Record} that holds a {@link List List&lt;Pebble&gt;} field, used
     * to drive the collection branch of {@code Record#convert(...)}.
     */
    class Stone extends Record {

        /**
         * A simple label for identification in assertions.
         */
        String label;

        /**
         * A collection of nested {@link Pebble} references whose elements are
         * loaded by the collection branch of the nested-load logic.
         */
        List<Pebble> pebbles = new ArrayList<>();
    }

}
