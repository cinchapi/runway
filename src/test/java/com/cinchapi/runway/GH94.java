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
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Link;

/**
 * Repro GH-94 https://github.com/cinchapi/runway/issues/94
 * <p>
 * When a {@link Record} is loaded as a nested record from a parent's
 * pre-fetched data, the inner {@code convert(...)} method is passed the
 * navigation path (e.g. {@code "stone.pebbles"}) under a parameter named
 * {@code key}. The dangling-link cleanup branch then forwards that path-shaped
 * value to {@code concourse.remove(...)}, which rejects it with
 * {@code InvalidArgumentException} because Concourse keys cannot contain dots.
 * The result is that any record graph containing a dangling {@link Link} at
 * depth becomes unloadable until the dangling {@link Link} is removed by hand.
 *
 * @author Jeff Nelson
 */
public class GH94 extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that loading a {@link Boulder} whose
     * deeply-nested {@link Stone} &rarr; {@link Pebble} collection contains a
     * dangling element (a {@link Link} whose target has been cleared) succeeds
     * and omits that element.
     * <p>
     * The {@link Stone#pebbles} field resolves under a navigation prefix rather
     * than under its own field name, so a dangling element on it exercises a
     * different resolution path than one on a record the load reached directly.
     * <p>
     * <strong>Start state:</strong> A {@link Boulder} &rarr; {@link Stone}
     * &rarr; {@link List List&lt;Pebble&gt;} graph saved with three pebbles,
     * then one of the pebble records cleared. Concourse permits {@link Link
     * Links} to empty records, so clearing the target leaves the outgoing
     * {@link Link} on {@link Stone} intact.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create and save the {@link Boulder} graph with three {@link Pebble
     * Pebbles} on {@link Stone#pebbles}.</li>
     * <li>Call {@code client.clear(...)} on the second pebble's id.</li>
     * <li>Reload the {@link Boulder} via
     * {@code runway.load(Boulder.class, id)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The reload completes without throwing and
     * {@code boulder.stone.pebbles} contains exactly the two surviving
     * {@link Pebble Pebbles}.
     */
    @Test
    public void testNestedDanglingCollectionElementOmittedOnLoad() {
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
        Boulder boulder = new Boulder();
        boulder.label = "b";
        boulder.stone = stone;
        runway.save(boulder, stone, p1, p2, p3);

        client.clear(p2.id());

        Boulder loaded = runway.load(Boulder.class, boulder.id());
        Assert.assertNotNull(loaded);
        Assert.assertNotNull(loaded.stone);
        Assert.assertEquals(2, loaded.stone.pebbles.size());
        Set<String> labels = loaded.stone.pebbles.stream().map(p -> p.label)
                .collect(Collectors.toSet());
        Assert.assertTrue(labels.contains("alpha"));
        Assert.assertTrue(labels.contains("gamma"));
    }

    /**
     * Leaf-level {@link Record} used as the dangling-link target.
     */
    class Pebble extends Record {

        /**
         * A simple label for identification in assertions.
         */
        String label;
    }

    /**
     * Mid-level {@link Record} that holds a collection of {@link Pebble
     * Pebbles}. When loaded as a nested {@link Record} of {@link Boulder}, its
     * fields are processed under prefix {@code "stone."}, which exercises the
     * path-vs-key conflation in {@code Record#convert(...)}.
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

    /**
     * Top-level {@link Record} whose single nested {@link Stone} field forces
     * loading of {@link Stone} under prefix {@code "stone."}, the precondition
     * for reproducing the path-as-key bug at depth.
     */
    class Boulder extends Record {

        /**
         * A simple label for identification in assertions.
         */
        String label;

        /**
         * The nested {@link Stone}.
         */
        Stone stone;
    }

}
