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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Record} with overloaded id via interface default
 * methods.
 *
 * @author Jeff Nelson
 */
public class RecordOverloadIdTest extends RunwayBaseClientServerTest {

    @Test
    public void testDerivedIdOverridesPrintedIdKey() {
        IdentifiableRecord record = new IdentifiableRecord();
        record.name = "Jeff Nelson";
        record.save();

        // Verify the actual object id is preserved
        long actualId = record.id();
        Assert.assertTrue(actualId > 0);

        // Test getting the derived id property directly
        Object derivedId = record.get("id");
        Assert.assertEquals("Jeff Nelson_Identifier", derivedId);

        // Test that the derived id is included in the map output
        Map<String, Object> recordMap = record.map();
        Assert.assertTrue(recordMap.containsKey("id"));
        Assert.assertEquals("Jeff Nelson_Identifier", recordMap.get("id"));

        // Verify the actual id is still accessible via the object
        Assert.assertEquals(actualId, record.id());
    }

    /**
     * <strong>Goal:</strong> Verify that reading {@code id} from a
     * {@link Record} whose {@link Derived} method supplies the id runs no other
     * {@link Derived} method and loads no linked {@link Record}.
     * <p>
     * <strong>Start state:</strong> A saved {@link AliasedRecord} with a
     * {@link DeferredReference} to a saved {@link Target}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the {@link AliasedRecord} anew, so its {@link DeferredReference}
     * is not yet loaded.</li>
     * <li>Read {@code id} with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is the alias; the {@code other}
     * method never ran, and the {@link DeferredReference} is still not loaded.
     */
    @Test
    public void testGetIdReturnsDerivedIdWithoutResolvingOtherKeys() {
        Target target = new Target();
        target.name = "target";
        AliasedRecord record = new AliasedRecord();
        record.name = "aliased";
        record.link = new DeferredReference<>(target);
        runway.save(target, record);
        AliasedRecord loaded = runway.load(AliasedRecord.class, record.id());
        Assert.assertEquals("alias-" + record.id(), loaded.get("id"));
        Assert.assertEquals(0, loaded.otherRuns);
        Assert.assertNull(loaded.link.$ref());
    }

    /**
     * <strong>Goal:</strong> Verify that reading {@code id} returns the
     * database id when the {@link Derived} method named {@code id} returns
     * {@code null}.
     * <p>
     * <strong>Start state:</strong> A new {@link NullAliasRecord}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Read {@code id} with {@code get}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is the {@link Record Record's}
     * {@link Record#id()}.
     */
    @Test
    public void testGetIdReturnsDatabaseIdWhenDerivedIdIsNull() {
        NullAliasRecord record = new NullAliasRecord();
        Assert.assertEquals((long) record.id(), (long) record.get("id"));
    }

    public interface Identifiable {

        String getName();

        @Derived("id")
        default String identifier() {
            return getName() + "_Identifier";
        }
    }

    class IdentifiableRecord extends Record implements Identifiable {
        String name;

        @Override
        public String getName() {
            return name;
        }
    }

    /**
     * A type whose {@link Record Records} replace their id with an alias, in
     * the way that records hide their database ids.
     */
    public interface Aliased {

        /**
         * Return the alias that stands in for the database id.
         *
         * @return {@code "alias-"} followed by the database id
         */
        @Derived("id")
        default String alias() {
            return "alias-" + ((Record) this).id();
        }
    }

    /**
     * A {@link Record} that an {@link AliasedRecord} links to.
     */
    class Target extends Record {

        /**
         * The name.
         */
        String name;
    }

    /**
     * An {@link Aliased} {@link Record} with another {@link Derived} value and
     * a link that loads on demand.
     */
    class AliasedRecord extends Record implements Aliased {

        /**
         * The name.
         */
        String name;

        /**
         * A link to a {@link Target}.
         */
        DeferredReference<Target> link;

        /**
         * The number of times {@link #other()} ran.
         */
        transient int otherRuns;

        /**
         * Return a derived value other than the id, and count the run.
         *
         * @return {@code "other"}
         */
        @Derived
        public String other() {
            otherRuns++;
            return "other";
        }
    }

    /**
     * A {@link Record} whose {@link Derived} method named {@code id} returns
     * {@code null}.
     */
    class NullAliasRecord extends Record {

        /**
         * The name.
         */
        String name;

        /**
         * Return no alias.
         *
         * @return {@code null}
         */
        @Derived("id")
        public String alias() {
            return null;
        }
    }

}