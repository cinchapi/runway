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

import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for functionality to cascade deletes in {@link Record Records}.
 *
 * @author Jeff Nelson
 */
public class RunwayCascadeDeleteTest extends RunwayBaseClientServerTest {
    
    @Test
    public void testCascadeDeleteOnPublicField() {
        ParentRecord parent = new ParentRecord();
        parent.child = new ChildRecord();
        parent.deleteOnSave();
        parent.save();
        Assert.assertNull(runway.load(ParentRecord.class, parent.id()));
        Assert.assertNull(runway.load(ChildRecord.class, parent.child.id()));
    }
    
    

    class ParentRecord extends Record {
        @CascadeDelete
        public ChildRecord child;

        public Collection<ChildRecord> children;

        @CascadeDelete
        protected InnerRecord inner;
    }

    class ChildRecord extends Record {
        @CascadeDelete
        private ParentRecord parent;

        protected Collection<InnerRecord> innerChildren;
    }

    class InnerRecord extends Record {
        public ChildRecord sibling;
    }

    class InheritedRecord extends ParentRecord {
        @CascadeDelete
        private AdditionalRecord additionalRecord;
    }

    class AdditionalRecord extends Record {
        public String data;
    }

}
