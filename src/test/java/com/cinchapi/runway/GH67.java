/*
 * Copyright (c) 2013-2026 Cinchapi Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Repro GH-67
 *
 * @author Jeff Nelson
 */
public class GH67 extends RunwayBaseClientServerTest {

    @Test
    public void reproLoadParentWithEmptyLinkedRecord() {
        Parent parent = new Parent();
        parent.name = "Parent";
        Child child = new Child();
        child.name = "Child";
        parent.children.add(child);
        runway.save(parent, child);

        // Clear all data for the linked record to simulate empty record
        client.clear(child.id());

        // Loading should handle the empty linked record gracefully
        parent = runway.load(Parent.class, parent.id());
        Assert.assertNotNull(parent);
        Assert.assertTrue(parent.children.isEmpty());
    }

    class Parent extends Record {
        String name;
        List<Child> children = new ArrayList<>();
    }

    class Child extends Record {
        String name;
    }
}

