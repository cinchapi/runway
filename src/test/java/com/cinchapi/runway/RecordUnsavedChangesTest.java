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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.util.Random;

/**
 * Tests for {@link Record} unsaved changes detection and save optimization.
 *
 * @author Jeff Nelson
 */
public class RecordUnsavedChangesTest extends AbstractRecordTest {

    @Test
    public void testDetectNoUnsavedChanges() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 37;
        person.alive = true;
        person.save();
        Assert.assertFalse(person.hasUnsavedChanges());
    }

    @Test
    public void testDetectNoUnsavedChangesAfterLoad() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 37;
        person.alive = true;
        person.save();
        person = runway.load(Mock.class, person.id());
        Assert.assertFalse(person.hasUnsavedChanges());
    }

    @Test
    public void testNewRecordHasUnsavedChanges() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 37;
        person.alive = true;
        Assert.assertTrue(person.hasUnsavedChanges());
        person.save();
        Assert.assertFalse(person.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesDoesNotRequireLoad() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 37;
        person.alive = true;
        person.save();
        person.age = 38;
        Assert.assertTrue(person.hasUnsavedChanges());
    }

    @Test
    public void testNoUnsavedChangesIfChangesAreReverted() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 37;
        person.alive = true;
        person.save();
        person.age = 38;
        person.age = 37;
        Assert.assertFalse(person.hasUnsavedChanges());
        person = runway.load(Mock.class, person.id());
        person.age = 38;
        person.age = 37;
        Assert.assertFalse(person.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesUsingSet() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 37;
        person.alive = true;
        person.save();
        person.set("age", 38);
        Assert.assertTrue(person.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesIfValueRemoved() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.age = 37;
        person.alive = true;
        person.save();
        person.age = null;
        Assert.assertTrue(person.hasUnsavedChanges());
    }

    @Test
    public void testNoUnsavedChangesIfValueisNotAdded() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.alive = true;
        person.save();
        person.age = null;
        Assert.assertFalse(person.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesIfValueisAdded() {
        Mock person = new Mock();
        person.name = "Jeff Nelson";
        person.alive = true;
        person.save();
        person.age = 37;
        Assert.assertTrue(person.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesInSequenceAdd() {
        Lock lock = new Lock(new ArrayList<>());
        Dock a = new Dock("a");
        lock.docks.add(a);
        lock.save();
        lock.docks.add(new Dock("b'"));
        Assert.assertTrue(lock.hasUnsavedChanges());
    }

    @Test
    public void testNoUnsavedChangesIfSequenceDoesNotChange() {
        Lock lock = new Lock(new ArrayList<>());
        Dock a = new Dock("a");
        lock.docks.add(a);
        lock.save();
        Assert.assertFalse(lock.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesInSequenceRemoval() {
        Lock lock = new Lock(new ArrayList<>());
        Dock a = new Dock("a");
        Dock b = new Dock("b");
        lock.docks.add(a);
        lock.docks.add(b);
        lock.save();
        lock.docks.remove(1);
        Assert.assertTrue(lock.hasUnsavedChanges());
    }

    @Test
    public void testNoUnsavedChangesIfSequenceRemovalReverted() {
        Lock lock = new Lock(new ArrayList<>());
        Dock a = new Dock("a");
        Dock b = new Dock("b");
        lock.docks.add(a);
        lock.docks.add(b);
        lock.save();
        lock.docks.remove(1);
        lock.docks.add(b);
        Assert.assertFalse(lock.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesIfSequenceOrderChanges() {
        Lock lock = new Lock(new ArrayList<>());
        Dock a = new Dock("a");
        Dock b = new Dock("b");
        lock.docks.add(a);
        lock.docks.add(b);
        lock.save();
        lock.docks.remove(0);
        lock.docks.add(a);
        Assert.assertTrue(lock.hasUnsavedChanges());
    }

    @Test
    public void testUnsavedChangesIfLinkValueChanges() {
        Company cinchapi = new Company("Cinchapi");
        Company blavity = new Company("Blavity");
        User user = new User("Jeff Nelson", "jeff@foo.com", cinchapi);
        runway.save(cinchapi, blavity, user);
        user.company = blavity;
        Assert.assertTrue(user.hasUnsavedChanges());
        Assert.assertFalse(cinchapi.hasUnsavedChanges());
        Assert.assertFalse(blavity.hasUnsavedChanges());
    }

    @Test
    public void testNoUnsavedChangesIfLinkDataChanges() {
        Company cinchapi = new Company("Cinchapi");
        Company blavity = new Company("Blavity");
        User user = new User("Jeff Nelson", "jeff@foo.com", cinchapi);
        runway.save(cinchapi, blavity, user);
        cinchapi.name = "Cinchapi Inc.";
        Assert.assertFalse(user.hasUnsavedChanges());
        Assert.assertTrue(cinchapi.hasUnsavedChanges());
        Assert.assertFalse(blavity.hasUnsavedChanges());
    }

    @Test
    public void testChangedLinkedRecordIsSavedEvenIfParentHasNoChanges() {
        Company cinchapi = new Company("Cinchapi");
        Company blavity = new Company("Blavity");
        User user = new User("Jeff Nelson", "jeff@foo.com", cinchapi);
        runway.save(cinchapi, blavity, user);
        user.company.name = "Cinchapi Inc";
        Assert.assertFalse(user.hasUnsavedChanges());
        user.save();
        user = runway.load(User.class, user.id());
        Assert.assertEquals("Cinchapi Inc", user.company.name);
        cinchapi = runway.load(Company.class, cinchapi.id());
        Assert.assertEquals("Cinchapi Inc", cinchapi.name);
    }

    @Test
    public void testSaveIsNoOpIfNoUnsavedChanges() {
        HasBeforeSaveHook a = new HasBeforeSaveHook("a");
        a.save();
        Assert.assertEquals(1, a.saves);
        a.save();
        Assert.assertEquals(1, a.saves);
        for (int i = 0; i < Random.getScaleCount(); ++i) {
            a.save();
            Assert.assertEquals(1, a.saves);
        }
    }

    @Test
    public void testSaveChildChangesEvenIfNoUnsavedParentChanges() {
        HasBeforeSaveHook a = new HasBeforeSaveHook("a");
        HasBeforeSaveHook b = new HasBeforeSaveHook("b");
        a.child = b;
        a.save();
        Assert.assertEquals(1, a.saves);
        Assert.assertEquals(1, b.saves);
        b.value = "b1";
        a.save();
        Assert.assertEquals(1, a.saves);
        Assert.assertEquals(2, b.saves);
    }

    @Test
    public void testSaveParentChangesDoesNotForceSaveInChildWithNoUnsavedChanges() {
        HasBeforeSaveHook a = new HasBeforeSaveHook("a");
        HasBeforeSaveHook b = new HasBeforeSaveHook("b");
        a.child = b;
        a.save();
        Assert.assertEquals(1, a.saves);
        Assert.assertEquals(1, b.saves);
        a.value = "a1";
        a.save();
        Assert.assertEquals(2, a.saves);
        Assert.assertEquals(1, b.saves);
    }

    @Test
    public void testCreatingRecordAndLinkingExistingRecordDoesNotSaveExistingRecordWithNoChanges() {
        HasBeforeSaveHook a = new HasBeforeSaveHook("a");
        a.save();
        a = runway.load(HasBeforeSaveHook.class, a.id());
        HasBeforeSaveHook b = new HasBeforeSaveHook("b");
        b.child = a;
        b.save();
        Assert.assertEquals(0, a.saves);
        Assert.assertEquals(1, b.saves);
    }

    @Test
    public void testCircularDependencyOnlySavedOnce() {
        HasBeforeSaveHook a = new HasBeforeSaveHook("a");
        HasBeforeSaveHook b = new HasBeforeSaveHook("b");
        a.child = b;
        b.child = a;
        a.save();
        Assert.assertEquals(1, a.saves);
        Assert.assertEquals(1, b.saves);
    }

}
