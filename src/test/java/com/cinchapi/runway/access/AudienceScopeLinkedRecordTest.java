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
package com.cinchapi.runway.access;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.google.common.collect.Lists;

/**
 * Integration tests verifying that {@link Scope}-based visibility is enforced
 * when navigating to linked {@link Record Records} during
 * {@link Audience#frame(Record) framing}. These tests cover the gap where a
 * linked {@link Record} has a registered {@link Scope} that is more restrictive
 * than its instance-level {@link AccessControl} permissions.
 *
 * @author Jeff Nelson
 */
public class AudienceScopeLinkedRecordTest extends RunwayBaseClientServerTest {

    /**
     * Clear the registry before and after each test.
     */
    @Before
    @After
    public void clearRegistry() {
        AccessControlSupport.VISIBILITY_SCOPES.clear();
    }

    /**
     * <strong>Goal:</strong> Verify that a linked {@link ScopedOwner} whose
     * data falls outside the registered {@link Scope} is excluded when the
     * parent {@link Project} is framed.
     * <p>
     * <strong>Start state:</strong> A {@link ScopedOwner} with
     * {@code status = "inactive"} linked from a {@link Project}. A criteria
     * {@link Scope} is registered for {@link ScopedOwner} that requires
     * {@code status = "active"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria {@link Scope} for {@link ScopedOwner} filtering
     * on {@code status = "active"}.</li>
     * <li>Save a {@link TestUser}, an inactive {@link ScopedOwner}, and a
     * {@link Project} linking to that owner.</li>
     * <li>Frame the {@link Project} as the {@link TestUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code owner} field in the framed data is
     * {@code null} because the linked {@link ScopedOwner} does not satisfy the
     * {@link Scope}.
     */
    @Test
    public void testLinkedRecordOutsideScopeIsNullWhenFramed() {
        AccessControl.registerVisibilityScope(ScopedOwner.class,
                audience -> Scope.of(Criteria.where().key("status")
                        .operator(Operator.EQUALS).value("active").build()));
        TestUser user = new TestUser("alice");
        user.save();
        ScopedOwner owner = new ScopedOwner("Acme", "inactive");
        owner.save();
        Project project = new Project("Alpha", owner);
        project.save();
        Map<String, Object> frame = user.frame(project);
        Assert.assertNotNull(frame);
        Assert.assertNull(frame.get("owner"));
    }

    /**
     * <strong>Goal:</strong> Verify that a linked {@link ScopedOwner} whose
     * data falls within the registered {@link Scope} is included when the
     * parent {@link Project} is framed.
     * <p>
     * <strong>Start state:</strong> A {@link ScopedOwner} with
     * {@code status = "active"} linked from a {@link Project}. A criteria
     * {@link Scope} is registered for {@link ScopedOwner} that requires
     * {@code status = "active"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria {@link Scope} for {@link ScopedOwner} filtering
     * on {@code status = "active"}.</li>
     * <li>Save a {@link TestUser}, an active {@link ScopedOwner}, and a
     * {@link Project} linking to that owner.</li>
     * <li>Frame the {@link Project} as the {@link TestUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code owner} field in the framed data is
     * a non-null map containing the {@link ScopedOwner ScopedOwner's} visible
     * fields.
     */
    @Test
    public void testLinkedRecordInsideScopeIsVisibleWhenFramed() {
        AccessControl.registerVisibilityScope(ScopedOwner.class,
                audience -> Scope.of(Criteria.where().key("status")
                        .operator(Operator.EQUALS).value("active").build()));
        TestUser user = new TestUser("alice");
        user.save();
        ScopedOwner owner = new ScopedOwner("Acme", "active");
        owner.save();
        Project project = new Project("Alpha", owner);
        project.save();
        Map<String, Object> frame = user.frame(project);
        Assert.assertNotNull(frame);
        Assert.assertNotNull(frame.get("owner"));
        Assert.assertTrue(frame.get("owner") instanceof Map);
    }

    /**
     * <strong>Goal:</strong> Verify that when a {@link Project} has a
     * collection of linked {@link ScopedOwner ScopedOwners}, only those within
     * the registered {@link Scope} appear in the framed data.
     * <p>
     * <strong>Start state:</strong> Three {@link ScopedOwner ScopedOwners}
     * &mdash; two active, one inactive &mdash; linked from a
     * {@link ProjectWithMembers}. A criteria {@link Scope} is registered
     * requiring {@code status = "active"}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria {@link Scope} for {@link ScopedOwner} filtering
     * on {@code status = "active"}.</li>
     * <li>Save a {@link TestUser} and three {@link ScopedOwner ScopedOwners}
     * (two active, one inactive).</li>
     * <li>Save a {@link ProjectWithMembers} linking to all three owners.</li>
     * <li>Frame the {@link ProjectWithMembers} as the {@link TestUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code members} field in the framed data
     * contains exactly two entries corresponding to the active
     * {@link ScopedOwner ScopedOwners}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testLinkedRecordInCollectionFilteredByScope() {
        AccessControl.registerVisibilityScope(ScopedOwner.class,
                audience -> Scope.of(Criteria.where().key("status")
                        .operator(Operator.EQUALS).value("active").build()));
        TestUser user = new TestUser("alice");
        user.save();
        ScopedOwner active1 = new ScopedOwner("Acme", "active");
        ScopedOwner active2 = new ScopedOwner("Beta", "active");
        ScopedOwner inactive = new ScopedOwner("Gamma", "inactive");
        runway.save(active1, active2, inactive);
        ProjectWithMembers project = new ProjectWithMembers("Alpha",
                Lists.newArrayList(active1, active2, inactive));
        project.save();
        Map<String, Object> frame = user.frame(project);
        Assert.assertNotNull(frame);
        Object members = frame.get("members");
        Assert.assertNotNull(members);
        Assert.assertTrue(members instanceof List);
        List<Object> memberList = (List<Object>) members;
        // NOTE: items that fail the Scope check are mapped to
        // null by frame(), and nulls are included in the
        // collected list. Filter them out before asserting.
        long visible = memberList.stream().filter(item -> item != null).count();
        Assert.assertEquals(2, visible);
    }

    /**
     * <strong>Goal:</strong> Verify that a linked {@link ScopedOwner} is
     * excluded when {@link Scope#none()} is registered for its class.
     * <p>
     * <strong>Start state:</strong> A {@link ScopedOwner} with
     * {@code status = "active"} linked from a {@link Project}. A
     * {@link Scope#none()} is registered for {@link ScopedOwner}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register {@link Scope#none()} for {@link ScopedOwner}.</li>
     * <li>Save a {@link TestUser}, an active {@link ScopedOwner}, and a
     * {@link Project} linking to that owner.</li>
     * <li>Frame the {@link Project} as the {@link TestUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code owner} field in the framed data is
     * {@code null} because {@link Scope#none()} unconditionally blocks
     * visibility.
     */
    @Test
    public void testLinkedRecordIsNullWhenScopeIsNone() {
        AccessControl.registerVisibilityScope(ScopedOwner.class,
                audience -> Scope.none());
        TestUser user = new TestUser("alice");
        user.save();
        ScopedOwner owner = new ScopedOwner("Acme", "active");
        owner.save();
        Project project = new Project("Alpha", owner);
        project.save();
        Map<String, Object> frame = user.frame(project);
        Assert.assertNotNull(frame);
        Assert.assertNull(frame.get("owner"));
    }

    /**
     * <strong>Goal:</strong> Verify that when {@link Scope#unsupported()} is
     * registered, visibility falls back to the instance-level
     * {@link AccessControl} permissions.
     * <p>
     * <strong>Start state:</strong> A {@link RestrictedOwner} linked from a
     * {@link RestrictedProject}. {@link Scope#unsupported()} is registered for
     * {@link RestrictedOwner}. The {@link RestrictedOwner RestrictedOwner's}
     * instance-level {@code $isDiscoverableBy} returns {@code false} for all
     * {@link Audience Audiences}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register {@link Scope#unsupported()} for
     * {@link RestrictedOwner}.</li>
     * <li>Save a {@link TestUser}, a {@link RestrictedOwner}, and a
     * {@link RestrictedProject} linking to that owner.</li>
     * <li>Frame the {@link RestrictedProject} as the {@link TestUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code owner} field in the framed data is
     * {@code null} because the fallback {@code $checkIfVisible()} check rejects
     * the {@link RestrictedOwner}.
     */
    @Test
    public void testFallsBackToCheckIfVisibleWhenScopeIsUnsupported() {
        AccessControl.registerVisibilityScope(RestrictedOwner.class,
                audience -> Scope.unsupported());
        TestUser user = new TestUser("alice");
        user.save();
        RestrictedOwner owner = new RestrictedOwner("secret");
        owner.save();
        RestrictedProject project = new RestrictedProject("Alpha", owner);
        project.save();
        Map<String, Object> frame = user.frame(project);
        Assert.assertNotNull(frame);
        Assert.assertNull(frame.get("owner"));
    }

    /**
     * <strong>Goal:</strong> Verify that when no {@link Scope} is registered
     * for a linked {@link Record Record's} class, visibility falls back to the
     * instance-level {@link AccessControl} permissions.
     * <p>
     * <strong>Start state:</strong> A {@link RestrictedOwner} linked from a
     * {@link RestrictedProject}. No {@link Scope} is registered for
     * {@link RestrictedOwner}. The {@link RestrictedOwner RestrictedOwner's}
     * instance-level {@code $isDiscoverableBy} returns {@code false} for all
     * {@link Audience Audiences}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TestUser}, a {@link RestrictedOwner}, and a
     * {@link RestrictedProject} linking to that owner.</li>
     * <li>Frame the {@link RestrictedProject} as the {@link TestUser}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code owner} field in the framed data is
     * {@code null} because the fallback {@code $checkIfVisible()} check rejects
     * the {@link RestrictedOwner}.
     */
    @Test
    public void testFallsBackToCheckIfVisibleWhenNoScopeRegistered() {
        TestUser user = new TestUser("alice");
        user.save();
        RestrictedOwner owner = new RestrictedOwner("secret");
        owner.save();
        RestrictedProject project = new RestrictedProject("Alpha", owner);
        project.save();
        Map<String, Object> frame = user.frame(project);
        Assert.assertNotNull(frame);
        Assert.assertNull(frame.get("owner"));
    }

    // ----------------------------------------------------------
    // Test fixtures
    // ----------------------------------------------------------

    /**
     * A minimal {@link Audience} whose identity is its name.
     */
    static class TestUser extends Record implements Audience {

        /**
         * The user's name.
         */
        public String name;

        /**
         * Construct a new {@link TestUser}.
         *
         * @param name the user's name
         */
        TestUser(String name) {
            this.name = name;
        }
    }

    /**
     * A {@link Record} that links to a single {@link ScopedOwner}. Discovery is
     * open to all authenticated {@link Audience Audiences} so the test can
     * focus on the linked record's {@link Scope} enforcement.
     */
    static class Project extends Record implements AccessControl {

        /**
         * The project name.
         */
        public String name;

        /**
         * The linked owner.
         */
        public ScopedOwner owner;

        /**
         * Construct a new {@link Project}.
         *
         * @param name the project name
         * @param owner the linked {@link ScopedOwner}
         */
        Project(String name, ScopedOwner owner) {
            this.name = name;
            this.owner = owner;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return AccessControl.NO_KEYS;
        }
    }

    /**
     * A {@link Record} that links to a collection of {@link ScopedOwner
     * ScopedOwners}.
     */
    static class ProjectWithMembers extends Record implements AccessControl {

        /**
         * The project name.
         */
        public String name;

        /**
         * The linked members.
         */
        public List<ScopedOwner> members;

        /**
         * Construct a new {@link ProjectWithMembers}.
         *
         * @param name the project name
         * @param members the linked {@link ScopedOwner ScopedOwners}
         */
        ProjectWithMembers(String name, List<ScopedOwner> members) {
            this.name = name;
            this.members = members;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return AccessControl.NO_KEYS;
        }
    }

    /**
     * A {@link Record} with a {@code status} field whose instance-level
     * permissions are intentionally permissive (all authenticated
     * {@link Audience Audiences} can discover and read). A {@link Scope} is
     * registered externally by each test to restrict visibility based on the
     * {@code status} value, creating the exact scenario where the {@link Scope}
     * is more restrictive than the instance-level checks.
     */
    static class ScopedOwner extends Record implements AccessControl {

        /**
         * The owner's name.
         */
        public String name;

        /**
         * The owner's status.
         */
        public String status;

        /**
         * Construct a new {@link ScopedOwner}.
         *
         * @param name the owner name
         * @param status the status
         */
        ScopedOwner(String name, String status) {
            this.name = name;
            this.status = status;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            // Intentionally permissive — the Scope provides
            // the real restriction
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return AccessControl.NO_KEYS;
        }
    }

    /**
     * A {@link Record} whose instance-level permissions reject all
     * {@link Audience Audiences}. Used to verify that the fallback to
     * {@link Audience#$checkIfVisible()} works correctly when no {@link Scope}
     * is registered or the {@link Scope} is {@link Scope#unsupported()}.
     */
    static class RestrictedOwner extends Record implements AccessControl {

        /**
         * The owner's name.
         */
        public String name;

        /**
         * Construct a new {@link RestrictedOwner}.
         *
         * @param name the owner name
         */
        RestrictedOwner(String name) {
            this.name = name;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return false;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return AccessControl.NO_KEYS;
        }
    }

    /**
     * A {@link Record} that links to a single {@link RestrictedOwner}.
     * Discovery is open to all authenticated {@link Audience Audiences} so the
     * test can focus on the linked record's visibility.
     */
    static class RestrictedProject extends Record implements AccessControl {

        /**
         * The project name.
         */
        public String name;

        /**
         * The linked owner.
         */
        public RestrictedOwner owner;

        /**
         * Construct a new {@link RestrictedProject}.
         *
         * @param name the project name
         * @param owner the linked {@link RestrictedOwner}
         */
        RestrictedProject(String name, RestrictedOwner owner) {
            this.name = name;
            this.owner = owner;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return AccessControl.NO_KEYS;
        }
    }

}
