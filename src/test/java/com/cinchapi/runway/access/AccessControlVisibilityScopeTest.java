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

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cinchapi.runway.Record;

/**
 * Unit tests for the static visibility-scope registry on {@link AccessControl}.
 *
 * @author Jeff Nelson
 */
public class AccessControlVisibilityScopeTest {

    /**
     * Clear the registry before and after each test to prevent cross-test
     * contamination.
     */
    @Before
    @After
    public void clearRegistry() {
        AccessControlSupport.VISIBILITY_SCOPES.clear();
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link AccessControl#resolveVisibilityScope(Class, Audience)} returns
     * {@code null} when no scope has been registered for the class.
     * <p>
     * <strong>Start state:</strong> Registry is empty.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code resolveVisibilityScope} with a class that has no
     * registration.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Return value is {@code null}.
     */
    @Test
    public void testResolveReturnsNullWhenNoScopeRegistered() {
        Scope result = AccessControl.resolveVisibilityScope(TestResource.class,
                new TestAudience());
        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that a registered scope provider is invoked
     * and its return value is passed back to the caller.
     * <p>
     * <strong>Start state:</strong> Registry has one entry for
     * {@link TestResource}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a provider that always returns
     * {@link Scope#unrestricted()}.</li>
     * <li>Call {@code resolveVisibilityScope}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Scope} is
     * {@code Scope.unrestricted()}.
     */
    @Test
    public void testResolveReturnsRegisteredScope() {
        AccessControl.registerVisibilityScope(TestResource.class,
                audience -> Scope.unrestricted());
        Scope result = AccessControl.resolveVisibilityScope(TestResource.class,
                new TestAudience());
        Assert.assertSame(Scope.unrestricted(), result);
    }

    /**
     * <strong>Goal:</strong> Verify that the {@link Audience} instance is
     * forwarded to the registered provider.
     * <p>
     * <strong>Start state:</strong> Registry has one entry for
     * {@link TestResource}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a provider that captures the {@link Audience} it
     * receives.</li>
     * <li>Resolve with a specific {@link Audience} instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The captured audience is the same instance
     * that was passed to {@code resolveVisibilityScope}.
     */
    @Test
    public void testResolvePassesAudienceToProvider() {
        AtomicReference<Audience> captured = new AtomicReference<>();
        AccessControl.registerVisibilityScope(TestResource.class, audience -> {
            captured.set(audience);
            return Scope.unrestricted();
        });
        TestAudience audience = new TestAudience();
        AccessControl.resolveVisibilityScope(TestResource.class, audience);
        Assert.assertSame(audience, captured.get());
    }

    /**
     * <strong>Goal:</strong> Verify that a provider can return different
     * {@link Scope} instances based on the type of {@link Audience}.
     * <p>
     * <strong>Start state:</strong> Registry has one entry for
     * {@link TestResource} with type-dispatching logic.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a provider that returns {@link Scope#unrestricted()} for
     * {@link AdminAudience} and {@link Scope#none()} for
     * {@link TestAudience}.</li>
     * <li>Resolve with each type in turn.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Admin receives {@code unrestricted}; regular
     * audience receives {@code none}.
     */
    @Test
    public void testProviderCanReturnDifferentScopesPerAudienceType() {
        AccessControl.registerVisibilityScope(TestResource.class, audience -> {
            if(audience instanceof AdminAudience) {
                return Scope.unrestricted();
            }
            else {
                return Scope.none();
            }
        });
        Scope adminScope = AccessControl.resolveVisibilityScope(
                TestResource.class, new AdminAudience());
        Scope regularScope = AccessControl
                .resolveVisibilityScope(TestResource.class, new TestAudience());
        Assert.assertSame(Scope.unrestricted(), adminScope);
        Assert.assertSame(Scope.none(), regularScope);
    }

    /**
     * <strong>Goal:</strong> Verify that resolving a class with no exact
     * registration returns {@code null} even if a parent class is registered.
     * <p>
     * <strong>Start state:</strong> Registry has a registration for
     * {@link TestResource} but not for {@link SubResource}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a provider for {@link TestResource}.</li>
     * <li>Resolve with {@link SubResource}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code null} is returned; no superclass walk
     * is performed.
     */
    @Test
    public void testResolveReturnsNullWhenOnlyParentIsRegistered() {
        AccessControl.registerVisibilityScope(TestResource.class,
                audience -> Scope.unrestricted());
        Scope result = AccessControl.resolveVisibilityScope(SubResource.class,
                new TestAudience());
        Assert.assertNull(result);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link AccessControl#registerVisibilityScopeHierarchy(Class, Function)}
     * registers the provider for the root class and all known subclasses.
     * <p>
     * <strong>Start state:</strong> Registry is empty.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code registerVisibilityScopeHierarchy} with
     * {@link TestResource} and a provider returning
     * {@link Scope#unrestricted()}.</li>
     * <li>Resolve with both {@link TestResource} and {@link SubResource}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both classes resolve to
     * {@code Scope.unrestricted()}.
     */
    @Test
    public void testRegisterHierarchyCoversSubclasses() {
        AccessControl.registerVisibilityScopeHierarchy(TestResource.class,
                audience -> Scope.unrestricted());
        Scope rootScope = AccessControl
                .resolveVisibilityScope(TestResource.class, new TestAudience());
        Scope subScope = AccessControl.resolveVisibilityScope(SubResource.class,
                new TestAudience());
        Assert.assertSame(Scope.unrestricted(), rootScope);
        Assert.assertSame(Scope.unrestricted(), subScope);
    }

    /**
     * <strong>Goal:</strong> Verify that
     * {@link AccessControl#registerVisibilityScopeHierarchy(Class, Function)}
     * does not overwrite a subclass registration that was made explicitly
     * before the hierarchy registration.
     * <p>
     * <strong>Start state:</strong> Registry has an explicit entry for
     * {@link SubResource}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register {@link Scope#none()} explicitly for
     * {@link SubResource}.</li>
     * <li>Call {@code registerVisibilityScopeHierarchy} for
     * {@link TestResource} with {@link Scope#unrestricted()}.</li>
     * <li>Resolve both classes.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@link TestResource} resolves to
     * {@code unrestricted}; {@link SubResource} retains {@code none}.
     */
    @Test
    public void testHierarchyRegistrationDoesNotOverwriteExplicitSubclass() {
        AccessControl.registerVisibilityScope(SubResource.class,
                audience -> Scope.none());
        AccessControl.registerVisibilityScopeHierarchy(TestResource.class,
                audience -> Scope.unrestricted());
        Scope rootScope = AccessControl
                .resolveVisibilityScope(TestResource.class, new TestAudience());
        Scope subScope = AccessControl.resolveVisibilityScope(SubResource.class,
                new TestAudience());
        Assert.assertSame(Scope.unrestricted(), rootScope);
        Assert.assertSame(Scope.none(), subScope);
    }

    // -------------------------------------------------------------------------
    // Test domain model
    // -------------------------------------------------------------------------

    /**
     * A minimal {@link AccessControl} record used as the resolution target.
     */
    static class TestResource extends Record implements AccessControl {

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
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return AccessControl.ALL_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return AccessControl.ALL_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return AccessControl.ALL_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return AccessControl.ALL_KEYS;
        }

    }

    /**
     * A subclass of {@link TestResource} used to test superclass fallback.
     */
    static class SubResource extends TestResource {}

    /**
     * A minimal {@link Audience} for testing. All database-interface operations
     * use inherited defaults from {@link Audience}.
     */
    static class TestAudience extends Record implements Audience {}

    /**
     * An admin variant of {@link TestAudience} for type-dispatch tests.
     */
    static class AdminAudience extends TestAudience {}

}
