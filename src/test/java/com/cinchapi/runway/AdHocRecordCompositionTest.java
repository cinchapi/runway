/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.access.AccessControl;
import com.cinchapi.runway.access.Audience;
import com.cinchapi.runway.access.RestrictedAccessException;
import com.google.common.collect.ImmutableSet;

/**
 * Unit tests verifying that {@link AdHocRecord AdHocRecords} can properly
 * compose with framework interfaces such as {@link Audience} and
 * {@link AccessControl}.
 *
 * @author Jeff Nelson
 */
public class AdHocRecordCompositionTest {

    // ========================================================================
    // Audience Tests
    // ========================================================================

    @Test
    public void testAdHocRecordAsAudienceVisibilityFilter() {
        AudienceAdHocRecord adminViewer = new AudienceAdHocRecord("Admin",
                "admin");
        AudienceAdHocRecord guestViewer = new AudienceAdHocRecord("Guest",
                "guest");

        AccessControlledAdHocRecord publicDoc = new AccessControlledAdHocRecord(
                "PublicDoc", true, true);
        AccessControlledAdHocRecord privateDoc = new AccessControlledAdHocRecord(
                "PrivateDoc", false, true);

        AdHocDatabase<AccessControlledAdHocRecord> db = new AdHocDatabase<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(publicDoc, privateDoc));

        // Admin should see both documents
        Predicate<AccessControlledAdHocRecord> adminFilter = adminViewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> adminResults = db.load(
                AccessControlledAdHocRecord.class, adminFilter);
        Assert.assertEquals(2, adminResults.size());

        // Guest should only see public document
        Predicate<AccessControlledAdHocRecord> guestFilter = guestViewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> guestResults = db.load(
                AccessControlledAdHocRecord.class, guestFilter);
        Assert.assertEquals(1, guestResults.size());
        Assert.assertEquals("PublicDoc",
                guestResults.iterator().next().title);
    }

    @Test
    public void testAdHocRecordAsAudienceFilterWithCriteria() {
        AudienceAdHocRecord viewer = new AudienceAdHocRecord("Viewer", "guest");

        AccessControlledAdHocRecord doc1 = new AccessControlledAdHocRecord(
                "Alpha", true, true);
        AccessControlledAdHocRecord doc2 = new AccessControlledAdHocRecord(
                "Beta", true, false);
        AccessControlledAdHocRecord doc3 = new AccessControlledAdHocRecord(
                "Gamma", false, true);

        AdHocDatabase<AccessControlledAdHocRecord> db = new AdHocDatabase<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(doc1, doc2, doc3));

        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();

        // Viewer (guest) with criteria should only see doc1 (public and active)
        Predicate<AccessControlledAdHocRecord> filter = viewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> results = db.find(
                AccessControlledAdHocRecord.class, criteria, filter);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Alpha", results.iterator().next().title);
    }

    // ========================================================================
    // AccessControl Tests
    // ========================================================================

    @Test
    public void testAccessControlledAdHocRecordReadableBy() {
        AudienceAdHocRecord admin = new AudienceAdHocRecord("Admin", "admin");
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        AccessControlledAdHocRecord doc = new AccessControlledAdHocRecord(
                "Secret", false, true);

        // Admin should be able to read all keys
        Set<String> adminReadable = doc.$readableBy(admin);
        Assert.assertEquals(AccessControl.ALL_KEYS, adminReadable);

        // Guest should not be able to read any keys for private doc
        Set<String> guestReadable = doc.$readableBy(guest);
        Assert.assertEquals(AccessControl.NO_KEYS, guestReadable);
    }

    @Test
    public void testAccessControlledAdHocRecordDiscoverability() {
        AudienceAdHocRecord admin = new AudienceAdHocRecord("Admin", "admin");
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        AccessControlledAdHocRecord publicDoc = new AccessControlledAdHocRecord(
                "Public", true, true);
        AccessControlledAdHocRecord privateDoc = new AccessControlledAdHocRecord(
                "Private", false, true);

        // Admin discovers all
        Assert.assertTrue(publicDoc.$isDiscoverableBy(admin));
        Assert.assertTrue(privateDoc.$isDiscoverableBy(admin));

        // Guest only discovers public
        Assert.assertTrue(publicDoc.$isDiscoverableBy(guest));
        Assert.assertFalse(privateDoc.$isDiscoverableBy(guest));
    }

    @Test
    public void testAccessControlledAdHocRecordFrameAs() {
        AudienceAdHocRecord admin = new AudienceAdHocRecord("Admin", "admin");
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        FieldLevelAccessAdHocRecord doc = new FieldLevelAccessAdHocRecord(
                "Document", "public summary", "confidential details");

        // Admin should see all fields
        Map<String, Object> adminFrame = doc.frameAs(admin);
        Assert.assertNotNull(adminFrame);
        Assert.assertTrue(adminFrame.containsKey("title"));
        Assert.assertTrue(adminFrame.containsKey("summary"));
        Assert.assertTrue(adminFrame.containsKey("confidential"));

        // Guest should only see title and summary
        Map<String, Object> guestFrame = doc.frameAs(guest);
        Assert.assertNotNull(guestFrame);
        Assert.assertTrue(guestFrame.containsKey("title"));
        Assert.assertTrue(guestFrame.containsKey("summary"));
        Assert.assertFalse(guestFrame.containsKey("confidential"));
    }

    @Test
    public void testAccessControlledAdHocRecordReadAsReturnsNullWhenRestricted() {
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        FieldLevelAccessAdHocRecord doc = new FieldLevelAccessAdHocRecord(
                "Document", "summary", "secret");

        // Guest trying to read confidential field should get null
        // (the single-key readAs filters data, the Collection-based read throws)
        Object result = doc.readAs(guest, "confidential");
        Assert.assertNull(result);
    }

    @Test(expected = RestrictedAccessException.class)
    public void testAccessControlledAdHocRecordReadCollectionThrowsWhenRestricted() {
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        FieldLevelAccessAdHocRecord doc = new FieldLevelAccessAdHocRecord(
                "Document", "summary", "secret");

        // Using Collection-based read should throw when accessing restricted key
        guest.read(ImmutableSet.of("confidential"), doc);
    }

    @Test
    public void testAccessControlledAdHocRecordAnonymousDiscoverability() {
        AccessControlledAdHocRecord publicDoc = new AccessControlledAdHocRecord(
                "Public", true, true);
        AccessControlledAdHocRecord privateDoc = new AccessControlledAdHocRecord(
                "Private", false, true);

        Assert.assertTrue(publicDoc.$isDiscoverableByAnonymous());
        Assert.assertFalse(privateDoc.$isDiscoverableByAnonymous());
    }

    @Test
    public void testAudienceAnonymousFilter() {
        AccessControlledAdHocRecord publicDoc = new AccessControlledAdHocRecord(
                "Public", true, true);
        AccessControlledAdHocRecord privateDoc = new AccessControlledAdHocRecord(
                "Private", false, true);

        AdHocDatabase<AccessControlledAdHocRecord> db = new AdHocDatabase<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(publicDoc, privateDoc));

        // Anonymous audience should only see public documents
        Audience anonymous = Audience.anonymous();
        Predicate<AccessControlledAdHocRecord> filter = anonymous
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> results = db.load(
                AccessControlledAdHocRecord.class, filter);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Public", results.iterator().next().title);
    }

    // ========================================================================
    // Combined Audience + AccessControl Tests
    // ========================================================================

    @Test
    public void testAdHocRecordImplementsBothAudienceAndAccessControl() {
        DualRoleAdHocRecord user1 = new DualRoleAdHocRecord("Alice", "admin");
        DualRoleAdHocRecord user2 = new DualRoleAdHocRecord("Bob", "viewer");

        AdHocDatabase<DualRoleAdHocRecord> db = new AdHocDatabase<>(
                DualRoleAdHocRecord.class, () -> Arrays.asList(user1, user2));

        // user1 (admin) should see both users
        Predicate<DualRoleAdHocRecord> adminFilter = user1.$checkIfVisible();
        Set<DualRoleAdHocRecord> adminResults = db.load(
                DualRoleAdHocRecord.class, adminFilter);
        Assert.assertEquals(2, adminResults.size());

        // user2 (viewer) should only see themselves
        Predicate<DualRoleAdHocRecord> viewerFilter = user2.$checkIfVisible();
        Set<DualRoleAdHocRecord> viewerResults = db.load(
                DualRoleAdHocRecord.class, viewerFilter);
        Assert.assertEquals(1, viewerResults.size());
        Assert.assertEquals("Bob", viewerResults.iterator().next().name);
    }

    @Test
    public void testVisibilityFilterWithFindAndCriteria() {
        AudienceAdHocRecord viewer = new AudienceAdHocRecord("Viewer", "guest");

        AccessControlledAdHocRecord doc1 = new AccessControlledAdHocRecord(
                "Public1", true, true);
        AccessControlledAdHocRecord doc2 = new AccessControlledAdHocRecord(
                "Private1", false, true);
        AccessControlledAdHocRecord doc3 = new AccessControlledAdHocRecord(
                "Public2", true, false);

        AdHocDatabase<AccessControlledAdHocRecord> db = new AdHocDatabase<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(doc1, doc2, doc3));

        // Guest viewer with filter should only see public documents
        Predicate<AccessControlledAdHocRecord> filter = viewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> results = db.load(
                AccessControlledAdHocRecord.class, filter);
        Assert.assertEquals(2, results.size());

        // With additional criteria for active only
        Criteria activeCriteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Set<AccessControlledAdHocRecord> activeResults = db.find(
                AccessControlledAdHocRecord.class, activeCriteria, filter);
        Assert.assertEquals(1, activeResults.size());
        Assert.assertEquals("Public1", activeResults.iterator().next().title);
    }

    @Test
    public void testAdHocRecordCheckIfVisibleReturnsPredicateThatFilters() {
        AudienceAdHocRecord admin = new AudienceAdHocRecord("Admin", "admin");
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        AccessControlledAdHocRecord publicDoc = new AccessControlledAdHocRecord(
                "Public", true, true);
        AccessControlledAdHocRecord privateDoc = new AccessControlledAdHocRecord(
                "Private", false, true);

        // Verify predicate behavior directly
        Predicate<AccessControlledAdHocRecord> adminPredicate = admin
                .$checkIfVisible();
        Predicate<AccessControlledAdHocRecord> guestPredicate = guest
                .$checkIfVisible();

        Assert.assertTrue(adminPredicate.test(publicDoc));
        Assert.assertTrue(adminPredicate.test(privateDoc));
        Assert.assertTrue(guestPredicate.test(publicDoc));
        Assert.assertFalse(guestPredicate.test(privateDoc));
    }

    @Test
    public void testAdHocRecordAccessControlWithSelfDiscovery() {
        // An Audience always has access to itself
        DualRoleAdHocRecord user = new DualRoleAdHocRecord("Alice", "viewer");

        Predicate<DualRoleAdHocRecord> filter = user.$checkIfVisible();

        // User should always be able to see themselves
        Assert.assertTrue(filter.test(user));
    }

    // ========================================================================
    // Test Record Classes
    // ========================================================================

    /**
     * An {@link AdHocRecord} that implements {@link Audience}.
     */
    static class AudienceAdHocRecord extends AdHocRecord implements Audience {

        String name;
        String role;

        AudienceAdHocRecord(String name, String role) {
            this.name = name;
            this.role = role;
        }

        @Override
        public DatabaseInterface $db() {
            // Return null since we don't have a real DB connection
            // This is OK for testing $checkIfVisible() which doesn't need $db()
            return null;
        }
    }

    /**
     * An {@link AdHocRecord} that implements {@link AccessControl}.
     */
    static class AccessControlledAdHocRecord extends AdHocRecord
            implements AccessControl {

        String title;
        boolean isPublic;
        boolean active;

        AccessControlledAdHocRecord(String title, boolean isPublic,
                boolean active) {
            this.title = title;
            this.isPublic = isPublic;
            this.active = active;
        }

        @Override
        public boolean $isCreatableBy(@Nonnull Audience audience) {
            return isAdmin(audience);
        }

        @Override
        public boolean $isCreatableByAnonymous() {
            return false;
        }

        @Override
        public boolean $isDeletableBy(@Nonnull Audience audience) {
            return isAdmin(audience);
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return isPublic || isAdmin(audience);
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return isPublic;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if(isAdmin(audience) || isPublic) {
                return ALL_KEYS;
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return isPublic ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return isAdmin(audience) ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        private boolean isAdmin(Audience audience) {
            if(audience instanceof AudienceAdHocRecord) {
                return "admin".equals(((AudienceAdHocRecord) audience).role);
            }
            return false;
        }
    }

    /**
     * An {@link AdHocRecord} with field-level access control.
     */
    static class FieldLevelAccessAdHocRecord extends AdHocRecord
            implements AccessControl {

        String title;
        String summary;
        String confidential;

        FieldLevelAccessAdHocRecord(String title, String summary,
                String confidential) {
            this.title = title;
            this.summary = summary;
            this.confidential = confidential;
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
            return isAdmin(audience);
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return true;
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return true;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            if(isAdmin(audience)) {
                return ALL_KEYS;
            }
            // Non-admins can only see title and summary
            return ImmutableSet.of("title", "summary");
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return ImmutableSet.of("title");
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return isAdmin(audience) ? ALL_KEYS : NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        private boolean isAdmin(Audience audience) {
            if(audience instanceof AudienceAdHocRecord) {
                return "admin".equals(((AudienceAdHocRecord) audience).role);
            }
            return false;
        }
    }

    /**
     * An {@link AdHocRecord} that implements both {@link Audience} and
     * {@link AccessControl}.
     */
    static class DualRoleAdHocRecord extends AdHocRecord
            implements Audience, AccessControl {

        String name;
        String role;

        DualRoleAdHocRecord(String name, String role) {
            this.name = name;
            this.role = role;
        }

        @Override
        public DatabaseInterface $db() {
            return null;
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
            return audience.equals(this) || isAdmin(audience);
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            // Admins can see everyone, others can only see themselves
            return isAdmin(audience) || audience.equals(this);
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            // Only admins and self can read; others have no access
            // This ensures $checkIfVisible() only passes for discoverable users
            if(isAdmin(audience) || audience.equals(this)) {
                return ALL_KEYS;
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            if(isAdmin(audience) || audience.equals(this)) {
                return ALL_KEYS;
            }
            return NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return NO_KEYS;
        }

        private boolean isAdmin(Audience audience) {
            if(audience instanceof DualRoleAdHocRecord) {
                return "admin".equals(((DualRoleAdHocRecord) audience).role);
            }
            return false;
        }
    }

}
