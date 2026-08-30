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

    /**
     * <strong>Goal:</strong> Verify that an {@link AdHocRecord} which
     * implements {@link Audience} supplies a visibility filter that governs
     * what an {@link AdHocDataSource} load returns.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} over one public
     * and one private document.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct an admin viewer and a guest viewer.</li>
     * <li>Load the documents with each viewer's {@code $checkIfVisible()}
     * predicate as the filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The admin sees both documents; the guest sees
     * only the public one.
     */
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

        AdHocDataSource<AccessControlledAdHocRecord> db = new AdHocDataSource<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(publicDoc, privateDoc));

        // Admin should see both documents
        Predicate<AccessControlledAdHocRecord> adminFilter = adminViewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> adminResults = db
                .load(AccessControlledAdHocRecord.class, adminFilter);
        Assert.assertEquals(2, adminResults.size());

        // Guest should only see public document
        Predicate<AccessControlledAdHocRecord> guestFilter = guestViewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> guestResults = db
                .load(AccessControlledAdHocRecord.class, guestFilter);
        Assert.assertEquals(1, guestResults.size());
        Assert.assertEquals("PublicDoc", guestResults.iterator().next().title);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} visibility filter
     * composes with a {@link Criteria} in an {@link AdHocDataSource} find.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} over three
     * documents that vary in visibility and active status.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a guest viewer.</li>
     * <li>Find documents where {@code active} is {@code true} with the guest's
     * {@code $checkIfVisible()} predicate as the filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only the document that is both public and
     * active is returned.
     */
    @Test
    public void testAdHocRecordAsAudienceFilterWithCriteria() {
        AudienceAdHocRecord viewer = new AudienceAdHocRecord("Viewer", "guest");

        AccessControlledAdHocRecord doc1 = new AccessControlledAdHocRecord(
                "Alpha", true, true);
        AccessControlledAdHocRecord doc2 = new AccessControlledAdHocRecord(
                "Beta", true, false);
        AccessControlledAdHocRecord doc3 = new AccessControlledAdHocRecord(
                "Gamma", false, true);

        AdHocDataSource<AccessControlledAdHocRecord> db = new AdHocDataSource<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(doc1, doc2, doc3));

        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();

        // Viewer (guest) with criteria should only see doc1 (public and active)
        Predicate<AccessControlledAdHocRecord> filter = viewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> results = db
                .find(AccessControlledAdHocRecord.class, criteria, filter);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Alpha", results.iterator().next().title);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code $readableBy} on an access
     * controlled {@link AdHocRecord} honors the {@link Audience Audience's}
     * role.
     * <p>
     * <strong>Start state:</strong> One private document.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct an admin viewer and a guest viewer.</li>
     * <li>Call {@code $readableBy} with each viewer.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The admin can read every key; the guest can
     * read none.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code $isDiscoverableBy} on an access
     * controlled {@link AdHocRecord} honors both the document's visibility and
     * the {@link Audience Audience's} role.
     * <p>
     * <strong>Start state:</strong> One public and one private document.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct an admin viewer and a guest viewer.</li>
     * <li>Call {@code $isDiscoverableBy} on each document with each
     * viewer.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The admin discovers both documents; the guest
     * discovers only the public one.
     */
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

    /**
     * <strong>Goal:</strong> Verify that {@code frameAs} on an
     * {@link AdHocRecord} with field-level access control returns only the
     * fields the {@link Audience} may read.
     * <p>
     * <strong>Start state:</strong> One document with a title, a summary and a
     * confidential field.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct an admin viewer and a guest viewer.</li>
     * <li>Call {@code frameAs} with each viewer.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The admin's frame holds every field; the
     * guest's frame holds the title and summary but not the confidential field.
     */
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

    /**
     * <strong>Goal:</strong> Verify that the single-key {@code readAs} throws
     * for a key the {@link Audience} may not read, the same as the
     * collection-based {@code read}.
     * <p>
     * <strong>Start state:</strong> One document with a confidential field that
     * a guest may not read.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code readAs(guest, "confidential")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown.
     */
    @Test(expected = RestrictedAccessException.class)
    public void testAccessControlledAdHocRecordReadAsThrowsWhenRestricted() {
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        FieldLevelAccessAdHocRecord doc = new FieldLevelAccessAdHocRecord(
                "Document", "summary", "secret");

        doc.readAs(guest, "confidential");
    }

    /**
     * <strong>Goal:</strong> Verify that the collection-based {@code read}
     * throws when the requested keys include one the {@link Audience} may not
     * read.
     * <p>
     * <strong>Start state:</strong> One document with a confidential field that
     * a guest may not read.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code guest.read(ImmutableSet.of("confidential"), doc)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link RestrictedAccessException} is thrown.
     */
    @Test(expected = RestrictedAccessException.class)
    public void testAccessControlledAdHocRecordReadCollectionThrowsWhenRestricted() {
        AudienceAdHocRecord guest = new AudienceAdHocRecord("Guest", "guest");

        FieldLevelAccessAdHocRecord doc = new FieldLevelAccessAdHocRecord(
                "Document", "summary", "secret");

        // Using Collection-based read should throw when accessing restricted
        // key
        guest.read(ImmutableSet.of("confidential"), doc);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code $isDiscoverableByAnonymous} on
     * an access controlled {@link AdHocRecord} reflects the document's
     * visibility.
     * <p>
     * <strong>Start state:</strong> One public and one private document.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code $isDiscoverableByAnonymous} on each document.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The public document is discoverable and the
     * private one is not.
     */
    @Test
    public void testAccessControlledAdHocRecordAnonymousDiscoverability() {
        AccessControlledAdHocRecord publicDoc = new AccessControlledAdHocRecord(
                "Public", true, true);
        AccessControlledAdHocRecord privateDoc = new AccessControlledAdHocRecord(
                "Private", false, true);

        Assert.assertTrue(publicDoc.$isDiscoverableByAnonymous());
        Assert.assertFalse(privateDoc.$isDiscoverableByAnonymous());
    }

    /**
     * <strong>Goal:</strong> Verify that the anonymous {@link Audience
     * Audience's} visibility filter limits an {@link AdHocDataSource} load to
     * anonymously discoverable documents.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} over one public
     * and one private document.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the documents with the {@link Audience#anonymous() anonymous}
     * audience's {@code $checkIfVisible()} predicate as the filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only the public document is returned.
     */
    @Test
    public void testAudienceAnonymousFilter() {
        AccessControlledAdHocRecord publicDoc = new AccessControlledAdHocRecord(
                "Public", true, true);
        AccessControlledAdHocRecord privateDoc = new AccessControlledAdHocRecord(
                "Private", false, true);

        AdHocDataSource<AccessControlledAdHocRecord> db = new AdHocDataSource<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(publicDoc, privateDoc));

        // Anonymous audience should only see public documents
        Audience anonymous = Audience.anonymous();
        Predicate<AccessControlledAdHocRecord> filter = anonymous
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> results = db
                .load(AccessControlledAdHocRecord.class, filter);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Public", results.iterator().next().title);
    }

    /**
     * <strong>Goal:</strong> Verify that an {@link AdHocRecord} which
     * implements both {@link Audience} and {@link AccessControl} filters
     * records of its own type by role.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} over an admin
     * user and a viewer user.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the users with each user's {@code $checkIfVisible()} predicate
     * as the filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The admin sees both users; the viewer sees
     * only themselves.
     */
    @Test
    public void testAdHocRecordImplementsBothAudienceAndAccessControl() {
        DualRoleAdHocRecord user1 = new DualRoleAdHocRecord("Alice", "admin");
        DualRoleAdHocRecord user2 = new DualRoleAdHocRecord("Bob", "viewer");

        AdHocDataSource<DualRoleAdHocRecord> db = new AdHocDataSource<>(
                DualRoleAdHocRecord.class, () -> Arrays.asList(user1, user2));

        // user1 (admin) should see both users
        Predicate<DualRoleAdHocRecord> adminFilter = user1.$checkIfVisible();
        Set<DualRoleAdHocRecord> adminResults = db
                .load(DualRoleAdHocRecord.class, adminFilter);
        Assert.assertEquals(2, adminResults.size());

        // user2 (viewer) should only see themselves
        Predicate<DualRoleAdHocRecord> viewerFilter = user2.$checkIfVisible();
        Set<DualRoleAdHocRecord> viewerResults = db
                .load(DualRoleAdHocRecord.class, viewerFilter);
        Assert.assertEquals(1, viewerResults.size());
        Assert.assertEquals("Bob", viewerResults.iterator().next().name);
    }

    /**
     * <strong>Goal:</strong> Verify that a visibility filter applies to both a
     * plain load and a {@link Criteria} find on the same
     * {@link AdHocDataSource}.
     * <p>
     * <strong>Start state:</strong> An {@link AdHocDataSource} over three
     * documents that vary in visibility and active status.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Load the documents with a guest's {@code $checkIfVisible()} predicate
     * as the filter.</li>
     * <li>Find the documents where {@code active} is {@code true} with the same
     * filter.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The load returns the two public documents; the
     * find returns only the public document that is active.
     */
    @Test
    public void testVisibilityFilterWithFindAndCriteria() {
        AudienceAdHocRecord viewer = new AudienceAdHocRecord("Viewer", "guest");

        AccessControlledAdHocRecord doc1 = new AccessControlledAdHocRecord(
                "Public1", true, true);
        AccessControlledAdHocRecord doc2 = new AccessControlledAdHocRecord(
                "Private1", false, true);
        AccessControlledAdHocRecord doc3 = new AccessControlledAdHocRecord(
                "Public2", true, false);

        AdHocDataSource<AccessControlledAdHocRecord> db = new AdHocDataSource<>(
                AccessControlledAdHocRecord.class,
                () -> Arrays.asList(doc1, doc2, doc3));

        // Guest viewer with filter should only see public documents
        Predicate<AccessControlledAdHocRecord> filter = viewer
                .$checkIfVisible();
        Set<AccessControlledAdHocRecord> results = db
                .load(AccessControlledAdHocRecord.class, filter);
        Assert.assertEquals(2, results.size());

        // With additional criteria for active only
        Criteria activeCriteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Set<AccessControlledAdHocRecord> activeResults = db.find(
                AccessControlledAdHocRecord.class, activeCriteria, filter);
        Assert.assertEquals(1, activeResults.size());
        Assert.assertEquals("Public1", activeResults.iterator().next().title);
    }

    /**
     * <strong>Goal:</strong> Verify that {@code $checkIfVisible()} returns a
     * {@link Predicate} whose direct evaluation matches the {@link Audience
     * Audience's} access.
     * <p>
     * <strong>Start state:</strong> One public and one private document.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct an admin viewer and a guest viewer.</li>
     * <li>Test each viewer's predicate against each document.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The admin's predicate accepts both documents;
     * the guest's predicate accepts only the public one.
     */
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

    /**
     * <strong>Goal:</strong> Verify that an {@link Audience} is always visible
     * to itself, even when its access rules hide it from other non-admin
     * audiences.
     * <p>
     * <strong>Start state:</strong> One dual-role user with the viewer role.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Test the user's own {@code $checkIfVisible()} predicate against the
     * user.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The predicate accepts the user.
     */
    @Test
    public void testAdHocRecordAccessControlWithSelfDiscovery() {
        // An Audience always has access to itself
        DualRoleAdHocRecord user = new DualRoleAdHocRecord("Alice", "viewer");

        Predicate<DualRoleAdHocRecord> filter = user.$checkIfVisible();

        // User should always be able to see themselves
        Assert.assertTrue(filter.test(user));
    }

    /**
     * An {@link AdHocRecord} that implements {@link Audience}.
     *
     * @author Jeff Nelson
     */
    static class AudienceAdHocRecord extends AdHocRecord implements Audience {

        /**
         * The display name.
         */
        String name;

        /**
         * The role that determines this viewer's access.
         */
        String role;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param role the role that determines this viewer's access
         */
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
     *
     * @author Jeff Nelson
     */
    static class AccessControlledAdHocRecord extends AdHocRecord implements
            AccessControl {

        /**
         * The display title.
         */
        String title;

        /**
         * Whether non-admin audiences may access this document.
         */
        boolean isPublic;

        /**
         * Whether this document is active.
         */
        boolean active;

        /**
         * Construct a new instance.
         *
         * @param title the display title
         * @param isPublic whether non-admin audiences may access this document
         * @param active whether this document is active
         */
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

        /**
         * Return {@code true} if {@code audience} has the admin role.
         *
         * @param audience the {@link Audience} to evaluate
         * @return {@code true} if the {@code audience} is an admin
         */
        private boolean isAdmin(Audience audience) {
            if(audience instanceof AudienceAdHocRecord) {
                return "admin".equals(((AudienceAdHocRecord) audience).role);
            }
            return false;
        }
    }

    /**
     * An {@link AdHocRecord} with field-level access control.
     *
     * @author Jeff Nelson
     */
    static class FieldLevelAccessAdHocRecord extends AdHocRecord implements
            AccessControl {

        /**
         * The display title, readable by everyone.
         */
        String title;

        /**
         * The summary, readable by identified audiences.
         */
        String summary;

        /**
         * The confidential details, readable only by admins.
         */
        String confidential;

        /**
         * Construct a new instance.
         *
         * @param title the display title
         * @param summary the summary
         * @param confidential the confidential details
         */
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

        /**
         * Return {@code true} if {@code audience} has the admin role.
         *
         * @param audience the {@link Audience} to evaluate
         * @return {@code true} if the {@code audience} is an admin
         */
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
     *
     * @author Jeff Nelson
     */
    static class DualRoleAdHocRecord extends AdHocRecord implements
            Audience,
            AccessControl {

        /**
         * The display name.
         */
        String name;

        /**
         * The role that determines this user's access.
         */
        String role;

        /**
         * Construct a new instance.
         *
         * @param name the display name
         * @param role the role that determines this user's access
         */
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

        /**
         * Return {@code true} if {@code audience} has the admin role.
         *
         * @param audience the {@link Audience} to evaluate
         * @return {@code true} if the {@code audience} is an admin
         */
        private boolean isAdmin(Audience audience) {
            if(audience instanceof DualRoleAdHocRecord) {
                return "admin".equals(((DualRoleAdHocRecord) audience).role);
            }
            return false;
        }
    }

}
