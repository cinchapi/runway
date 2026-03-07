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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.Timestamp;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Record.Revision;
import com.cinchapi.runway.RunwayBaseClientServerTest;

/**
 * Unit tests for automatic author attribution through the {@link Audience}
 * framework
 *
 * @author Jeff Nelson
 */
public class AudienceAuthorTrackingTest extends RunwayBaseClientServerTest {

    @Test
    public void testAuthorIsTracked() {
        User user = new User();
        user.name = "Jeff Nelson";
        user.age = 37;
        Document document = user.create(Document.class);
        user.write("text", "This is written by Jeff Nelson", document);
        runway.save(user, document);
        System.out.println(client.select(document.id()));
        document.set("text", "This is anonymous text");
        document.save();
        System.out.println(client.select(document.id()));
        System.out.println(client.review(document.id()));
        System.out.println(document.audit());
        Assert.assertFalse(document.audit().isEmpty());
    }

    @Test
    public void testAuthorAttributionWhenAudienceMakesChanges() {
        User user = new User();
        user.name = "Alice";
        user.email = "alice@example.com";
        user.roles = Arrays.asList("editor", "reviewer");
        user.active = true;

        Document document = user.create(Document.class);
        user.write("text", "Initial content", document);
        user.write("title", "My Document", document);
        user.write("version", 1, document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertFalse(audit.isEmpty());

        // Verify the first save has author attribution
        Timestamp firstSave = audit.keySet().iterator().next();
        Map<String, Revision> revisions = audit.get(firstSave);
        Assert.assertTrue(revisions.containsKey("text"));
        Assert.assertTrue(revisions.containsKey("title"));
        Assert.assertTrue(revisions.containsKey("version"));

        Revision textRevision = revisions.get("text");
        Assert.assertTrue(textRevision.isAttributed());
        Assert.assertEquals(user, textRevision.author());
        Assert.assertNull(textRevision.from()); // First save, no previous value
        Assert.assertEquals("Initial content", textRevision.to());

        Revision titleRevision = revisions.get("title");
        Assert.assertTrue(titleRevision.isAttributed());
        Assert.assertEquals(user, titleRevision.author());
        Assert.assertEquals("My Document", titleRevision.to());

        Revision versionRevision = revisions.get("version");
        Assert.assertTrue(versionRevision.isAttributed());
        Assert.assertEquals(user, versionRevision.author());
        Assert.assertEquals(1, versionRevision.to());
    }

    @Test
    public void testAuthorAttributionForMultipleSequentialChanges() {
        User user = new User();
        user.name = "Bob";
        user.email = "bob@example.com";
        user.roles = Arrays.asList("author");
        user.active = true;

        Document document = user.create(Document.class);

        // First change - create document
        user.write("text", "First version", document);
        user.write("title", "Draft Document", document);
        user.write("version", 1, document);
        runway.save(user, document);

        // Second change - update content
        user.write("text", "Second version", document);
        user.write("version", 2, document);
        user.write("published", true, document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertEquals(2, audit.size());

        // Verify both changes are attributed to the same user
        for (Map<String, Revision> revisions : audit.values()) {
            // Check text field in both revisions
            if(revisions.containsKey("text")) {
                Revision revision = revisions.get("text");
                Assert.assertTrue(revision.isAttributed());
                Assert.assertEquals(user, revision.author());
            }

            // Check version field in both revisions
            if(revisions.containsKey("version")) {
                Revision revision = revisions.get("version");
                Assert.assertTrue(revision.isAttributed());
                Assert.assertEquals(user, revision.author());
            }
        }
    }

    @Test
    public void testAuthorAttributionWithDifferentAudiences() {
        User user1 = new User();
        user1.name = "Alice";
        user1.email = "alice@example.com";
        user1.roles = Arrays.asList("editor");
        user1.active = true;

        User user2 = new User();
        user2.name = "Bob";
        user2.email = "bob@example.com";
        user2.roles = Arrays.asList("reviewer");
        user2.active = true;

        Document document = user1.create(Document.class);
        user1.write("text", "Alice's content", document);
        user1.write("title", "Alice's Document", document);
        user1.write("version", 1, document);
        runway.save(user1, document);

        // Create a new document instance for user2 to avoid concurrent access
        Document document2 = runway.load(Document.class, document.id());
        user2.write("text", "Bob's content", document2);
        user2.write("title", "Bob's Document", document2);
        user2.write("version", 2, document2);
        user2.write("published", true, document2);
        runway.save(user2, document2);

        // Load fresh instance to get complete audit
        Document finalDoc = runway.load(Document.class, document.id());
        Map<Timestamp, Map<String, Revision>> audit = finalDoc.audit();
        Assert.assertEquals(2, audit.size());

        // Verify different authors for different saves
        List<Revision> revisions = audit.values().stream()
                .map(r -> r.get("text")).collect(Collectors.toList());
        Assert.assertEquals(2, revisions.size());
        Assert.assertEquals(user1, revisions.get(0).author());
        Assert.assertEquals(user2, revisions.get(1).author());
    }

    @Test
    public void testAuthorAttributionClearedAfterSave() {
        User user = new User();
        user.name = "Charlie";
        Document document = user.create(Document.class);
        user.write("text", "Initial content", document);
        runway.save(user, document);

        // Verify author was attributed in audit
        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Revision firstRevision = audit.values().iterator().next().get("text");
        Assert.assertTrue(firstRevision.isAttributed());

        // Make another change without setting author explicitly
        document.set("text", "Modified content");
        document.save();

        // Verify the second change has no author attribution
        audit = document.audit();
        List<Revision> revisions = audit.values().stream()
                .map(r -> r.get("text")).collect(Collectors.toList());
        Assert.assertFalse(revisions.get(1).isAttributed());
    }

    @Test
    public void testNoAuthorAttributionWhenNoAudienceSet() {
        Document document = new Document();
        document.text = "Anonymous content";
        runway.save(document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertFalse(audit.isEmpty());

        Revision revision = audit.values().iterator().next().get("text");
        Assert.assertFalse(revision.isAttributed());
        Assert.assertNull(revision.author());
    }

    @Test
    public void testAuditTrailCapturesAllFieldChanges() {
        User user = new User();
        user.name = "David";
        Document document = user.create(Document.class);
        user.write("text", "Initial text", document);
        user.write("title", "Initial title", document);
        runway.save(user, document);

        // Modify multiple fields
        user.write("text", "Updated text", document);
        user.write("title", "Updated title", document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertEquals(2, audit.size());

        // Verify all field changes are captured
        List<Map<String, Revision>> allRevisions = new ArrayList<>(
                audit.values());
        Assert.assertTrue(allRevisions.get(0).containsKey("text"));
        Assert.assertTrue(allRevisions.get(0).containsKey("title"));
        Assert.assertTrue(allRevisions.get(1).containsKey("text"));
        Assert.assertTrue(allRevisions.get(1).containsKey("title"));
    }

    @Test
    public void testAuditTrailWithPositiveKeyFiltering() {
        User user = new User();
        user.name = "Eve";
        Document document = user.create(Document.class);
        user.write("text", "Content", document);
        user.write("title", "Title", document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit("text");
        Assert.assertFalse(audit.isEmpty());

        // Verify only 'text' field is included
        Map<String, Revision> revisions = audit.values().iterator().next();
        Assert.assertTrue(revisions.containsKey("text"));
        Assert.assertFalse(revisions.containsKey("title"));
    }

    @Test
    public void testAuditTrailWithNegativeKeyFiltering() {
        User user = new User();
        user.name = "Frank";
        Document document = user.create(Document.class);
        user.write("text", "Content", document);
        user.write("title", "Title", document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit("-title");
        Assert.assertFalse(audit.isEmpty());

        // Verify 'title' field is excluded
        Map<String, Revision> revisions = audit.values().iterator().next();
        Assert.assertTrue(revisions.containsKey("text"));
        Assert.assertFalse(revisions.containsKey("title"));
    }

    @Test
    public void testAuditTrailWithMixedKeyFiltering() {
        User user = new User();
        user.name = "Grace";
        Document document = user.create(Document.class);
        user.write("text", "Content", document);
        user.write("title", "Title", document);
        user.write("description", "Description", document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit("text",
                "-title");
        Assert.assertFalse(audit.isEmpty());

        // Verify 'text' is included, 'title' is excluded, 'description' is
        // excluded
        Map<String, Revision> revisions = audit.values().iterator().next();
        Assert.assertTrue(revisions.containsKey("text"));
        Assert.assertFalse(revisions.containsKey("title"));
        Assert.assertFalse(revisions.containsKey("description"));
    }

    @Test
    public void testAuditTrailWithNoChanges() {
        Document document = new Document();
        document.text = "Static content";
        runway.save(document);

        // No additional changes
        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertEquals(1, audit.size()); // Only the initial save

        // Verify the initial save is captured
        Map<String, Revision> revisions = audit.values().iterator().next();
        Assert.assertTrue(revisions.containsKey("text"));
    }

    @Test
    public void testAuditTrailWithDeletedFields() {
        User user = new User();
        user.name = "Henry";
        Document document = user.create(Document.class);
        user.write("text", "Content", document);
        user.write("title", "Title", document);
        runway.save(user, document);

        // Clear one field
        user.write("title", null, document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertEquals(2, audit.size());

        // Verify the deletion is captured
        List<Revision> titleRevisions = audit.values().stream()
                .filter(r -> r.containsKey("title")).map(r -> r.get("title"))
                .collect(Collectors.toList());
        Assert.assertEquals(2, titleRevisions.size());

        // Second revision should show field being cleared
        Revision deletionRevision = titleRevisions.get(1);
        Assert.assertEquals("Title", deletionRevision.from());
        Assert.assertNull(deletionRevision.to());
    }

    @Test
    public void testAuditTrailWithInvalidKeys() {
        User user = new User();
        user.name = "Ivy";
        Document document = user.create(Document.class);
        user.write("text", "Content", document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document
                .audit("nonexistent");
        Assert.assertFalse(audit.isEmpty());

        // Verify no revisions for non-existent field
        Map<String, Revision> revisions = audit.values().iterator().next();
        Assert.assertFalse(revisions.containsKey("nonexistent"));
    }

    @Test
    public void testAuditTrailWithEmptyKeyArray() {
        User user = new User();
        user.name = "Jack";
        Document document = user.create(Document.class);
        user.write("text", "Content", document);
        user.write("title", "Title", document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertFalse(audit.isEmpty());

        // Verify all non-internal fields are included
        Map<String, Revision> revisions = audit.values().iterator().next();
        Assert.assertTrue(revisions.containsKey("text"));
        Assert.assertTrue(revisions.containsKey("title"));
        Assert.assertFalse(revisions.containsKey("_author")); // Internal field
                                                              // excluded
    }

    @Test
    public void testAuditTrailWithCollectionFieldChanges() {
        User user = new User();
        user.name = "Kate";
        Document document = user.create(Document.class);
        user.write("tags", Arrays.asList("tag1", "tag2"), document);
        runway.save(user, document);

        // Modify collection
        user.write("tags", Arrays.asList("tag1", "tag3"), document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertEquals(2, audit.size());

        // Verify collection changes are captured
        List<Revision> tagRevisions = audit.values().stream()
                .filter(r -> r.containsKey("tags")).map(r -> r.get("tags"))
                .collect(Collectors.toList());
        Assert.assertEquals(2, tagRevisions.size());
    }

    @Test
    public void testAuditTrailWithRealmChanges() {
        User user = new User();
        user.name = "Liam";
        Document document = user.create(Document.class);
        user.write("text", "Content", document);
        runway.save(user, document);

        // Add realm
        document.addRealm("test-realm");
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertEquals(2, audit.size());

        // Verify realm changes are captured but excluded from normal filtering
        Map<Timestamp, Map<String, Revision>> filteredAudit = document
                .audit("-_realms");
        Assert.assertEquals(2, filteredAudit.size());

        // Realm changes should not appear in normal field filtering
        Map<String, Revision> revisions = filteredAudit.values().iterator()
                .next();
        Assert.assertFalse(revisions.containsKey("_realms"));
    }

    @Test
    public void testAuditTrailWithComplexRecordTypes() {
        User user = new User();
        user.name = "Charlie";
        user.email = "charlie@example.com";
        user.roles = Arrays.asList("admin", "manager");
        user.active = true;

        Project project = user.create(Project.class);
        user.write("name", "My Project", project);
        user.write("description", "A complex project", project);
        user.write("status", "active", project);
        user.write("members", Arrays.asList("Alice", "Bob", "Charlie"),
                project);
        user.write("budget", 100000.0, project);
        user.write("startDate", Timestamp.now(), project);
        runway.save(user, project);

        Map<Timestamp, Map<String, Revision>> audit = project.audit();
        Assert.assertFalse(audit.isEmpty());

        Map<String, Revision> revisions = audit.values().iterator().next();
        Assert.assertTrue(revisions.containsKey("name"));
        Assert.assertTrue(revisions.containsKey("description"));
        Assert.assertTrue(revisions.containsKey("status"));
        Assert.assertTrue(revisions.containsKey("members"));
        Assert.assertTrue(revisions.containsKey("budget"));
        Assert.assertTrue(revisions.containsKey("startDate"));

        // Verify complex field types are handled correctly
        Revision membersRevision = revisions.get("members");
        Assert.assertTrue(membersRevision.isAttributed());
        Assert.assertEquals(user, membersRevision.author());
        Assert.assertTrue(membersRevision.to() instanceof Collection);

        Revision budgetRevision = revisions.get("budget");
        Assert.assertTrue(budgetRevision.isAttributed());
        Assert.assertEquals(user, budgetRevision.author());
        Assert.assertEquals(100000.0, budgetRevision.to());
    }

    @Test
    public void testAuditTrailWithBooleanAndNumericFields() {
        User user = new User();
        user.name = "Diana";
        user.email = "diana@example.com";
        user.roles = Arrays.asList("tester");
        user.active = true;

        Comment comment = user.create(Comment.class);
        user.write("content", "Great work!", comment);
        user.write("author", "Diana", comment);
        user.write("rating", 5, comment);
        user.write("approved", true, comment);
        user.write("mentions", Arrays.asList("@alice", "@bob"), comment);
        runway.save(user, comment);

        Map<Timestamp, Map<String, Revision>> audit = comment.audit();
        Assert.assertFalse(audit.isEmpty());

        Map<String, Revision> revisions = audit.values().iterator().next();

        // Test boolean field
        Revision approvedRevision = revisions.get("approved");
        Assert.assertTrue(approvedRevision.isAttributed());
        Assert.assertEquals(user, approvedRevision.author());
        Assert.assertEquals(true, approvedRevision.to());

        // Test numeric field
        Revision ratingRevision = revisions.get("rating");
        Assert.assertTrue(ratingRevision.isAttributed());
        Assert.assertEquals(user, ratingRevision.author());
        Assert.assertEquals(5, ratingRevision.to());

        // Test string field
        Revision contentRevision = revisions.get("content");
        Assert.assertTrue(contentRevision.isAttributed());
        Assert.assertEquals(user, contentRevision.author());
        Assert.assertEquals("Great work!", contentRevision.to());
    }

    @Test
    public void testAuditTrailWithMixedDataTypes() {
        User user = new User();
        user.name = "Eve";
        user.email = "eve@example.com";
        user.roles = Arrays.asList("developer");
        user.active = true;

        Document document = user.create(Document.class);
        user.write("text", "Sample text", document);
        user.write("title", "Sample Title", document);
        user.write("version", 1, document);
        user.write("published", false, document);
        user.write("tags", Arrays.asList("sample", "test", "document"),
                document);
        user.write("lastModified", Timestamp.now(), document);
        runway.save(user, document);

        // Update with different data types
        user.write("text", "Updated text", document);
        user.write("version", 2, document);
        user.write("published", true, document);
        user.write("tags", Arrays.asList("updated", "final"), document);
        runway.save(user, document);

        Map<Timestamp, Map<String, Revision>> audit = document.audit();
        Assert.assertEquals(2, audit.size());

        // Verify all data types are handled correctly
        List<Map<String, Revision>> allRevisions = new ArrayList<>(
                audit.values());

        // First save should have all initial values
        Map<String, Revision> firstRevisions = allRevisions.get(0);
        Assert.assertTrue(firstRevisions.containsKey("text"));
        Assert.assertTrue(firstRevisions.containsKey("title"));
        Assert.assertTrue(firstRevisions.containsKey("version"));
        Assert.assertTrue(firstRevisions.containsKey("published"));
        Assert.assertTrue(firstRevisions.containsKey("tags"));
        Assert.assertTrue(firstRevisions.containsKey("lastModified"));

        // Second save should show changes
        Map<String, Revision> secondRevisions = allRevisions.get(1);
        Assert.assertTrue(secondRevisions.containsKey("text"));
        Assert.assertTrue(secondRevisions.containsKey("version"));
        Assert.assertTrue(secondRevisions.containsKey("published"));
        Assert.assertTrue(secondRevisions.containsKey("tags"));

        // Verify specific changes
        Revision textChange = secondRevisions.get("text");
        Assert.assertEquals("Sample text", textChange.from());
        Assert.assertEquals("Updated text", textChange.to());

        Revision versionChange = secondRevisions.get("version");
        Assert.assertEquals(1, versionChange.from());
        Assert.assertEquals(2, versionChange.to());

        Revision publishedChange = secondRevisions.get("published");
        Assert.assertEquals(false, publishedChange.from());
        Assert.assertEquals(true, publishedChange.to());
    }

    @Test
    public void testAuditTrailWithSelectiveFieldUpdates() {
        User user = new User();
        user.name = "Frank";
        user.email = "frank@example.com";
        user.roles = Arrays.asList("analyst");
        user.active = true;

        Project project = user.create(Project.class);
        user.write("name", "Initial Project", project);
        user.write("description", "Initial description", project);
        user.write("status", "draft", project);
        user.write("budget", 50000.0, project);
        runway.save(user, project);

        // Update only specific fields
        user.write("status", "active", project);
        user.write("budget", 75000.0, project);
        runway.save(user, project);

        // Test filtering to see only changed fields
        Map<Timestamp, Map<String, Revision>> audit = project.audit("status",
                "budget");
        Assert.assertEquals(2, audit.size());

        List<Map<String, Revision>> allRevisions = new ArrayList<>(
                audit.values());

        // First save should have initial values
        Map<String, Revision> firstRevisions = allRevisions.get(0);
        Assert.assertTrue(firstRevisions.containsKey("status"));
        Assert.assertTrue(firstRevisions.containsKey("budget"));
        Assert.assertFalse(firstRevisions.containsKey("name")); // Should be
                                                                // filtered out

        // Second save should show only the changes
        Map<String, Revision> secondRevisions = allRevisions.get(1);
        Assert.assertTrue(secondRevisions.containsKey("status"));
        Assert.assertTrue(secondRevisions.containsKey("budget"));
        Assert.assertFalse(secondRevisions.containsKey("name")); // Should be
                                                                 // filtered out

        // Verify the changes
        Revision statusChange = secondRevisions.get("status");
        Assert.assertEquals("draft", statusChange.from());
        Assert.assertEquals("active", statusChange.to());

        Revision budgetChange = secondRevisions.get("budget");
        Assert.assertEquals(50000.0, budgetChange.from());
        Assert.assertEquals(75000.0, budgetChange.to());
    }

    @Test
    public void testSelfAuthorshipOnInitialSave() {
        // Create a User that is its own author (e.g., self-registration)
        User user = new User();
        user.name = "Self Registrant";
        user.email = "self@example.com";
        user.roles = Arrays.asList("user");
        user.active = true;

        // Set the user as its own author
        user.write("name", user.name, user);
        user.write("email", user.email, user);
        user.write("roles", user.roles, user);
        user.write("active", user.active, user);

        // This should not throw an exception about self-referential links
        boolean saved = runway.save(user);
        if(!saved) {
            user.throwSupressedExceptions();
        }
        Assert.assertTrue(saved);

        // Verify the record was saved
        Assert.assertNotEquals(0, user.id());

        // Verify audit trail shows self-authorship
        Map<Timestamp, Map<String, Revision>> audit = user.audit();
        Assert.assertFalse(audit.isEmpty());

        Map<String, Revision> revisions = audit.values().iterator().next();
        Revision nameRevision = revisions.get("name");
        Assert.assertTrue(nameRevision.isAttributed());
        Assert.assertEquals(user, nameRevision.author());
        Assert.assertEquals(user.id(), nameRevision.author().id());
    }

    @Test
    public void testSelfAuthorshipAfterLoad() {
        // Create a User that is its own author
        User user = new User();
        user.name = "Self Author";
        user.email = "selfauthor@example.com";
        user.roles = Arrays.asList("admin");
        user.active = true;

        user.write("name", user.name, user);
        user.write("email", user.email, user);
        runway.save(user);

        long userId = user.id();

        // Load the user from the database
        User loadedUser = runway.load(User.class, userId);

        // Verify the audit trail still shows self-authorship
        Map<Timestamp, Map<String, Revision>> audit = loadedUser.audit();
        Assert.assertFalse(audit.isEmpty());

        Map<String, Revision> revisions = audit.values().iterator().next();
        Revision nameRevision = revisions.get("name");
        Assert.assertTrue(nameRevision.isAttributed());
        Assert.assertEquals(loadedUser, nameRevision.author());
        Assert.assertEquals(loadedUser.id(), nameRevision.author().id());
    }

    @Test
    public void testSelfAuthorshipWithMultipleChanges() {
        // Create a User that is its own author
        User user = new User();
        user.name = "Evolving User";
        user.email = "evolving@example.com";
        user.roles = Arrays.asList("user");
        user.active = true;

        // First save - self-authored
        user.write("name", user.name, user);
        user.write("email", user.email, user);
        runway.save(user);

        // Second save - still self-authored
        user.write("roles", Arrays.asList("user", "editor"), user);
        user.write("active", true, user);
        runway.save(user);

        // Third save - promote to admin, still self-authored
        user.write("roles", Arrays.asList("user", "editor", "admin"), user);
        runway.save(user);

        // Verify audit trail shows all changes with self-authorship
        Map<Timestamp, Map<String, Revision>> audit = user.audit();
        Assert.assertEquals(3, audit.size());

        // Verify all revisions are attributed to self
        for (Map<String, Revision> revisions : audit.values()) {
            for (Revision revision : revisions.values()) {
                if(revision.isAttributed()) {
                    Assert.assertEquals(user, revision.author());
                    Assert.assertEquals(user.id(), revision.author().id());
                }
            }
        }
    }

    @Test
    public void testSelfAuthorshipMixedWithOtherAuthors() {
        // User creates itself
        User user1 = new User();
        user1.name = "User One";
        user1.email = "user1@example.com";
        user1.roles = Arrays.asList("user");
        user1.active = true;

        user1.write("name", user1.name, user1);
        user1.write("email", user1.email, user1);
        runway.save(user1);

        // Another user creates itself
        User user2 = new User();
        user2.name = "User Two";
        user2.email = "user2@example.com";
        user2.roles = Arrays.asList("admin");
        user2.active = true;

        user2.write("name", user2.name, user2);
        user2.write("email", user2.email, user2);
        runway.save(user2);

        // Now user2 modifies user1 (cross-authorship)
        User loadedUser1 = runway.load(User.class, user1.id());
        user2.write("roles", Arrays.asList("user", "verified"), loadedUser1);
        runway.save(user2, loadedUser1);

        // Verify user1's audit trail shows self-authorship then user2
        Map<Timestamp, Map<String, Revision>> audit = loadedUser1.audit();
        Assert.assertEquals(2, audit.size());

        List<Map<String, Revision>> allRevisions = new ArrayList<>(
                audit.values());

        // First save - self-authored
        Map<String, Revision> firstRevisions = allRevisions.get(0);
        Revision firstNameRevision = firstRevisions.get("name");
        Assert.assertTrue(firstNameRevision.isAttributed());
        Assert.assertEquals(loadedUser1.id(), firstNameRevision.author().id());

        // Second save - authored by user2
        Map<String, Revision> secondRevisions = allRevisions.get(1);
        Revision rolesRevision = secondRevisions.get("roles");
        Assert.assertTrue(rolesRevision.isAttributed());
        Assert.assertEquals(user2.id(), rolesRevision.author().id());
        Assert.assertNotEquals(loadedUser1.id(), rolesRevision.author().id());
    }

    static class User extends Record implements Audience {
        String name;
        int age;
        String email;
        List<String> roles;
        boolean active;
    }

    static class Document extends Record {
        String text;
        String title;
        String author;
        List<String> tags;
        int version;
        boolean published;
        Timestamp lastModified;
    }

    static class Comment extends Record {
        String content;
        String author;
        int rating;
        boolean approved;
        List<String> mentions;
    }

    static class Project extends Record {
        String name;
        String description;
        String status;
        List<String> members;
        double budget;
        Timestamp startDate;
        Timestamp endDate;
    }

}
