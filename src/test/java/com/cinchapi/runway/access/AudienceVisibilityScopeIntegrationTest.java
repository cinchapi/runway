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

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.lang.paginate.Page;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.RunwayBaseClientServerTest;
import com.cinchapi.runway.Selection;
import com.cinchapi.runway.Selections;

/**
 * Integration tests verifying that the scope-based visibility system correctly
 * filters records at the database level, enabling correct pagination and cache
 * behavior.
 *
 * @author Jeff Nelson
 */
public class AudienceVisibilityScopeIntegrationTest
        extends RunwayBaseClientServerTest {

    /**
     * Clear the registry before and after each test.
     */
    @Before
    @After
    public void clearRegistry() {
        AccessControlSupport.VISIBILITY_SCOPES.clear();
    }

    /**
     * <strong>Goal:</strong> Verify that when {@link Scope#unrestricted()} is
     * returned, all records are visible regardless of what
     * {@code $isDiscoverableBy} would return.
     * <p>
     * <strong>Start state:</strong> Three {@link OwnedDocument Documents} with
     * different owners saved. Scope registered to return
     * {@link Scope#unrestricted()} for all audiences.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register an unrestricted scope for {@link OwnedDocument}.</li>
     * <li>Save three documents owned by different users.</li>
     * <li>Load an audience and call {@code select(Selection.of(...))}</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> All three documents are returned despite
     * {@code $isDiscoverableBy} being restrictive.
     */
    @Test
    public void testUnrestrictedScopeSkipsVisibilityFiltering() {
        AccessControl.registerVisibilityScope(OwnedDocument.class,
                audience -> Scope.unrestricted());
        TestUser userA = new TestUser("alice");
        userA.save();
        OwnedDocument d1 = new OwnedDocument("doc1", "alice");
        OwnedDocument d2 = new OwnedDocument("doc2", "bob");
        OwnedDocument d3 = new OwnedDocument("doc3", "carol");
        runway.save(d1, d2, d3);
        Selections sel = userA.select(Selection.of(OwnedDocument.class));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(3, results.size());
    }

    /**
     * <strong>Goal:</strong> Verify that when {@link Scope#none()} is returned,
     * no records are visible to the audience.
     * <p>
     * <strong>Start state:</strong> Two saved {@link OwnedDocument Documents}.
     * Scope registered to return {@link Scope#none()} for all audiences.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a none scope for {@link OwnedDocument}.</li>
     * <li>Save two documents.</li>
     * <li>Call {@code select} as an audience.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The result set is empty.
     */
    @Test
    public void testNoneScopeReturnsEmptyResults() {
        AccessControl.registerVisibilityScope(OwnedDocument.class,
                audience -> Scope.none());
        TestUser user = new TestUser("alice");
        user.save();
        runway.save(new OwnedDocument("doc1", "alice"),
                new OwnedDocument("doc2", "bob"));
        Selections sel = user.select(Selection.of(OwnedDocument.class));
        Set<OwnedDocument> results = sel.next();
        Assert.assertTrue(results.isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a criteria-based {@link Scope} filters
     * records at the database level, returning only matching records.
     * <p>
     * <strong>Start state:</strong> Three documents: two owned by "alice", one
     * owned by "bob". Scope registered to return criteria matching the
     * audience's name.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria scope that matches on {@code owner}.</li>
     * <li>Save three documents with different owners.</li>
     * <li>Call {@code select} as alice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only alice's two documents are returned.
     */
    @Test
    public void testCriteriaScopeFiltersAtDatabaseLevel() {
        AccessControl.registerVisibilityScope(OwnedDocument.class, audience -> {
            if(audience instanceof TestUser) {
                String name = ((TestUser) audience).name;
                return Scope.of(Criteria.where().key("owner")
                        .operator(Operator.EQUALS).value(name).build());
            }
            else {
                return Scope.none();
            }
        });
        TestUser alice = new TestUser("alice");
        alice.save();
        runway.save(new OwnedDocument("d1", "alice"),
                new OwnedDocument("d2", "alice"),
                new OwnedDocument("d3", "bob"));
        Selections sel = alice.select(Selection.of(OwnedDocument.class));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(2, results.size());
        results.forEach(doc -> Assert.assertEquals("alice", doc.owner));
    }

    /**
     * <strong>Goal:</strong> Verify that criteria-scoped pagination returns
     * correctly sized pages even when many records fail the visibility check.
     * <p>
     * <strong>Start state:</strong> 20 documents owned by "alice", 10 owned by
     * "bob". Scope registered to filter on owner. Page size 10.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria scope filtering on {@code owner}.</li>
     * <li>Save 30 documents (20 alice, 10 bob).</li>
     * <li>Query page 1 (size 10) as alice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Page 1 contains exactly 10 of alice's
     * documents.
     */
    @Test
    public void testCriteriaScopeWithPaginationProducesCorrectPages() {
        AccessControl.registerVisibilityScope(OwnedDocument.class, audience -> {
            if(audience instanceof TestUser) {
                String name = ((TestUser) audience).name;
                return Scope.of(Criteria.where().key("owner")
                        .operator(Operator.EQUALS).value(name).build());
            }
            else {
                return Scope.none();
            }
        });
        TestUser alice = new TestUser("alice");
        alice.save();
        for (int i = 0; i < 20; ++i) {
            runway.save(new OwnedDocument("alice-doc-" + i, "alice"));
        }
        for (int i = 0; i < 10; ++i) {
            runway.save(new OwnedDocument("bob-doc-" + i, "bob"));
        }
        Page page = Page.sized(10).go(1);
        Selections sel = alice
                .select(Selection.of(OwnedDocument.class).page(page));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(10, results.size());
        results.forEach(doc -> Assert.assertEquals("alice", doc.owner));
    }

    /**
     * <strong>Goal:</strong> Verify that when {@link Scope#unsupported()} is
     * returned, the framework falls back to the legacy predicate filter.
     * <p>
     * <strong>Start state:</strong> Two documents: one owned by "alice", one by
     * "bob". Scope returns {@code unsupported} — predicate falls back to
     * {@code $isDiscoverableBy} (owner-match only).
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a scope that returns {@link Scope#unsupported()}.</li>
     * <li>Save two documents with different owners.</li>
     * <li>Query as alice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only alice's document is returned (predicate
     * path still works).
     */
    @Test
    public void testUnsupportedScopeFallsBackToPredicate() {
        AccessControl.registerVisibilityScope(OwnedDocument.class,
                audience -> Scope.unsupported());
        TestUser alice = new TestUser("alice");
        alice.save();
        runway.save(new OwnedDocument("d1", "alice"),
                new OwnedDocument("d2", "bob"));
        Selections sel = alice.select(Selection.of(OwnedDocument.class));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("alice", results.iterator().next().owner);
    }

    /**
     * <strong>Goal:</strong> Verify that when no scope is registered for a
     * class, the legacy predicate behavior is preserved (backwards
     * compatibility).
     * <p>
     * <strong>Start state:</strong> Two documents with different owners. No
     * scope registered.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save two documents (no scope registered).</li>
     * <li>Query as alice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only alice's document is returned (legacy
     * predicate path is active).
     */
    @Test
    public void testNoScopeRegisteredPreservesLegacyBehavior() {
        TestUser alice = new TestUser("alice");
        alice.save();
        runway.save(new OwnedDocument("d1", "alice"),
                new OwnedDocument("d2", "bob"));
        Selections sel = alice.select(Selection.of(OwnedDocument.class));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("alice", results.iterator().next().owner);
    }

    /**
     * <strong>Goal:</strong> Verify that a count query through a criteria scope
     * returns the correct count without fetching records.
     * <p>
     * <strong>Start state:</strong> Five documents owned by "alice", three by
     * "bob". Criteria scope registered.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria scope filtering on {@code owner}.</li>
     * <li>Save eight documents.</li>
     * <li>Count as alice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Count is 5.
     */
    @Test
    public void testCountWithCriteriaScope() {
        AccessControl.registerVisibilityScope(OwnedDocument.class, audience -> {
            if(audience instanceof TestUser) {
                String name = ((TestUser) audience).name;
                return Scope.of(Criteria.where().key("owner")
                        .operator(Operator.EQUALS).value(name).build());
            }
            else {
                return Scope.none();
            }
        });
        TestUser alice = new TestUser("alice");
        alice.save();
        for (int i = 0; i < 5; ++i) {
            runway.save(new OwnedDocument("alice-" + i, "alice"));
        }
        for (int i = 0; i < 3; ++i) {
            runway.save(new OwnedDocument("bob-" + i, "bob"));
        }
        Selections sel = alice
                .select(Selection.of(OwnedDocument.class).count());
        int count = sel.next();
        Assert.assertEquals(5, count);
    }

    /**
     * <strong>Goal:</strong> Verify that a load-class selection through a
     * criteria scope returns only matching records.
     * <p>
     * <strong>Start state:</strong> Four documents with mixed owners. Criteria
     * scope registered for load-class queries.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria scope filtering on {@code owner}.</li>
     * <li>Save four documents (two per owner).</li>
     * <li>Query all records as alice via a plain load-class selection.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only alice's two documents are returned.
     */
    @Test
    public void testLoadClassWithCriteriaScope() {
        AccessControl.registerVisibilityScope(OwnedDocument.class, audience -> {
            if(audience instanceof TestUser) {
                String name = ((TestUser) audience).name;
                return Scope.of(Criteria.where().key("owner")
                        .operator(Operator.EQUALS).value(name).build());
            }
            else {
                return Scope.none();
            }
        });
        TestUser alice = new TestUser("alice");
        alice.save();
        runway.save(new OwnedDocument("a1", "alice"),
                new OwnedDocument("a2", "alice"),
                new OwnedDocument("b1", "bob"), new OwnedDocument("b2", "bob"));
        Selections sel = alice.select(Selection.of(OwnedDocument.class));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(2, results.size());
        results.forEach(doc -> Assert.assertEquals("alice", doc.owner));
    }

    /**
     * <strong>Goal:</strong> Verify that when a registered provider returns
     * {@code null}, the framework falls back to the instance-based predicate
     * filter.
     * <p>
     * <strong>Start state:</strong> Two documents with different owners. Scope
     * provider registered to return {@code null}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a scope provider that returns {@code null}.</li>
     * <li>Save one document owned by alice and one by bob.</li>
     * <li>Query as alice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Only alice's document is returned; the
     * {@code null} scope triggers the predicate fallback path.
     */
    @Test
    public void testNullScopeFallsBackToPredicate() {
        AccessControl.registerVisibilityScope(OwnedDocument.class,
                audience -> null);
        TestUser alice = new TestUser("alice");
        alice.save();
        runway.save(new OwnedDocument("d1", "alice"),
                new OwnedDocument("d2", "bob"));
        Selections sel = alice.select(Selection.of(OwnedDocument.class));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("alice", results.iterator().next().owner);
    }

    /**
     * <strong>Goal:</strong> Verify that a criteria scope is ANDed with an
     * existing query {@link Criteria} on a find {@link Selection}, so that both
     * constraints must be satisfied.
     * <p>
     * <strong>Start state:</strong> Three documents: one matching both
     * criteria, one matching only the user query, one matching only the scope.
     * Criteria scope registered to filter on owner.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria scope: {@code owner = "alice"}.</li>
     * <li>Save three documents: {@code "target"/"alice"},
     * {@code "other"/"alice"}, {@code "target"/"bob"}.</li>
     * <li>Query using a find {@link Selection} with criteria
     * {@code title = "target"} as alice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Exactly one document is returned — the one
     * where both {@code title = "target"} and {@code owner = "alice"} are
     * satisfied.
     */
    @Test
    public void testFindSelectionWithCriteriaScopeConjoinsCriteria() {
        AccessControl.registerVisibilityScope(OwnedDocument.class, audience -> {
            if(audience instanceof TestUser) {
                String name = ((TestUser) audience).name;
                return Scope.of(Criteria.where().key("owner")
                        .operator(Operator.EQUALS).value(name).build());
            }
            else {
                return Scope.none();
            }
        });
        TestUser alice = new TestUser("alice");
        alice.save();
        runway.save(new OwnedDocument("target", "alice"),
                new OwnedDocument("other", "alice"),
                new OwnedDocument("target", "bob"));
        Criteria userCriteria = Criteria.where().key("title")
                .operator(Operator.EQUALS).value("target").build();
        Selections sel = alice
                .select(Selection.of(OwnedDocument.class).where(userCriteria));
        Set<OwnedDocument> results = sel.next();
        Assert.assertEquals(1, results.size());
        OwnedDocument doc = results.iterator().next();
        Assert.assertEquals("target", doc.title);
        Assert.assertEquals("alice", doc.owner);
    }

    /**
     * <strong>Goal:</strong> Verify that a batch {@link Selection} containing
     * both an {@link AccessControl} type (with a registered scope) and a
     * non-{@link AccessControl} type processes each selection independently,
     * applying the scope to the former and returning all records for the
     * latter.
     * <p>
     * <strong>Start state:</strong> Two {@link OwnedDocument OwnedDocuments}
     * (alice's and bob's) and three {@link Note Notes} saved. Criteria scope
     * registered for {@link OwnedDocument}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Register a criteria scope for {@link OwnedDocument}.</li>
     * <li>Save one document per owner and three notes.</li>
     * <li>Issue a two-selection batch: one for {@link OwnedDocument}, one for
     * {@link Note}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@link OwnedDocument} result contains only
     * alice's document; the {@link Note} result contains all three notes.
     */
    @Test
    public void testMixedBatchAppliesScopeToAccessControlOnly() {
        AccessControl.registerVisibilityScope(OwnedDocument.class, audience -> {
            if(audience instanceof TestUser) {
                String name = ((TestUser) audience).name;
                return Scope.of(Criteria.where().key("owner")
                        .operator(Operator.EQUALS).value(name).build());
            }
            else {
                return Scope.none();
            }
        });
        TestUser alice = new TestUser("alice");
        alice.save();
        runway.save(new OwnedDocument("doc-alice", "alice"),
                new OwnedDocument("doc-bob", "bob"));
        runway.save(new Note("n1"), new Note("n2"), new Note("n3"));
        Selections sel = alice.select(Selection.of(OwnedDocument.class),
                Selection.of(Note.class));
        Set<OwnedDocument> docs = sel.next();
        Set<Note> notes = sel.next();
        Assert.assertEquals(1, docs.size());
        Assert.assertEquals("alice", docs.iterator().next().owner);
        Assert.assertEquals(3, notes.size());
    }

    /**
     * A document with an explicit {@code owner} field. Visibility (via the
     * predicate path) is granted only to the audience whose name matches the
     * document's owner.
     */
    static class OwnedDocument extends Record implements AccessControl {

        /**
         * The document title.
         */
        public String title;

        /**
         * The owner's name.
         */
        public String owner;

        /**
         * Construct a new {@link OwnedDocument}.
         *
         * @param title the title
         * @param owner the owner name
         */
        OwnedDocument(String title, String owner) {
            this.title = title;
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
            return audience instanceof TestUser
                    && ((TestUser) audience).name.equals(owner);
        }

        @Override
        public boolean $isDiscoverableBy(@Nonnull Audience audience) {
            return audience instanceof TestUser
                    && ((TestUser) audience).name.equals(owner);
        }

        @Override
        public boolean $isDiscoverableByAnonymous() {
            return false;
        }

        @Override
        public Set<String> $readableBy(@Nonnull Audience audience) {
            return $isDiscoverableBy(audience) ? AccessControl.ALL_KEYS
                    : AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $readableByAnonymous() {
            return AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableBy(@Nonnull Audience audience) {
            return $isDiscoverableBy(audience) ? AccessControl.ALL_KEYS
                    : AccessControl.NO_KEYS;
        }

        @Override
        public Set<String> $writableByAnonymous() {
            return AccessControl.NO_KEYS;
        }

    }

    /**
     * A plain {@link Record} with no {@link AccessControl} constraints, used to
     * verify that non-access-controlled selections are unaffected by scope
     * registration.
     */
    static class Note extends Record {

        /**
         * The note text.
         */
        public String text;

        /**
         * Construct a new {@link Note}.
         *
         * @param text the note text
         */
        Note(String text) {
            this.text = text;
        }

    }

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

}
