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
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.util.Random;

/**
 * Tests for hierarchy-scoped {@link Unique} constraints declared with
 * {@code any = true}. The tests cover how the scope on the declaration governs
 * both {@link Unique} enforcement during a save and the identity that
 * {@link Runway#intern(Record) intern} converges on. Each test runs under both
 * Command-API modes (bulk enabled and disabled), so the tests exercise both
 * read paths of the transactional find.
 *
 * @author Jeff Nelson
 */
@RunWith(Parameterized.class)
public class UniqueAnyScopeTest extends RunwayBaseClientServerTest {

    /**
     * Return the parameter matrix that runs each test once per Command-API
     * mode.
     *
     * @return one row with bulk commands enabled and one with it disabled
     */
    @Parameters(name = "bulkCommands={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    /**
     * Whether the test run exercises the bulk Command-API read path.
     */
    private final boolean useBulkCommands;

    /**
     * Construct a new instance.
     *
     * @param useBulkCommands {@code true} to exercise the bulk Command-API read
     *            path; {@code false} for the incremental path
     */
    public UniqueAnyScopeTest(boolean useBulkCommands) {
        this.useBulkCommands = useBulkCommands;
    }

    @Override
    protected void beforeTestRun() {
        super.beforeTestRun();
        Reflection.set("supportsBulkCommands", useBulkCommands, runway); // (authorized)
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Unique} constraint with
     * {@code any = true} on an abstract parent is enforced across the parent's
     * subclasses, so a sibling class cannot store the same value.
     * <p>
     * <strong>Start state:</strong> No saved {@link File Files}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link ImageFile} with a locator.</li>
     * <li>Save a {@link VideoFile} with the same locator.</li>
     * <li>Call {@code throwSupressedExceptions()} on the {@link VideoFile} and
     * catch the recorded violation.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first save succeeds, the second save fails
     * with the constraint violation recorded, and only the {@link ImageFile}
     * exists in the {@link File} hierarchy.
     */
    @Test
    public void testSaveFailsWhenAnyConstraintValueExistsInSiblingClass() {
        String locator = Random.getSimpleString();
        Assert.assertTrue(runway.save(new ImageFile(locator, "image")));
        VideoFile video = new VideoFile(locator, "video");
        Assert.assertFalse(runway.save(video));
        boolean recorded = false;
        try {
            video.throwSupressedExceptions();
        }
        catch (SuppressedRunwayException e) {
            recorded = true;
        }
        Assert.assertTrue(recorded);
        Assert.assertEquals(1, runway.countAny(File.class));
    }

    /**
     * <strong>Goal:</strong> Verify the control: a plain class-scoped
     * {@link Unique} constraint on an abstract parent lets two different
     * subclasses store the same value.
     * <p>
     * <strong>Start state:</strong> No saved {@link Doc Docs}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link TextDoc} with a slug.</li>
     * <li>Save a {@link PdfDoc} with the same slug.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves succeed and two {@link Doc Docs}
     * exist in the hierarchy.
     */
    @Test
    public void testSaveSucceedsAcrossSubclassesWhenConstraintIsClassScoped() {
        String slug = Random.getSimpleString();
        Assert.assertTrue(runway.save(new TextDoc(slug)));
        Assert.assertTrue(runway.save(new PdfDoc(slug)));
        Assert.assertEquals(2, runway.countAny(Doc.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} adopts an existing
     * record of the same concrete class when it holds the {@code any}-scoped
     * identity.
     * <p>
     * <strong>Start state:</strong> One saved {@link ImageFile}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link ImageFile} with a distinct label.</li>
     * <li>Call {@code intern} with a new {@link ImageFile} that has the same
     * locator but a different label.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link ImageFile} has the saved
     * record's id and label, and no additional {@link File} exists.
     */
    @Test
    public void testInternAdoptsSameClassMatchUnderAnyConstraint() {
        String locator = Random.getSimpleString();
        ImageFile existing = new ImageFile(locator, "original");
        runway.save(existing);
        ImageFile probe = new ImageFile(locator, "copy");
        ImageFile interned = runway.intern(probe);
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals("original", interned.label);
        Assert.assertEquals(1, runway.countAny(File.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} never adopts a record
     * of a different concrete class that holds the {@code any}-scoped identity;
     * the conflict surfaces from the save instead.
     * <p>
     * <strong>Start state:</strong> One saved {@link VideoFile}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link VideoFile} with a locator.</li>
     * <li>Call {@code intern} with a new {@link ImageFile} that has the same
     * locator, and catch the expected exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link SuppressedRunwayException} is thrown
     * and only the original {@link VideoFile} exists in the hierarchy.
     */
    @Test
    public void testInternFailsLoudlyWhenSiblingClassClaimsAnyIdentity() {
        String locator = Random.getSimpleString();
        runway.save(new VideoFile(locator, "video"));
        boolean threw = false;
        try {
            runway.intern(new ImageFile(locator, "image"));
        }
        catch (SuppressedRunwayException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals(1, runway.countAny(File.class));
    }

    /**
     * <strong>Goal:</strong> Verify that, under mixed scopes, {@code intern}
     * does not adopt a same-class candidate that agrees with the {@code any}
     * constraint but not the class-scoped one; the save fails instead.
     * <p>
     * <strong>Start state:</strong> One saved {@link NamedFile}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link NamedFile} with a locator and a name.</li>
     * <li>Call {@code intern} with a new {@link NamedFile} that has the same
     * locator but a different name, and catch the expected exception.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A {@link SuppressedRunwayException} is thrown
     * and only the original {@link NamedFile} exists.
     */
    @Test
    public void testInternFailsWhenMixedScopeConstraintsPartiallyAgree() {
        String locator = Random.getSimpleString();
        runway.save(new NamedFile(locator, "first"));
        boolean threw = false;
        try {
            runway.intern(new NamedFile(locator, "second"));
        }
        catch (SuppressedRunwayException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertEquals(1, runway.count(NamedFile.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a {@code null} value under an
     * {@code any} constraint does not participate in identity, so the lookup
     * matches on the remaining class-scoped constraint.
     * <p>
     * <strong>Start state:</strong> One saved {@link NamedFile}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link NamedFile} with a locator and a name.</li>
     * <li>Call {@code intern} with a new {@link NamedFile} that has a
     * {@code null} locator and the same name.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link NamedFile} has the saved
     * record's id and no additional {@link NamedFile} exists.
     */
    @Test
    public void testInternSkipsNullAnyConstraintAndMatchesOnRemaining() {
        NamedFile existing = new NamedFile(Random.getSimpleString(), "shared");
        runway.save(existing);
        NamedFile probe = new NamedFile(null, "shared");
        NamedFile interned = runway.intern(probe);
        Assert.assertEquals(existing.id(), interned.id());
        Assert.assertEquals(1, runway.count(NamedFile.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@code intern} refuses a
     * {@link Record} whose every {@link Unique} value is {@code null}, even
     * when a constraint is {@code any}-scoped.
     * <p>
     * <strong>Start state:</strong> No saved {@link NamedFile NamedFiles}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} with a new {@link NamedFile} whose locator and
     * name are both {@code null}.</li>
     * <li>Catch the expected exception, then load every {@link NamedFile}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> An {@link IllegalArgumentException} is thrown
     * and no {@link NamedFile} exists in the database.
     */
    @Test
    public void testInternRefusesRecordWhoseUniqueValuesAreAllNull() {
        boolean threw = false;
        try {
            runway.intern(new NamedFile(null, null));
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertTrue(runway.load(NamedFile.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a named compound {@link Unique}
     * constraint with {@code any = true} on an abstract parent is enforced as
     * one identity across the parent's subclasses.
     * <p>
     * <strong>Start state:</strong> No saved {@link Region Regions}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link AudioRegion} that spans (1, 2).</li>
     * <li>Save a {@link VideoRegion} that spans (1, 2).</li>
     * <li>Save a {@link VideoRegion} that spans (1, 3).</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first save succeeds, the second save
     * fails, the third save succeeds because the group only partially agrees,
     * and two {@link Region Regions} exist in the hierarchy.
     */
    @Test
    public void testSaveEnforcesAnyGroupIdentityAcrossSubclasses() {
        Assert.assertTrue(runway.save(new AudioRegion(1, 2)));
        Assert.assertFalse(runway.save(new VideoRegion(1, 2)));
        Assert.assertTrue(runway.save(new VideoRegion(1, 3)));
        Assert.assertEquals(2, runway.countAny(Region.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a named {@link Unique} group whose
     * members disagree on {@code any} is rejected as a misdeclaration.
     * <p>
     * <strong>Start state:</strong> No saved {@link Mismatch Mismatches}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} with a new {@link Mismatch} and catch the
     * expected exception.</li>
     * <li>Save another new {@link Mismatch}, then load every
     * {@link Mismatch}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code intern} rejection is an
     * {@link IllegalArgumentException}, the save returns {@code false}, and no
     * {@link Mismatch} exists in the database.
     */
    @Test
    public void testInternRejectsNamedGroupWithMixedAnyFlags() {
        boolean threw = false;
        try {
            runway.intern(new Mismatch("a", "b"));
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertFalse(runway.save(new Mismatch("c", "d")));
        Assert.assertTrue(runway.load(Mismatch.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that, within a caller-owned
     * {@link Transaction}, an {@code intern} under an {@code any} constraint
     * stages the created record so it is invisible outside the transaction
     * until the commit and visible after it.
     * <p>
     * <strong>Start state:</strong> No saved {@link File Files}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Start a {@link Transaction} with {@link Runway#startTransaction()} in
     * a try-with-resources block.</li>
     * <li>Call {@code intern} on the transaction with a new
     * {@link ImageFile}.</li>
     * <li>Query the {@link File} hierarchy through the enclosing {@link Runway}
     * before the commit, then {@code commit()} and query again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The pre-commit query observes no match, the
     * commit succeeds, and the post-commit query returns the created
     * {@link ImageFile}.
     */
    @Test
    public void testInternStagesAnyCreateWithinOpenTransaction() {
        String value = Random.getSimpleString();
        long id;
        try (Transaction transaction = runway.startTransaction()) {
            ImageFile file = new ImageFile(value, "image");
            ImageFile interned = transaction.intern(file);
            Assert.assertSame(file, interned);
            Assert.assertNull(runway.findAnyUnique(File.class, locator(value)));
            Assert.assertTrue(transaction.commit());
            id = interned.id();
        }
        File visible = runway.findAnyUnique(File.class, locator(value));
        Assert.assertNotNull(visible);
        Assert.assertEquals(id, visible.id());
    }

    /**
     * <strong>Goal:</strong> Verify that a named {@code any} group whose
     * members are declared by different classes is rejected as a
     * misdeclaration.
     * <p>
     * <strong>Start state:</strong> No saved {@link Composite Composites}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@code intern} with a new {@link ExtendedComposite} and catch
     * the expected exception.</li>
     * <li>Save another new {@link ExtendedComposite}, then load every
     * {@link Composite}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The {@code intern} rejection is an
     * {@link IllegalArgumentException}, the save returns {@code false}, and no
     * {@link Composite} exists in the database.
     */
    @Test
    public void testRejectsAnyGroupWhoseMembersHaveDifferentDeclarers() {
        boolean threw = false;
        try {
            runway.intern(new ExtendedComposite("head", "tail"));
        }
        catch (IllegalArgumentException e) {
            threw = true;
        }
        Assert.assertTrue(threw);
        Assert.assertFalse(
                runway.save(new ExtendedComposite("other", "value")));
        Assert.assertTrue(runway.loadAny(Composite.class).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a re-save of a record under a
     * hierarchy-scoped {@link Unique} constraint does not collide with the
     * record itself.
     * <p>
     * <strong>Start state:</strong> One saved {@link ImageFile}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link ImageFile} with a locator.</li>
     * <li>Change the non-identity label on the same instance.</li>
     * <li>Save the instance again.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both saves succeed and one {@link File} exists
     * in the hierarchy.
     */
    @Test
    public void testResaveDoesNotCollideWithItselfUnderAnyConstraint() {
        ImageFile file = new ImageFile(Random.getSimpleString(), "before");
        Assert.assertTrue(runway.save(file));
        file.label = "after";
        Assert.assertTrue(runway.save(file));
        Assert.assertEquals(1, runway.countAny(File.class));
    }

    /**
     * <strong>Goal:</strong> Verify that a hierarchy-scoped {@link Unique}
     * constraint declared by a concrete class is enforced between the declarer
     * itself and its descendants.
     * <p>
     * <strong>Start state:</strong> No saved {@link Asset Assets}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save an {@link Asset} with a locator.</li>
     * <li>Save a {@link DerivedAsset} with the same locator.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The first save succeeds, the second save
     * fails, and one {@link Asset} exists in the hierarchy.
     */
    @Test
    public void testSaveFailsWhenAnyConstraintValueExistsInAncestorClass() {
        String locator = Random.getSimpleString();
        Assert.assertTrue(runway.save(new Asset(locator)));
        Assert.assertFalse(runway.save(new DerivedAsset(locator)));
        Assert.assertEquals(1, runway.countAny(Asset.class));
    }

    /**
     * Return a {@link Criteria} that matches every {@link File} whose
     * {@code locator} equals the given {@code value}.
     *
     * @param value the locator to match
     * @return the {@code locator == value} {@link Criteria}
     */
    private static Criteria locator(String value) {
        return Criteria.where().key("locator").operator(Operator.EQUALS)
                .value(value).build();
    }

    /**
     * An abstract {@link Record} whose identity is a {@link Unique} locator
     * that spans the class hierarchy.
     *
     * @author Jeff Nelson
     */
    public static abstract class File extends Record {

        /**
         * The identity locator, shared across every {@link File} subclass.
         */
        @Unique(any = true)
        String locator;

        /**
         * A non-identity label.
         */
        String label;

        /**
         * Construct a new instance.
         *
         * @param locator the identity locator
         * @param label the label
         */
        public File(String locator, String label) {
            this.locator = locator;
            this.label = label;
        }
    }

    /**
     * A concrete {@link File} subclass.
     *
     * @author Jeff Nelson
     */
    public static class ImageFile extends File {

        /**
         * Construct a new instance.
         *
         * @param locator the identity locator
         * @param label the label
         */
        public ImageFile(String locator, String label) {
            super(locator, label);
        }
    }

    /**
     * A concrete {@link File} subclass that is a sibling of {@link ImageFile}.
     *
     * @author Jeff Nelson
     */
    public static class VideoFile extends File {

        /**
         * Construct a new instance.
         *
         * @param locator the identity locator
         * @param label the label
         */
        public VideoFile(String locator, String label) {
            super(locator, label);
        }
    }

    /**
     * A concrete {@link File} subclass that adds a class-scoped {@link Unique}
     * name alongside the parent's hierarchy-scoped locator.
     *
     * @author Jeff Nelson
     */
    public static class NamedFile extends File {

        /**
         * The class-scoped identity name.
         */
        @Unique
        String name;

        /**
         * Construct a new instance.
         *
         * @param locator the identity locator
         * @param name the identity name
         */
        public NamedFile(String locator, String name) {
            super(locator, null);
            this.name = name;
        }
    }

    /**
     * An abstract {@link Record} whose {@link Unique} slug is class-scoped, as
     * the control for the hierarchy-scoped fixtures.
     *
     * @author Jeff Nelson
     */
    public static abstract class Doc extends Record {

        /**
         * The class-scoped identity slug.
         */
        @Unique
        String slug;

        /**
         * Construct a new instance.
         *
         * @param slug the identity slug
         */
        public Doc(String slug) {
            this.slug = slug;
        }
    }

    /**
     * A concrete {@link Doc} subclass.
     *
     * @author Jeff Nelson
     */
    public static class TextDoc extends Doc {

        /**
         * Construct a new instance.
         *
         * @param slug the identity slug
         */
        public TextDoc(String slug) {
            super(slug);
        }
    }

    /**
     * A concrete {@link Doc} subclass that is a sibling of {@link TextDoc}.
     *
     * @author Jeff Nelson
     */
    public static class PdfDoc extends Doc {

        /**
         * Construct a new instance.
         *
         * @param slug the identity slug
         */
        public PdfDoc(String slug) {
            super(slug);
        }
    }

    /**
     * An abstract {@link Record} whose identity is a named compound
     * {@link Unique} constraint that spans the class hierarchy.
     *
     * @author Jeff Nelson
     */
    public static abstract class Region extends Record {

        /**
         * The start of the compound identity.
         */
        @Unique(name = "span", any = true)
        int start;

        /**
         * The end of the compound identity.
         */
        @Unique(name = "span", any = true)
        int end;

        /**
         * Construct a new instance.
         *
         * @param start the start of the span
         * @param end the end of the span
         */
        public Region(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    /**
     * A concrete {@link Region} subclass.
     *
     * @author Jeff Nelson
     */
    public static class AudioRegion extends Region {

        /**
         * Construct a new instance.
         *
         * @param start the start of the span
         * @param end the end of the span
         */
        public AudioRegion(int start, int end) {
            super(start, end);
        }
    }

    /**
     * A concrete {@link Region} subclass that is a sibling of
     * {@link AudioRegion}.
     *
     * @author Jeff Nelson
     */
    public static class VideoRegion extends Region {

        /**
         * Construct a new instance.
         *
         * @param start the start of the span
         * @param end the end of the span
         */
        public VideoRegion(int start, int end) {
            super(start, end);
        }
    }

    /**
     * A {@link Record} whose named {@link Unique} group misdeclares its scope:
     * the members disagree on {@code any}.
     *
     * @author Jeff Nelson
     */
    public static class Mismatch extends Record {

        /**
         * The first member of the misdeclared group.
         */
        @Unique(name = "pair", any = true)
        String first;

        /**
         * The second member of the misdeclared group.
         */
        @Unique(name = "pair")
        String second;

        /**
         * Construct a new instance.
         *
         * @param first the first member
         * @param second the second member
         */
        public Mismatch(String first, String second) {
            this.first = first;
            this.second = second;
        }
    }

    /**
     * An abstract {@link Record} that declares the first member of a named
     * {@link Unique} group that spans the class hierarchy.
     *
     * @author Jeff Nelson
     */
    public static abstract class Composite extends Record {

        /**
         * The member of the group that the least-derived class declares.
         */
        @Unique(name = "whole", any = true)
        String head;

        /**
         * Construct a new instance.
         *
         * @param head the first member of the group
         */
        public Composite(String head) {
            this.head = head;
        }
    }

    /**
     * A concrete {@link Composite} subclass that declares the second member of
     * the named group, so the group's members span two classes of one lineage.
     *
     * @author Jeff Nelson
     */
    public static class ExtendedComposite extends Composite {

        /**
         * The member of the group that the subclass declares.
         */
        @Unique(name = "whole", any = true)
        String tail;

        /**
         * Construct a new instance.
         *
         * @param head the first member of the group
         * @param tail the second member of the group
         */
        public ExtendedComposite(String head, String tail) {
            super(head);
            this.tail = tail;
        }
    }

    /**
     * A concrete {@link Record} that declares a hierarchy-scoped {@link Unique}
     * locator, so the identity space includes the declarer itself.
     *
     * @author Jeff Nelson
     */
    public static class Asset extends Record {

        /**
         * The identity locator, shared between {@link Asset} and every
         * descendant.
         */
        @Unique(any = true)
        String locator;

        /**
         * Construct a new instance.
         *
         * @param locator the identity locator
         */
        public Asset(String locator) {
            this.locator = locator;
        }
    }

    /**
     * A concrete {@link Asset} subclass, a descendant of the concrete declarer.
     *
     * @author Jeff Nelson
     */
    public static class DerivedAsset extends Asset {

        /**
         * Construct a new instance.
         *
         * @param locator the identity locator
         */
        public DerivedAsset(String locator) {
            super(locator);
        }
    }

}
