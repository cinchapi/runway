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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.concourse.lang.Criteria;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.runway.Record;
import com.cinchapi.runway.Selection;

/**
 * Unit tests for the {@link Scope} factory methods and singleton guarantees.
 *
 * @author Jeff Nelson
 */
public class ScopeTest {

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unrestricted()} returns
     * the input {@link Selection} unchanged.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Selection}.</li>
     * <li>Call {@link Scope#unrestricted()}.{@code apply(selection)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The returned {@link Selection} is the same
     * reference as the input.
     */
    @Test
    public void testUnrestrictedApplyReturnsSameSelection() {
        Selection<?> selection = Selection.of(TestRecord.class);
        Assert.assertSame(selection, Scope.unrestricted().apply(selection));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#none()} returns a
     * different {@link Selection} with a filter injected.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Selection}.</li>
     * <li>Call {@link Scope#none()}.{@code apply(selection)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A non-null {@link Selection} is returned that
     * is not the same reference as the input.
     */
    @Test
    public void testNoneApplyReturnsModifiedSelection() {
        Selection<?> selection = Selection.of(TestRecord.class);
        Selection<?> result = Scope.none().apply(selection);
        Assert.assertNotNull(result);
        Assert.assertNotSame(selection, result);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#of(Criteria)} returns a
     * different {@link Selection} with criteria injected.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Create a {@link Criteria} and a {@link Selection}.</li>
     * <li>Call {@link Scope#of(Criteria)}.{@code apply(selection)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> A non-null {@link Selection} is returned that
     * is not the same reference as the input.
     */
    @Test
    public void testOfApplyReturnsModifiedSelection() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Selection<?> selection = Selection.of(TestRecord.class);
        Selection<?> result = Scope.of(criteria).apply(selection);
        Assert.assertNotNull(result);
        Assert.assertNotSame(selection, result);
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unsupported()} returns
     * {@code false} from {@link Scope#isApplicable()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#isApplicable()} on
     * {@link Scope#unsupported()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code false} is returned.
     */
    @Test
    public void testUnsupportedIsNotApplicable() {
        Assert.assertFalse(Scope.unsupported().isApplicable());
    }

    /**
     * <strong>Goal:</strong> Verify that all concrete {@link Scope} variants
     * that carry a meaningful visibility rule return {@code true} from
     * {@link Scope#isApplicable()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#isApplicable()} on {@link Scope#unrestricted()},
     * {@link Scope#none()}, and a {@link Scope#of(Criteria)} instance.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> All three return {@code true}.
     */
    @Test
    public void testApplicableVariantsReturnTrue() {
        Criteria criteria = Criteria.where().key("active")
                .operator(Operator.EQUALS).value(true).build();
        Assert.assertTrue(Scope.unrestricted().isApplicable());
        Assert.assertTrue(Scope.none().isApplicable());
        Assert.assertTrue(Scope.of(criteria).isApplicable());
    }

    /**
     * <strong>Goal:</strong> Verify that calling {@link Scope#apply(Selection)}
     * on {@link Scope#unsupported()} throws
     * {@link UnsupportedOperationException}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#apply(Selection)} on
     * {@link Scope#unsupported()}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@link UnsupportedOperationException} is
     * thrown.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedApplyThrows() {
        Scope.unsupported().apply(Selection.of(TestRecord.class));
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unrestricted()} returns
     * the same singleton instance on every call.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#unrestricted()} twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same object reference.
     */
    @Test
    public void testUnrestrictedIsSingleton() {
        Assert.assertSame(Scope.unrestricted(), Scope.unrestricted());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#none()} returns the same
     * singleton instance on every call.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#none()} twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same object reference.
     */
    @Test
    public void testNoneIsSingleton() {
        Assert.assertSame(Scope.none(), Scope.none());
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Scope#unsupported()} returns
     * the same singleton instance on every call.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Call {@link Scope#unsupported()} twice.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls return the same object reference.
     */
    @Test
    public void testUnsupportedIsSingleton() {
        Assert.assertSame(Scope.unsupported(), Scope.unsupported());
    }

    /**
     * A minimal {@link Record} used as the type parameter for {@link Selection}
     * instances in these tests.
     */
    static class TestRecord extends Record {}

}
