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

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.runway.access.Audience;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Regression tests for
 * <a href="https://github.com/cinchapi/runway/issues/147">GH-147</a>: a
 * {@link DynamicWritePolicy} governs which fields
 * {@link Record#set(String, Object)} may write, while the default policy
 * preserves the historical behavior of writing any field.
 *
 * @author Jeff Nelson
 */
public class GH147 extends RunwayBaseClientServerTest {

    /**
     * Assert that setting {@code key} to {@code value} on {@code record} throws
     * a {@link NonWritableFieldException}.
     *
     * @param record the {@link Record} to modify
     * @param key the key name
     * @param value the value to set
     */
    private static void assertNonWritable(Record record, String key,
            Object value) {
        try {
            record.set(key, value);
            Assert.fail("Expected a NonWritableFieldException when setting '"
                    + key + "'");
        }
        catch (NonWritableFieldException e) {
            // expected
        }
    }

    /**
     * Return a {@link Runway} bound to the test server that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     *
     * @return the strict {@link Runway}
     */
    private Runway strictRunway() {
        return runwayBuilder()
                .dynamicWritePolicy(DynamicWritePolicy.javaDefaults()).build();
    }

    /**
     * <strong>Goal:</strong> Verify that, by default, {@link Record#set} can
     * still write a final field, which preserves the historical behavior.
     * <p>
     * <strong>Start state:</strong> A single default {@link Runway} instance,
     * so new {@link Record Records} auto-assign to it.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} whose {@code name} field is final.</li>
     * <li>Call {@code set("name", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The final field holds the new value.
     */
    @Test
    public void testSetFinalFieldSucceedsByDefault() {
        Vault vault = new Vault("vault", "code");
        vault.set("name", "changed");
        Assert.assertEquals("changed", vault.name);
    }

    /**
     * <strong>Goal:</strong> Verify that, by default, {@link Record#set} can
     * still write a private field, which preserves the historical behavior.
     * <p>
     * <strong>Start state:</strong> A single default {@link Runway} instance,
     * so new {@link Record Records} auto-assign to it.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} with a private {@code secret} field.</li>
     * <li>Call {@code set("secret", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The private field holds the new value.
     */
    @Test
    public void testSetPrivateFieldSucceedsByDefault() {
        Vault vault = new Vault("vault", "code");
        vault.set("secret", "changed");
        Assert.assertEquals("changed", vault.secret);
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy causes
     * {@link Record#set} to refuse a write to a final field.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("name", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and the field keeps its original value.
     */
    @Test
    public void testJavaDefaultsRefusesSetOfFinalField() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            assertNonWritable(vault, "name", "changed");
            Assert.assertEquals("vault", vault.name);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy causes
     * {@link Record#set} to refuse a write to a private field.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("secret", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and the field keeps its original value.
     */
    @Test
    public void testJavaDefaultsRefusesSetOfPrivateField() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            assertNonWritable(vault, "secret", "changed");
            Assert.assertNull(vault.secret);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy causes
     * {@link Record#set} to refuse a write to a protected field.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("owner", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and the field keeps its original value.
     */
    @Test
    public void testJavaDefaultsRefusesSetOfProtectedField() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            assertNonWritable(vault, "owner", "changed");
            Assert.assertNull(vault.owner);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy causes
     * {@link Record#set} to refuse a write to a package-private field.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("region", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and the field keeps its original value.
     */
    @Test
    public void testJavaDefaultsRefusesSetOfPackagePrivateField()
            throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            assertNonWritable(vault, "region", "changed");
            Assert.assertNull(vault.region);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy still
     * allows {@link Record#set} to write a public non-final field.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("description", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The public field holds the new value.
     */
    @Test
    public void testJavaDefaultsAllowsSetOfPublicField() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            vault.set("description", "changed");
            Assert.assertEquals("changed", vault.description);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a field annotated with
     * {@link Writable} is writable through {@link Record#set} even when the
     * field is private and final and the governing policy is strict.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} whose private final {@code code} field is
     * annotated with {@link Writable}.</li>
     * <li>Assign the {@link Vault} to the strict {@link Runway}.</li>
     * <li>Call {@code set("code", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The field holds the new value.
     */
    @Test
    public void testWritableFieldIsExemptFromPolicy() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            vault.set("code", "changed");
            Assert.assertEquals("changed", vault.code);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a refused write does not store the
     * value as dynamic data, so a repeat of the same call is refused again.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("secret", "changed")} two times.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> Both calls throw
     * {@link NonWritableFieldException} and the field keeps its original value.
     */
    @Test
    public void testRefusedWriteDoesNotBecomeDynamicData() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            assertNonWritable(vault, "secret", "changed");
            assertNonWritable(vault, "secret", "changed");
            Assert.assertNull(vault.secret);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a strict policy does not affect
     * dynamic attributes whose keys do not name a field in the {@link Record
     * Record's} schema.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("nickname", "the-vault")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The value is stored as a dynamic attribute and
     * {@code get("nickname")} returns it.
     */
    @Test
    public void testDynamicKeyStillSupportedUnderStrictPolicy()
            throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            vault.set("nickname", "the-vault");
            Assert.assertEquals("the-vault", vault.get("nickname"));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#set(java.util.Map)} also
     * enforces the governing policy.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set} with a map that contains the final {@code name}
     * key.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and the field keeps its original value.
     */
    @Test
    public void testSetMapRefusesNonWritableField() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            try {
                vault.set(ImmutableMap.of("name", "changed"));
                Assert.fail("Expected a NonWritableFieldException");
            }
            catch (NonWritableFieldException e) {
                Assert.assertEquals("vault", vault.name);
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a custom policy can allow final fields
     * while it continues to refuse private fields.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} with a
     * {@link DynamicWritePolicy#builder() built} policy that only allows final
     * fields.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the custom
     * {@link Runway}.</li>
     * <li>Call {@code set("name", "changed")} on the public final field.</li>
     * <li>Call {@code set("secret", "changed")} on the private field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The final field holds the new value and the
     * private write throws {@link NonWritableFieldException}.
     */
    @Test
    public void testCustomPolicyAllowsFinalFieldsOnly() throws Exception {
        try (Runway custom = runwayBuilder()
                .dynamicWritePolicy(
                        DynamicWritePolicy.builder().allowFinalFields().build())
                .build()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(custom);
            vault.set("name", "changed");
            Assert.assertEquals("changed", vault.name);
            assertNonWritable(vault, "secret", "changed");
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the framework's hydration path is
     * exempt from the policy, so a load populates final and private fields
     * under a strict policy.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} with a final {@code name} and a private
     * {@code secret}.</li>
     * <li>Save the {@link Vault} with the strict {@link Runway}.</li>
     * <li>Load the {@link Vault} by id.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The loaded record has the saved values in its
     * final and private fields.
     */
    @Test
    public void testHydrationIsExemptFromPolicy() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.secret = "secret";
            strict.save(vault);
            Vault loaded = strict.load(Vault.class, vault.id());
            Assert.assertEquals("vault", loaded.name);
            Assert.assertEquals("code", loaded.code);
            Assert.assertEquals("secret", loaded.secret);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a {@link Record} that is not assigned
     * to any {@link Runway} instance follows the permissive default policy.
     * <p>
     * <strong>Start state:</strong> Two open {@link Runway} instances, so a new
     * {@link Record} does not auto-assign to either one.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Open a second, strict {@link Runway} without an assignment.</li>
     * <li>Construct a {@link Vault} and do not assign it.</li>
     * <li>Call {@code set("name", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The final field holds the new value.
     */
    @Test
    public void testUnassignedRecordUsesPermissiveDefault() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.set("name", "changed");
            Assert.assertEquals("changed", vault.name);
        }
    }

    /**
     * <strong>Goal:</strong> Verify that, by default, {@link Record#set} can
     * still write internal framework state, such as the {@link Record Record's}
     * id, which preserves the historical behavior.
     * <p>
     * <strong>Start state:</strong> A single default {@link Runway} instance,
     * so new {@link Record Records} auto-assign to it.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault}.</li>
     * <li>Call {@code set("id", 12345L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> {@code id()} returns the new value.
     */
    @Test
    public void testSetInternalFieldSucceedsByDefault() {
        Vault vault = new Vault("vault", "code");
        vault.set("id", 12345L);
        Assert.assertEquals(12345L, vault.id());
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy causes
     * {@link Record#set} to refuse a write to the {@link Record Record's}
     * internal id, so a dynamic write cannot rebind the record's identity.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("id", 12345L)}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and {@code id()} returns the original
     * value.
     */
    @Test
    public void testJavaDefaultsRefusesSetOfInternalIdField() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            long id = vault.id();
            assertNonWritable(vault, "id", 12345L);
            Assert.assertEquals(id, vault.id());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy causes
     * {@link Record#set} to refuse a write to internal framework metadata, such
     * as the {@link Record Record's} realm membership.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set("_realms", ...)} with a set of realm names.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and the {@link Vault} belongs to no
     * realms.
     */
    @Test
    public void testJavaDefaultsRefusesSetOfInternalMetadataField()
            throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            assertNonWritable(vault, "_realms", ImmutableSet.of("internal"));
            Assert.assertTrue(vault.realms().isEmpty());
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#set(java.util.Map)}
     * applies each entry in iteration order until the governing policy refuses
     * one, so the entries before the refusal remain applied.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set} with a map that lists the writable
     * {@code description} key before the non-writable final {@code name}
     * key.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException}, the {@code description} entry is
     * applied and the {@code name} field keeps its original value.
     */
    @Test
    public void testSetMapAppliesEntriesBeforePolicyRefusal() throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            try {
                vault.set(ImmutableMap.of("description", "changed", "name",
                        "changed"));
                Assert.fail("Expected a NonWritableFieldException");
            }
            catch (NonWritableFieldException e) {
                Assert.assertEquals("changed", vault.description);
                Assert.assertEquals("vault", vault.name);
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#set(java.util.Map)}
     * stops at a policy refusal, so the entries after the refused entry are not
     * applied.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set} with a map that lists the non-writable final
     * {@code name} key before the writable {@code description} key.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and neither field changes.
     */
    @Test
    public void testSetMapDoesNotApplyEntriesAfterPolicyRefusal()
            throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            try {
                vault.set(ImmutableMap.of("name", "changed", "description",
                        "changed"));
                Assert.fail("Expected a NonWritableFieldException");
            }
            catch (NonWritableFieldException e) {
                Assert.assertEquals("vault", vault.name);
                Assert.assertNull(vault.description);
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that {@link Record#set(java.util.Map)}
     * applies every entry when the governing policy permits all of them.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code set} with a map that contains the public
     * {@code description} field and a {@code nickname} key that does not name a
     * field.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The field holds the new value and the dynamic
     * attribute is stored.
     */
    @Test
    public void testSetMapAppliesAllEntriesWhenPolicyPermits()
            throws Exception {
        try (Runway strict = strictRunway()) {
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            vault.set(ImmutableMap.of("description", "changed", "nickname",
                    "the-vault"));
            Assert.assertEquals("changed", vault.description);
            Assert.assertEquals("the-vault", vault.get("nickname"));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a write made through
     * {@link Audience#write(String, Object, Record)} is governed by the
     * {@link DynamicWritePolicy} of the {@link Record Record's} assigned
     * {@link Runway} instance.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Custodian} to act as the {@link Audience}.</li>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code write("name", "changed", vault)} on the
     * {@link Custodian}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException}, the final field keeps its original
     * value and the {@link Vault} has no author attribution.
     */
    @Test
    public void testAudienceWriteRefusesNonWritableField() throws Exception {
        try (Runway strict = strictRunway()) {
            Custodian custodian = new Custodian();
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            try {
                custodian.write("name", "changed", vault);
                Assert.fail("Expected a NonWritableFieldException");
            }
            catch (NonWritableFieldException e) {
                Assert.assertEquals("vault", vault.name);
                Assert.assertNull(Reflection.get("_author", vault));
            }
        }
    }

    /**
     * <strong>Goal:</strong> Verify that a successful write through
     * {@link Audience#write(String, Object, Record)} still attributes the
     * change to the {@link Audience}.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} that enforces the
     * {@link DynamicWritePolicy#javaDefaults() javaDefaults} policy.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Construct a {@link Custodian} to act as the {@link Audience}.</li>
     * <li>Construct a {@link Vault} and assign it to the strict
     * {@link Runway}.</li>
     * <li>Call {@code write("description", "changed", vault)} on the
     * {@link Custodian}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The public field holds the new value and the
     * {@link Vault Vault's} author is the {@link Custodian}.
     */
    @Test
    public void testAudienceWriteAttributesAuthorOnSuccess() throws Exception {
        try (Runway strict = strictRunway()) {
            Custodian custodian = new Custodian();
            Vault vault = new Vault("vault", "code");
            vault.assign(strict);
            custodian.write("description", "changed", vault);
            Assert.assertEquals("changed", vault.description);
            Assert.assertSame(custodian, Reflection.get("_author", vault));
        }
    }

    /**
     * <strong>Goal:</strong> Verify that the governing
     * {@link DynamicWritePolicy} is available through
     * {@link Runway#properties()}.
     * <p>
     * <strong>Start state:</strong> No prior state needed.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Runway} configured with a specific
     * {@link DynamicWritePolicy}.</li>
     * <li>Read {@code properties().dynamicWritePolicy()} from the built
     * instance and from the default {@code runway}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The built instance returns the configured
     * policy and the default {@code runway} returns a non-null policy.
     */
    @Test
    public void testDynamicWritePolicyIsAvailableThroughProperties()
            throws Exception {
        DynamicWritePolicy policy = DynamicWritePolicy.javaDefaults();
        try (Runway strict = runwayBuilder().dynamicWritePolicy(policy)
                .build()) {
            Assert.assertSame(policy, strict.properties().dynamicWritePolicy());
        }
        Assert.assertNotNull(runway.properties().dynamicWritePolicy());
    }

    /**
     * <strong>Goal:</strong> Verify that replacing the
     * {@link DynamicWritePolicy} through {@link Runway#properties()} governs
     * subsequent {@link Record#set} calls.
     * <p>
     * <strong>Start state:</strong> A {@link Runway} built without a configured
     * policy, so the permissive default governs.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Build a {@link Runway} without configuring a policy.</li>
     * <li>Replace the policy with {@link DynamicWritePolicy#javaDefaults()
     * javaDefaults} via {@code properties().dynamicWritePolicy(...)}.</li>
     * <li>Construct a {@link Vault}, assign it, and call
     * {@code set("name", "changed")}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The call throws
     * {@link NonWritableFieldException} and the field keeps its original value.
     */
    @Test
    public void testDynamicWritePolicyReplacedThroughPropertiesGovernsSet()
            throws Exception {
        try (Runway db = runwayBuilder().build()) {
            db.properties()
                    .dynamicWritePolicy(DynamicWritePolicy.javaDefaults());
            Vault vault = new Vault("vault", "code");
            vault.assign(db);
            assertNonWritable(vault, "name", "changed");
            Assert.assertEquals("vault", vault.name);
        }
    }

    /**
     * A minimal {@link Audience} used to drive writes through the access
     * control framework.
     */
    static class Custodian extends Record implements Audience {}

    /**
     * A {@link Record} that declares one field for each modifier shape that a
     * {@link DynamicWritePolicy} evaluates.
     */
    class Vault extends Record {

        /**
         * A public final identity field.
         */
        public final String name;

        /**
         * A public non-final field.
         */
        public String description;

        /**
         * A private field.
         */
        private String secret;

        /**
         * A protected field.
         */
        protected String owner;

        /**
         * A package-private field.
         */
        String region;

        /**
         * A private final field that is annotated as {@link Writable}.
         */
        @Writable
        private final String code;

        /**
         * Construct a new instance.
         *
         * @param name the vault name
         * @param code the vault code
         */
        public Vault(String name, String code) {
            this.name = name;
            this.code = code;
        }
    }

}
