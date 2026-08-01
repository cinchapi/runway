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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import javax.annotation.concurrent.Immutable;

/**
 * A {@link DynamicWritePolicy} governs which fields
 * {@link Record#set(String, Object)} &mdash; the dynamic write surface of a
 * {@link Record} &mdash; is permitted to write.
 * <p>
 * A field's writability is judged along two axes: <em>finality</em> (whether
 * the field is {@code final}) and <em>visibility</em> (whether the field is
 * {@code private}, package-private or {@code protected}). A public non-final
 * field is always writable. Each other trait must be explicitly {@link Builder
 * allowed} for a field bearing it to be writable.
 * </p>
 * <p>
 * The {@link #permissive()} policy allows every trait and therefore permits a
 * dynamic write to reach any field. The {@link #javaDefaults()} policy allows
 * no traits and therefore respects the intent of the Java modifiers: only
 * public non-final fields are writable. A custom mix of allowances can be
 * assembled with the {@link #builder()}.
 * </p>
 * <p>
 * A field annotated as {@link Writable} is always writable, regardless of the
 * policy's rules. A policy only governs writes made through
 * {@link Record#set(String, Object)}; it never affects the framework's ability
 * to populate fields when a {@link Record} is loaded from the database.
 * </p>
 *
 * @author Jeff Nelson
 */
@Immutable
public final class DynamicWritePolicy {

    /**
     * Return a {@link Builder} for a {@link DynamicWritePolicy}. The builder
     * starts with no allowances, so, absent any customization, it
     * {@link Builder#build() builds} a policy equal to {@link #javaDefaults()}.
     *
     * @return a {@link DynamicWritePolicy} builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Return a {@link DynamicWritePolicy} that respects the intent of the Java
     * modifiers: only public non-final fields (and fields annotated as
     * {@link Writable}) can be written.
     *
     * @return the java defaults {@link DynamicWritePolicy}
     */
    public static DynamicWritePolicy javaDefaults() {
        return JAVA_DEFAULTS;
    }

    /**
     * Return a {@link DynamicWritePolicy} that permits a dynamic write to reach
     * any field, regardless of visibility or finality. This is the default
     * policy for every {@link Runway} instance.
     *
     * @return the permissive {@link DynamicWritePolicy}
     */
    public static DynamicWritePolicy permissive() {
        return PERMISSIVE;
    }

    /**
     * The {@link DynamicWritePolicy} returned from {@link #javaDefaults()}.
     */
    private static final DynamicWritePolicy JAVA_DEFAULTS = new Builder()
            .build();

    /**
     * The {@link DynamicWritePolicy} returned from {@link #permissive()}.
     */
    private static final DynamicWritePolicy PERMISSIVE = new Builder()
            .allowFinalFields().allowPrivateFields().allowPackagePrivateFields()
            .allowProtectedFields().build();

    /**
     * A flag that indicates if this policy allows writes to final fields.
     */
    private final boolean allowsFinalFields;

    /**
     * A flag that indicates if this policy allows writes to package-private
     * fields.
     */
    private final boolean allowsPackagePrivateFields;

    /**
     * A flag that indicates if this policy allows writes to private fields.
     */
    private final boolean allowsPrivateFields;

    /**
     * A flag that indicates if this policy allows writes to protected fields.
     */
    private final boolean allowsProtectedFields;

    /**
     * Construct a new instance.
     *
     * @param allowsFinalFields whether writes to final fields are allowed
     * @param allowsPrivateFields whether writes to private fields are allowed
     * @param allowsPackagePrivateFields whether writes to package-private
     *            fields are allowed
     * @param allowsProtectedFields whether writes to protected fields are
     *            allowed
     */
    private DynamicWritePolicy(boolean allowsFinalFields,
            boolean allowsPrivateFields, boolean allowsPackagePrivateFields,
            boolean allowsProtectedFields) {
        this.allowsFinalFields = allowsFinalFields;
        this.allowsPrivateFields = allowsPrivateFields;
        this.allowsPackagePrivateFields = allowsPackagePrivateFields;
        this.allowsProtectedFields = allowsProtectedFields;
    }

    /**
     * Return {@code true} if this policy permits a dynamic write to
     * {@code field}.
     * <p>
     * A field annotated as {@link Writable} is always writable, regardless of
     * this policy's rules.
     * </p>
     *
     * @param field the {@link Field} to evaluate
     * @return {@code true} if {@code field} can be written
     */
    public boolean isWritable(Field field) {
        if(field.isAnnotationPresent(Writable.class)) {
            return true;
        }
        else {
            int modifiers = field.getModifiers();
            if(Modifier.isFinal(modifiers) && !allowsFinalFields) {
                return false;
            }
            else if(Modifier.isPrivate(modifiers) && !allowsPrivateFields) {
                return false;
            }
            else if(Modifier.isProtected(modifiers) && !allowsProtectedFields) {
                return false;
            }
            else if(!Modifier.isPublic(modifiers)
                    && !Modifier.isPrivate(modifiers)
                    && !Modifier.isProtected(modifiers)
                    && !allowsPackagePrivateFields) {
                return false;
            }
            else {
                return true;
            }
        }
    }

    /**
     * A builder for a {@link DynamicWritePolicy}. The builder starts with no
     * allowances; each {@code allow*} method grants writability to fields with
     * the corresponding trait.
     *
     * @author Jeff Nelson
     */
    public static class Builder {

        /**
         * Whether the built policy allows writes to final fields.
         */
        private boolean allowFinalFields = false;

        /**
         * Whether the built policy allows writes to package-private fields.
         */
        private boolean allowPackagePrivateFields = false;

        /**
         * Whether the built policy allows writes to private fields.
         */
        private boolean allowPrivateFields = false;

        /**
         * Whether the built policy allows writes to protected fields.
         */
        private boolean allowProtectedFields = false;

        /**
         * Allow writes to final fields.
         *
         * @return this builder
         */
        public Builder allowFinalFields() {
            this.allowFinalFields = true;
            return this;
        }

        /**
         * Allow writes to package-private fields.
         *
         * @return this builder
         */
        public Builder allowPackagePrivateFields() {
            this.allowPackagePrivateFields = true;
            return this;
        }

        /**
         * Allow writes to private fields.
         *
         * @return this builder
         */
        public Builder allowPrivateFields() {
            this.allowPrivateFields = true;
            return this;
        }

        /**
         * Allow writes to protected fields.
         *
         * @return this builder
         */
        public Builder allowProtectedFields() {
            this.allowProtectedFields = true;
            return this;
        }

        /**
         * Build the configured {@link DynamicWritePolicy} and return the
         * instance.
         *
         * @return a {@link DynamicWritePolicy}
         */
        public DynamicWritePolicy build() {
            return new DynamicWritePolicy(allowFinalFields, allowPrivateFields,
                    allowPackagePrivateFields, allowProtectedFields);
        }

    }

}
