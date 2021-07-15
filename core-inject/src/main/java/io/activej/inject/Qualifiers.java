/*
 * Copyright (C) 2020 ActiveJ LLC.
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

package io.activej.inject;

import io.activej.inject.annotation.Named;
import io.activej.inject.module.UniqueQualifierImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

/**
 * This class holds utility methods used for validating and creating objects used as qualifiers.
 * Qualifiers serve as additional tags to distinguish different {@link Key keys} that have same type.
 * <p>
 */
public final class Qualifiers {
	/**
	 * A shortcut for creating qualifiers based on {@link Named} built-in annotation.
	 */
	public static Object named(String name) {
		return new NamedImpl(name);
	}

	public static Object uniqueQualifier() {
		return new UniqueQualifierImpl();
	}

	public static Object uniqueQualifier(@Nullable Object qualifier) {
		return qualifier instanceof UniqueQualifierImpl ? qualifier : new UniqueQualifierImpl(qualifier);
	}

	public static boolean isNamed(@Nullable Object qualifier) {
		return qualifier instanceof NamedImpl;
	}

	public static boolean isUnique(@Nullable Object qualifier) {
		return qualifier instanceof UniqueQualifierImpl;
	}

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static final class NamedImpl implements Named {
		private static final int VALUE_HASHCODE = 127 * "value".hashCode();

		@NotNull
		private final String value;

		NamedImpl(@NotNull String value) {
			this.value = value;
		}

		@NotNull
		@Override
		public String value() {
			return value;
		}

		@Override
		public int hashCode() {
			// This is specified in java.lang.Annotation.
			return VALUE_HASHCODE ^ value.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Named)) return false;

			Named other = (Named) o;
			return value.equals(other.value());
		}

		@NotNull
		@Override
		public String toString() {
			return "@" + Named.class.getName() + "(" + value + ")";
		}

		@NotNull
		@Override
		public Class<? extends Annotation> annotationType() {
			return Named.class;
		}
	}
}
