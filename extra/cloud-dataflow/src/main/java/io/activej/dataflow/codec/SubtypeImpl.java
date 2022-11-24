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

package io.activej.dataflow.codec;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.util.Objects;

@SuppressWarnings("ClassExplicitlyAnnotation")
public final class SubtypeImpl implements Subtype {
	private final int value;

	private SubtypeImpl(int value) {
		this.value = value;
	}

	public static Subtype subtype(int index) {
		return new SubtypeImpl(index);
	}

	@Override
	public int value() {
		return value;
	}

	@Override
	public int hashCode() {
		// This is specified in java.lang.Annotation.
		return ((127 * "value".hashCode()) ^ Integer.valueOf(value).hashCode());
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Subtype other)) return false;

		return Objects.equals(value, other.value());
	}

	@Override
	public @NotNull String toString() {
		return "@" + Subtype.class.getName() + "(value=" + value + ")";
	}

	@Override
	public @NotNull Class<? extends Annotation> annotationType() {
		return Subtype.class;
	}
}
