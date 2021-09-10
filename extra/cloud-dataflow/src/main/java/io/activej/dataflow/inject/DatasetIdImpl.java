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

package io.activej.dataflow.inject;

import io.activej.inject.Key;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;

@SuppressWarnings("ClassExplicitlyAnnotation")
public final class DatasetIdImpl implements DatasetId {
	private final @NotNull String value;

	private DatasetIdImpl(@NotNull String value) {
		this.value = value;
	}

	public static Key<Object> datasetId(String id) {
		return Key.of(Object.class, new DatasetIdImpl(id));
	}

	@Override
	public @NotNull String value() {
		return value;
	}

	@Override
	public int hashCode() {
		// This is specified in java.lang.Annotation.
		return (127 * "value".hashCode()) ^ value.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof DatasetId)) return false;

		DatasetId other = (DatasetId) o;
		return value.equals(other.value());
	}

	@Override
	public @NotNull String toString() {
		return "@" + DatasetId.class.getName() + "(" + value + ")";
	}

	@Override
	public @NotNull Class<? extends Annotation> annotationType() {
		return DatasetId.class;
	}
}
