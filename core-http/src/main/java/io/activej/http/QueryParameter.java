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

package io.activej.http;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Plain POJO for the query parameter key-value pair.
 */
public final class QueryParameter {
	private final String key;
	private final String value;

	QueryParameter(String key, @Nullable String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public @Nullable String getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		QueryParameter queryParameter = (QueryParameter) o;
		return
			Objects.equals(key, queryParameter.key) &&
			Objects.equals(value, queryParameter.value);
	}

	@Override
	public int hashCode() {
		int result = key != null ? key.hashCode() : 0;
		result = 31 * result + (value != null ? value.hashCode() : 0);
		return result;
	}
}
