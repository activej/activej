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

package io.activej.http.decoder;

import io.activej.common.builder.AbstractBuilder;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;

/**
 * A tree of https errors. A tree structure matches the structure of the decoder it was received from
 */
public final class DecodeErrors {
	private static final String DEFAULT_SEPARATOR = ".";

	private @Nullable List<DecodeError> errors;
	private @Nullable Map<String, DecodeErrors> children;

	private DecodeErrors() {
	}

	public static DecodeErrors create() {
		return builder().build();
	}

	public static DecodeErrors of(String message, Object... args) {
		return builder()
			.with(DecodeError.of(message, args))
			.build();
	}

	public static DecodeErrors of(DecodeError error) {
		return builder()
			.with(error)
			.build();
	}

	public static DecodeErrors of(List<DecodeError> errors) {
		return builder()
			.with(errors)
			.build();
	}

	public static Builder builder() {
		return new DecodeErrors().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, DecodeErrors> {
		private Builder() {}

		public Builder with(DecodeError error) {
			checkNotBuilt(this);
			setError(error);
			return this;
		}

		public Builder with(List<DecodeError> errors) {
			checkNotBuilt(this);
			if (DecodeErrors.this.errors == null) DecodeErrors.this.errors = new ArrayList<>();
			DecodeErrors.this.errors.addAll(errors);
			return this;
		}

		public Builder with(String id, DecodeErrors nestedError) {
			checkNotBuilt(this);
			if (children == null) children = new HashMap<>();
			children.merge(id, nestedError, DecodeErrors::merge);
			return this;
		}

		public Builder with(String id, DecodeError nestedError) {
			checkNotBuilt(this);
			if (children == null) children = new HashMap<>();
			children.computeIfAbsent(id, $ -> new DecodeErrors()).setError(nestedError);
			return this;
		}

		@Override
		protected DecodeErrors doBuild() {
			return DecodeErrors.this;
		}
	}

	public DecodeErrors merge(DecodeErrors another) {
		if (another.errors != null) {
			if (this.errors == null) {
				this.errors = new ArrayList<>(another.errors);
			} else {
				this.errors.addAll(another.errors);
			}
		}
		if (another.children != null) {
			if (this.children == null) {
				this.children = new HashMap<>(another.children);
			} else {
				for (String key : another.children.keySet()) {
					this.children.merge(key, another.children.get(key), DecodeErrors::merge);
				}
			}
		}
		return this;
	}

	public boolean hasErrors() {
		return errors != null || children != null;
	}

	public List<DecodeError> getErrors() {
		return errors != null ? errors : List.of();
	}

	public Set<String> getChildren() {
		return children != null ? children.keySet() : Set.of();
	}

	public DecodeErrors getChild(String id) {
		return children != null ? children.get(id) : null;
	}

	public Map<String, String> toMap() {
		return toMap(String::format);
	}

	public Map<String, String> toMap(String separator) {
		return toMap(String::format, separator);
	}

	public Map<String, List<String>> toMultimap() {
		return toMultimap(String::format);
	}

	public Map<String, List<String>> toMultimap(String separator) {
		return toMultimap(String::format, separator);
	}

	public Map<String, String> toMap(BiFunction<String, Object[], String> formatter) {
		return toMap(formatter, DEFAULT_SEPARATOR);
	}

	public Map<String, String> toMap(BiFunction<String, Object[], String> formatter, String separator) {
		Map<String, String> map = new HashMap<>();
		toMapImpl(this, map, "", formatter, separator);
		return map;
	}

	public Map<String, List<String>> toMultimap(BiFunction<String, Object[], String> formatter, String separator) {
		Map<String, List<String>> multimap = new HashMap<>();
		toMultimapImpl(this, multimap, "", formatter, separator);
		return multimap;
	}

	public Map<String, List<String>> toMultimap(BiFunction<String, Object[], String> formatter) {
		return toMultimap(formatter, DEFAULT_SEPARATOR);
	}

	private static void toMultimapImpl(
		DecodeErrors errors, Map<String, List<String>> multimap, String prefix,
		BiFunction<String, Object[], String> formatter, String separator
	) {
		if (errors.errors != null) {
			multimap.put(prefix, errors.errors.stream().map(error -> formatter.apply(error.message, error.getArgs())).collect(toList()));
		}
		if (errors.children != null) {
			errors.children.forEach((id, child) -> toMultimapImpl(child, multimap, (prefix.isEmpty() ? "" : prefix + separator) + id, formatter, separator));
		}
	}

	private static void toMapImpl(
		DecodeErrors errors, Map<String, String> map, String prefix, BiFunction<String, Object[], String> formatter,
		String separator
	) {
		if (errors.errors != null) {
			DecodeError error = errors.errors.get(0);
			map.put(prefix, formatter.apply(error.message, error.getArgs()));
		}
		if (errors.children != null) {
			errors.children.forEach((id, child) -> toMapImpl(child, map, (prefix.isEmpty() ? "" : prefix + separator) + id, formatter, separator));
		}
	}

	private void setError(DecodeError error) {
		if (errors == null) errors = new ArrayList<>();
		errors.add(error);
	}
}
