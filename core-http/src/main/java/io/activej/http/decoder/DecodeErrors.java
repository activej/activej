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

import io.activej.common.initializer.WithInitializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

/**
 * A tree of https errors. A tree structure matches the structure of the decoder it was received from
 */
public final class DecodeErrors implements WithInitializer<DecodeErrors> {
	private static final String DEFAULT_SEPARATOR = ".";

	private @Nullable List<DecodeError> errors;
	private @Nullable Map<String, DecodeErrors> children;

	private DecodeErrors() {
	}

	public static DecodeErrors create() {
		return new DecodeErrors();
	}

	public static DecodeErrors of(String message, Object... args) {
		return create().with(DecodeError.of(message, args));
	}

	public static DecodeErrors of(@NotNull DecodeError error) {
		return create().with(error);
	}

	public static DecodeErrors of(@NotNull List<DecodeError> errors) {
		return create().with(errors);
	}

	public DecodeErrors with(@NotNull DecodeError error) {
		if (this.errors == null) this.errors = new ArrayList<>();
		this.errors.add(error);
		return this;
	}

	public DecodeErrors with(@NotNull List<DecodeError> errors) {
		if (this.errors == null) this.errors = new ArrayList<>();
		this.errors.addAll(errors);
		return this;
	}

	public DecodeErrors with(@NotNull String id, @NotNull DecodeErrors nestedError) {
		if (children == null) children = new HashMap<>();
		children.merge(id, nestedError, DecodeErrors::merge);
		return this;
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

	public DecodeErrors with(@NotNull String id, @NotNull DecodeError nestedError) {
		if (children == null) children = new HashMap<>();
		children.computeIfAbsent(id, $ -> new DecodeErrors()).with(nestedError);
		return this;
	}

	public boolean hasErrors() {
		return errors != null || children != null;
	}

	public @NotNull List<DecodeError> getErrors() {
		return errors != null ? errors : emptyList();
	}

	public @NotNull Set<String> getChildren() {
		return children != null ? children.keySet() : emptySet();
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

	private static void toMultimapImpl(DecodeErrors errors,
			Map<String, List<String>> multimap, String prefix,
			BiFunction<String, Object[], String> formatter,
			String separator) {
		if (errors.errors != null) {
			multimap.put(prefix, errors.errors.stream().map(error -> formatter.apply(error.message, error.getArgs())).collect(toList()));
		}
		if (errors.children != null) {
			errors.children.forEach((id, child) -> toMultimapImpl(child, multimap, (prefix.isEmpty() ? "" : prefix + separator) + id, formatter, separator));
		}
	}

	private static void toMapImpl(DecodeErrors errors,
			Map<String, String> map, String prefix,
			BiFunction<String, Object[], String> formatter,
			String separator) {
		if (errors.errors != null) {
			DecodeError error = errors.errors.get(0);
			map.put(prefix, formatter.apply(error.message, error.getArgs()));
		}
		if (errors.children != null) {
			errors.children.forEach((id, child) -> toMapImpl(child, map, (prefix.isEmpty() ? "" : prefix + separator) + id, formatter, separator));
		}
	}
}
