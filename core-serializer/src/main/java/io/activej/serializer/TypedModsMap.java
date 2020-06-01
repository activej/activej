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

package io.activej.serializer;

import io.activej.serializer.impl.SerializerDefBuilder;
import io.activej.serializer.impl.SerializerDefBuilder.SerializerForType;

import java.util.*;

final class TypedModsMap {
	private static final TypedModsMap EMPTY = new TypedModsMap();

	private final List<SerializerDefBuilder> mods;
	private final Map<Integer, TypedModsMap> children;

	public static class Builder {
		private final List<SerializerDefBuilder> mods = new ArrayList<>();
		private final Map<Integer, Builder> children = new LinkedHashMap<>();

		public void add(SerializerDefBuilder serializerDefBuilder) {
			mods.add(serializerDefBuilder);
		}

		private Builder ensureChild(int childKey) {
			Builder result = children.get(childKey);
			if (result == null) {
				result = new Builder();
				children.put(childKey, result);
			}
			return result;
		}

		public Builder ensureChild(int[] path) {
			Builder result = this;
			for (int n : path) {
				result = result.ensureChild(n);
			}
			return result;
		}

		public TypedModsMap build() {
			if (mods.isEmpty() && children.isEmpty())
				return empty();
			return new TypedModsMap(this);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static TypedModsMap empty() {
		return EMPTY;
	}

	private TypedModsMap() {
		this.mods = new ArrayList<>();
		this.children = new HashMap<>();
	}

	private TypedModsMap(Builder builder) {
		this.mods = new ArrayList<>(builder.mods);
		Map<Integer, TypedModsMap> childrenBuilder = new HashMap<>();
		for (Integer key : builder.children.keySet()) {
			childrenBuilder.put(key, new TypedModsMap(builder.children.get(key)));
		}
		this.children = childrenBuilder;
	}

	public boolean hasChildren() {
		return !children.isEmpty();
	}

	public List<SerializerDefBuilder> getMods() {
		return mods;
	}

	public boolean isEmpty() {
		return children.isEmpty() && mods.isEmpty();
	}

	public TypedModsMap get(int typeIndex) {
		TypedModsMap result = children.get(typeIndex);
		return result == null ? empty() : result;
	}

	public SerializerDef rewrite(Class<?> type, SerializerForType[] generics, SerializerDef serializer) {
		SerializerDef result = serializer;
		for (SerializerDefBuilder mod : mods) {
			result = mod.serializer(type, generics, result);
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(32).append(simpleName(getClass())).append("{");
		sb.append("mods=").append(mods);
		sb.append("children=").append(children);
		return sb.append("}").toString();
	}

	private static String simpleName(Class<?> clazz) {
		String name = clazz.getName();
		name = name.replaceAll("\\$[0-9]+", "\\$");
		int start = name.lastIndexOf('$');
		if (start == -1) {
			start = name.lastIndexOf('.');
		}
		return name.substring(start + 1);
	}
}
