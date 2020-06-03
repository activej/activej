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

package io.activej.cube;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.*;

public final class RecordScheme {
	private final LinkedHashMap<String, Type> fieldTypes = new LinkedHashMap<>();

	int objects;
	int ints;
	int doubles;
	int longs;
	int floats;

	final Map<String, Integer> fieldIndices = new HashMap<>();
	final Map<String, Integer> fieldRawIndices = new HashMap<>();
	String[] fields = {};
	int[] rawIndices = {};

	private RecordScheme() {
	}

	public static RecordScheme create() {
		return new RecordScheme();
	}

	public RecordScheme withField(@NotNull String field, @NotNull Type type) {
		addField(field, type);
		return this;
	}

	public void addField(@NotNull String field, @NotNull Type type) {
		fieldTypes.put(field, type);
		fields = Arrays.copyOf(fields, fields.length + 1);
		fields[fields.length - 1] = field;
		int rawIndex;
		if (type == int.class) {
			rawIndex = (1 << 16) + ints;
			ints++;
		} else if (type == double.class) {
			rawIndex = (2 << 16) + doubles;
			doubles++;
		} else if (type == long.class) {
			rawIndex = (3 << 16) + longs;
			longs++;
		} else if (type == float.class) {
			rawIndex = (4 << 16) + floats;
			floats++;
		} else {
			rawIndex = objects;
			objects++;
		}
		fieldIndices.put(field, fieldIndices.size());
		fieldRawIndices.put(field, rawIndex);
		rawIndices = Arrays.copyOf(rawIndices, rawIndices.length + 1);
		rawIndices[rawIndices.length - 1] = rawIndex;
	}

	public void addFields(Map<String, Class<?>> types) {
		for (Map.Entry<String, Class<?>> entry : types.entrySet()) {
			addField(entry.getKey(), entry.getValue());
		}
	}

	public List<String> getFields() {
		return new ArrayList<>(fieldTypes.keySet());
	}

	public String getField(int index) {
		return fields[index];
	}

	public Type getFieldType(String field) {
		return fieldTypes.get(field);
	}

	public int getFieldIndex(String field) {
		return fieldIndices.get(field);
	}
}
