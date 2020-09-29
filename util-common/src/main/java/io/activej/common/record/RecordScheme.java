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

package io.activej.common.record;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.joining;

@SuppressWarnings("unused")
public final class RecordScheme {
	protected static final int INTERNAL_OBJECT = 0;
	protected static final int INTERNAL_BOOLEAN = 1;
	protected static final int INTERNAL_CHAR = 2;
	protected static final int INTERNAL_BYTE = 3;
	protected static final int INTERNAL_SHORT = 4;
	protected static final int INTERNAL_INT = 5;
	protected static final int INTERNAL_LONG = 6;
	protected static final int INTERNAL_FLOAT = 7;
	protected static final int INTERNAL_DOUBLE = 8;

	protected final LinkedHashMap<String, Type> fieldTypes = new LinkedHashMap<>();

	protected int objects;
	protected int bytes;
	protected int ints;

	final Map<String, Integer> fieldIndices = new HashMap<>();
	final Map<String, Integer> fieldRawIndices = new HashMap<>();
	String[] fields = {};
	int[] rawIndices = {};

	private RecordScheme() {
	}

	public static RecordScheme create() {
		return new RecordScheme();
	}

	public Record recordOf(Object... values) {
		checkArgument(values.length == rawIndices.length);
		Record record = Record.create(this);
		for (int i = 0; i < values.length; i++) {
			record.putRaw(rawIndices[i], values[i]);
		}
		return record;
	}

	@SuppressWarnings("UnusedReturnValue")
	public RecordScheme withField(@NotNull String field, @NotNull Class<?> type) {
		addField(field, type);
		return this;
	}

	public void addField(@NotNull String field, @NotNull Type type) {
		fieldTypes.put(field, type);
		fields = Arrays.copyOf(fields, fields.length + 1);
		fields[fields.length - 1] = field;
		int rawIndex;
		if (type == boolean.class) {
			rawIndex = pack(INTERNAL_BOOLEAN, reserveBytes(1));
		} else if (type == char.class) {
			rawIndex = pack(INTERNAL_CHAR, reserveBytes(2));
		} else if (type == byte.class) {
			rawIndex = pack(INTERNAL_BYTE, reserveBytes(1));
		} else if (type == short.class) {
			rawIndex = pack(INTERNAL_SHORT, reserveBytes(2));
		} else if (type == int.class) {
			rawIndex = pack(INTERNAL_INT, ints++);
		} else if (type == long.class) {
			rawIndex = pack(INTERNAL_LONG, ints);
			ints += 2;
		} else if (type == float.class) {
			rawIndex = pack(INTERNAL_FLOAT, ints++);
		} else if (type == double.class) {
			rawIndex = pack(INTERNAL_DOUBLE, ints);
			ints += 2;
		} else {
			rawIndex = objects++;
		}
		fieldIndices.put(field, fieldIndices.size());
		fieldRawIndices.put(field, rawIndex);
		rawIndices = Arrays.copyOf(rawIndices, rawIndices.length + 1);
		rawIndices[rawIndices.length - 1] = rawIndex;
	}

	static int pack(int type, int index) {
		return (type << 16) + index;
	}

	static int unpackType(int raw) {
		return raw >>> 16;
	}

	static int unpackIndex(int raw) {
		return raw & 0xFFFF;
	}

	private int reserveBytes(int count) {
		assert ints * 4 >= bytes;
		int offset = this.bytes & 3;

		if (offset + count > 4) {
			bytes = ints * 4;
		}

		int result = bytes;
		bytes += count;

		if (!(ints * 4 >= bytes)) {
			ints++;
		}
		return result;
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

	@Override
	public String toString() {
		return fieldTypes.entrySet().stream()
				.map(entry -> entry.getKey() + "=" +
						(entry.getValue() instanceof Class ? ((Class<?>) entry.getValue()).getSimpleName() : entry.getValue()))
				.collect(joining(", ", "{", "}"));
	}
}
