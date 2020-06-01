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

import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Record {
	private final RecordScheme scheme;

	@Nullable
	private final Object[] objects;

	@Nullable
	private final int[] ints;
	@Nullable
	private final double[] doubles;
	@Nullable
	private final long[] longs;
	@Nullable
	private final float[] floats;

	private Record(RecordScheme scheme) {
		this.scheme = scheme;
		this.objects = scheme.objects != 0 ? new Object[scheme.objects] : null;
		this.ints = scheme.ints != 0 ? new int[scheme.ints] : null;
		this.doubles = scheme.doubles != 0 ? new double[scheme.doubles] : null;
		this.longs = scheme.longs != 0 ? new long[scheme.longs] : null;
		this.floats = scheme.floats != 0 ? new float[scheme.floats] : null;
	}

	public static Record create(RecordScheme scheme) {
		return new Record(scheme);
	}

	public RecordScheme getScheme() {
		return scheme;
	}

	private void putRaw(int rawIndex, Object value) {
		int index = rawIndex & 0xFFFF;
		int rawType = rawIndex >>> 16;
		if (rawType == 1 && ints != null) {
			ints[index] = (int) value;
		} else if (rawType == 2 && doubles != null) {
			doubles[index] = (double) value;
		} else if (rawType == 3 && longs != null) {
			longs[index] = (long) value;
		} else if (rawType == 4 && floats != null) {
			floats[index] = (float) value;
		} else if (rawType == 0 && objects != null) {
			objects[index] = value;
		} else {
			throw new IllegalArgumentException("Invalid raw index");
		}
	}

	private Object getRaw(int rawIndex) {
		int index = rawIndex & 0xFFFF;
		int type = rawIndex >>> 16;
		if (type == 1 && ints != null) {
			return ints[index];
		} else if (type == 2 && doubles != null) {
			return doubles[index];
		} else if (type == 3 && longs != null) {
			return longs[index];
		} else if (type == 4 && floats != null) {
			return floats[index];
		} else if (type == 0 && objects != null) {
			return objects[index];
		} else {
			throw new IllegalArgumentException("Invalid raw index");
		}
	}

	public void put(String field, Object value) {
		putRaw(scheme.fieldRawIndices.get(field), value);
	}

	public void put(int field, Object value) {
		putRaw(scheme.rawIndices[field], value);
	}

	public void putAll(Map<String, Object> values) {
		for (Map.Entry<String, Object> entry : values.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	public void putAll(Object[] values) {
		for (int i = 0; i < values.length; i++) {
			put(i, values[i]);
		}
	}

	public Object get(String field) {
		return getRaw(scheme.fieldRawIndices.get(field));
	}

	public Object get(int field) {
		return getRaw(scheme.rawIndices[field]);
	}

	public Map<String, Object> asMap() {
		Map<String, Object> result = new LinkedHashMap<>(scheme.rawIndices.length * 2);
		getInto(result);
		return result;
	}

	public Object[] asArray() {
		Object[] result = new Object[scheme.rawIndices.length];
		getInto(result);
		return result;
	}

	public void getInto(Map<String, Object> result) {
		for (int i = 0; i < scheme.rawIndices.length; i++) {
			result.put(scheme.fields[i], get(i));
		}
	}

	public void getInto(Object[] result) {
		for (int i = 0; i < scheme.rawIndices.length; i++) {
			result[i] = get(i);
		}
	}

	@Override
	public String toString() {
		return asMap().toString();
	}
}
