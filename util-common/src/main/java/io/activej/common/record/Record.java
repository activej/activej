package io.activej.common.record;

import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Record {
	private final RecordScheme scheme;

	@Nullable
	protected final Object[] objects;
	@Nullable
	protected final int[] ints;

	private Record(RecordScheme scheme) {
		this.scheme = scheme;
		this.objects = scheme.objects != 0 ? new Object[scheme.objects] : null;
		this.ints = scheme.ints != 0 ? new int[scheme.ints] : null;
	}

	public static Record create(RecordScheme scheme) {
		return new Record(scheme);
	}

	public static Record of(RecordScheme scheme, Object... values) {
		return scheme.recordOf(values);
	}

	public RecordScheme getScheme() {
		return scheme;
	}

	protected void putRaw(int rawIndex, Object value) {
		int index = RecordScheme.unpackIndex(rawIndex);
		int rawType = RecordScheme.unpackType(rawIndex);
		switch (rawType) {
			case RecordScheme.INTERNAL_OBJECT: {
				objects[index] = value;
				break;
			}
			case RecordScheme.INTERNAL_BOOLEAN: {
				byte v = (boolean) (Boolean) value ? (byte) 1 : (byte) 0;
				int shift = (index & 3) * 8;
				ints[index >>> 2] = (ints[index >>> 2] & ~(0xFF << shift)) | (v << shift);
				break;
			}
			case RecordScheme.INTERNAL_CHAR: {
				int v = (Character) value;
				int shift = (index & 3) * 8;
				ints[index >>> 2] = (ints[index >>> 2] & ~(0xFFFF << shift)) | (v << shift);
				break;
			}
			case RecordScheme.INTERNAL_BYTE: {
				byte v = (Byte) value;
				int shift = (index & 3) * 8;
				ints[index >>> 2] = (ints[index >>> 2] & ~(0xFF << shift)) | (v << shift);
				break;
			}
			case RecordScheme.INTERNAL_SHORT: {
				short v = (Short) value;
				int shift = (index & 3) * 8;
				ints[index >>> 2] = (ints[index >>> 2] & ~(0xFFFF << shift)) | (v << shift);
				break;
			}
			case RecordScheme.INTERNAL_INT: {
				ints[index] = (Integer) value;
				break;
			}
			case RecordScheme.INTERNAL_LONG: {
				long v = (Long) value;
				ints[index] = (int) (v);
				ints[index + 1] = (int) (v >>> 32);
				break;
			}
			case RecordScheme.INTERNAL_FLOAT: {
				ints[index] = Float.floatToIntBits((Float) value);
				break;
			}
			case RecordScheme.INTERNAL_DOUBLE: {
				long v = Double.doubleToLongBits((Double) value);
				ints[index] = (int) (v);
				ints[index + 1] = (int) (v >>> 32);
				break;
			}
		}
	}

	protected Object getRaw(int rawIndex) {
		int index = rawIndex & 0xFFFF;
		int rawType = rawIndex >>> 16;
		switch (rawType & 0xFF) {
			case RecordScheme.INTERNAL_OBJECT: {
				return objects[index];
			}
			case RecordScheme.INTERNAL_BOOLEAN: {
				int shift = (index & 3) * 8;
				return ints[index >>> 2] >>> shift != 0;
			}
			case RecordScheme.INTERNAL_CHAR: {
				int shift = (index & 3) * 8;
				return (char) (short) (ints[index >>> 2] >>> shift);
			}
			case RecordScheme.INTERNAL_BYTE: {
				int shift = (index & 3) * 8;
				return (byte) (ints[index >>> 2] >>> shift);
			}
			case RecordScheme.INTERNAL_SHORT: {
				int shift = (index & 3) * 8;
				return (short) (ints[index >>> 2] >>> shift);
			}
			case RecordScheme.INTERNAL_INT: {
				return ints[index];
			}
			case RecordScheme.INTERNAL_LONG: {
				return (ints[index] & 0xFFFFFFFFL) | ((ints[index + 1] & 0xFFFFFFFFL) << 32);
			}
			case RecordScheme.INTERNAL_FLOAT: {
				return Float.intBitsToFloat(ints[index]);
			}
			case RecordScheme.INTERNAL_DOUBLE: {
				return Double.longBitsToDouble((ints[index] & 0xFFFFFFFFL) | ((ints[index + 1] & 0xFFFFFFFFL) << 32));
			}
			default:
				throw new IllegalArgumentException("Invalid raw index: " + Long.toString(rawIndex, 16));
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
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Record record = (Record) o;
		//noinspection PointlessBooleanExpression
		return true &&
				Arrays.equals(ints, record.ints) &&
				Arrays.equals(objects, record.objects);
	}

	@Override
	public int hashCode() {
		int result = 0;
		result = 31 * result + Arrays.hashCode(ints);
		result = 31 * result + Arrays.hashCode(objects);
		return result;
	}

	@Override
	public String toString() {
		return asMap().toString();
	}
}
