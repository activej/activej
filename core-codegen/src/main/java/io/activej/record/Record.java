package io.activej.record;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class Record {
	private final RecordScheme scheme;

	protected Record(RecordScheme scheme) {
		this.scheme = scheme;
	}

	public final RecordScheme getScheme() {
		return scheme;
	}

	public Object get(String field) {
		return scheme.get(this, field);
	}

	public Object get(int field) {
		return scheme.get(this, field);
	}

	public int getInt(String field) {
		return scheme.getInt(this, field);
	}

	public int getInt(int field) {
		return scheme.getInt(this, field);
	}

	public long getLong(String field) {
		return scheme.getLong(this, field);
	}

	public long getLong(int field) {
		return scheme.getLong(this, field);
	}

	public float getFloat(String field) {
		return scheme.getFloat(this, field);
	}

	public float getFloat(int field) {
		return scheme.getFloat(this, field);
	}

	public double getDouble(String field) {
		return scheme.getDouble(this, field);
	}

	public double getDouble(int field) {
		return scheme.getDouble(this, field);
	}

	public void set(String field, Object value) {
		scheme.set(this, field, value);
	}

	public void set(int field, Object value) {
		scheme.set(this, field, value);
	}

	public void setInt(String field, int value) {
		scheme.setInt(this, field, value);
	}

	public void setInt(int field, int value) {
		scheme.setInt(this, field, value);
	}

	public void setLong(String field, long value) {
		scheme.setLong(this, field, value);
	}

	public void setLong(int field, long value) {
		scheme.setLong(this, field, value);
	}

	public void setFloat(String field, float value) {
		scheme.setFloat(this, field, value);
	}

	public void setFloat(int field, float value) {
		scheme.setFloat(this, field, value);
	}

	public void setDouble(String field, double value) {
		scheme.setDouble(this, field, value);
	}

	public void setDouble(int field, double value) {
		scheme.setDouble(this, field, value);
	}

	public Map<String, Object> toMap() {
		Map<String, Object> result = new LinkedHashMap<>(scheme.size() * 2);
		toMap(result);
		return result;
	}

	public Object[] toArray() {
		Object[] result = new Object[scheme.size()];
		toArray(result);
		return result;
	}

	public Map<String, Object> toMap(Map<String, Object> result) {
		for (int i = 0; i < scheme.size(); i++) {
			result.put(scheme.fields[i], get(i));
		}
		return result;
	}

	public Object[] toArray(Object[] result) {
		for (int i = 0; i < result.length; i++) {
			result[i] = get(i);
		}
		return result;
	}

	public void setMap(Map<String, Object> values) {
		for (Map.Entry<String, Object> entry : values.entrySet()) {
			set(entry.getKey(), entry.getValue());
		}
	}

	public void setArray(Object[] values) {
		for (int i = 0; i < values.length; i++) {
			set(i, values[i]);
		}
	}

	@Override
	public String toString() {
		return toMap().toString();
	}
}
