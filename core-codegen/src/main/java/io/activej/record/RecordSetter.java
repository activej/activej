package io.activej.record;

import java.lang.reflect.Type;

public interface RecordSetter<T> {
	void set(Record record, T value);

	default void setInt(Record record, int value) {
		throw new UnsupportedOperationException();
	}

	default void setLong(Record record, long value) {
		throw new UnsupportedOperationException();
	}

	default void setFloat(Record record, float value) {
		throw new UnsupportedOperationException();
	}

	default void setDouble(Record record, double value) {
		throw new UnsupportedOperationException();
	}

	RecordScheme getScheme();

	String getField();

	Type getType();
}
