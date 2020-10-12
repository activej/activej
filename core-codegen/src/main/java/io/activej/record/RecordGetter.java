package io.activej.record;

public interface RecordGetter<T> {
	T get(Record record);

	default int getInt(Record record) {
		throw new UnsupportedOperationException();
	}

	default long getLong(Record record) {
		throw new UnsupportedOperationException();
	}

	default float getFloat(Record record) {
		throw new UnsupportedOperationException();
	}

	default double getDouble(Record record) {
		throw new UnsupportedOperationException();
	}

	RecordScheme getScheme();

	String getField();

	Class<?> getType();
}
