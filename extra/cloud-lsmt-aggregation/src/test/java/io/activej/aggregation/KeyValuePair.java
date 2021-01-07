package io.activej.aggregation;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Let's first define a class that will hold our key-value pair.
 * This class also contains 'timestamp' field, so that AggregationDB will return the latest entry for the particular key.
 */
public class KeyValuePair {
	public int key;
	public int value;
	public long timestamp;

	public KeyValuePair() {
	}

	public KeyValuePair(int key, int value, long timestamp) {
		this.key = key;
		this.value = value;
		this.timestamp = timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KeyValuePair that = (KeyValuePair) o;

		if (key != that.key) return false;
		if (value != that.value) return false;
		return timestamp == that.timestamp;

	}

	@Override
	public int hashCode() {
		int result = key;
		result = 31 * result + value;
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
	public static final List<String> KEYS = asList("key");

	public static final List<String> FIELDS = asList("value", "timestamp");

	@Override
	public String toString() {
		return "KeyValuePair{" +
				"key=" + key +
				", value=" + value +
				", timestamp=" + timestamp +
				'}';
	}
}
