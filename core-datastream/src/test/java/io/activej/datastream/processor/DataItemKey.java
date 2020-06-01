package io.activej.datastream.processor;

import org.jetbrains.annotations.NotNull;

public class DataItemKey implements Comparable<DataItemKey> {
	public int key1;
	public int key2;

	public DataItemKey() {
	}

	public DataItemKey(int key1, int key2) {
		this.key1 = key1;
		this.key2 = key2;
	}

	@Override
	public boolean equals(Object o) {
		DataItemKey that = (DataItemKey) o;

		return key1 == that.key1 && key2 == that.key2;
	}

	@Override
	public String toString() {
		return "DataItemKey{" +
				"key1=" + key1 +
				", key2=" + key2 +
				'}';
	}

	@Override
	public int compareTo(@NotNull DataItemKey o) {
		int result = Integer.compare(key1, o.key1);
		if (result != 0)
			return result;
		return Integer.compare(key2, o.key2);
	}

	@Override
	public int hashCode() {
		int result = key1;
		result = 31 * result + key2;
		return result;
	}
}
