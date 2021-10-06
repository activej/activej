package io.activej.datastream.processor;

import java.util.Objects;

public class DataItemResult {
	public int key1;
	public int key2;

	public long metric1;
	public long metric2;
	public long metric3;

	public DataItemResult() {
	}

	public DataItemResult(int key1, int key2, long metric1, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	@Override
	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
	public boolean equals(Object o) {
		DataItemResult that = (DataItemResult) o;

		return (key1 == that.key1) && (key2 == that.key2)
				&& (metric1 == that.metric1) && (metric2 == that.metric2) && (metric3 == that.metric3);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key1, key2, metric1, metric2, metric3);
	}

	@Override
	public String toString() {
		return "DataItemResult{" +
				"key1=" + key1 +
				", key2=" + key2 +
				", metric1=" + metric1 +
				", metric2=" + metric2 +
				", metric3=" + metric3 +
				'}';
	}
}
