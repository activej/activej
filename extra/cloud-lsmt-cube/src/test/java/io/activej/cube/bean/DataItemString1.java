package io.activej.cube.bean;

import io.activej.cube.aggregation.annotation.Key;
import io.activej.cube.aggregation.annotation.Measures;

import java.util.List;
import java.util.Objects;

public class DataItemString1 {
	@Key
	public String key1;
	@Key
	public int key2;

	@Measures
	public long metric1;
	@Measures
	public long metric2;

	public DataItemString1() {
	}

	public DataItemString1(String key1, int key2, long metric1, long metric2) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
	}

	public static final List<String> DIMENSIONS = List.of("key1", "key2");

	public static final List<String> METRICS = List.of("metric1", "metric2");

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataItemString1 that = (DataItemString1) o;

		if (key2 != that.key2) return false;
		if (metric1 != that.metric1) return false;
		if (metric2 != that.metric2) return false;
		return Objects.equals(key1, that.key1);

	}

	@Override
	public int hashCode() {
		int result = key1 != null ? key1.hashCode() : 0;
		result = 31 * result + key2;
		result = 31 * result + (int) (metric1 ^ (metric1 >>> 32));
		result = 31 * result + (int) (metric2 ^ (metric2 >>> 32));
		return result;
	}
}
