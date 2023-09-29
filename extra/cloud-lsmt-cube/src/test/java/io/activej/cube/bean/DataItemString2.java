package io.activej.cube.bean;

import io.activej.cube.aggregation.annotation.Key;
import io.activej.cube.aggregation.annotation.Measures;

import java.util.Objects;

public class DataItemString2 {
	@Key
	public String key1;
	@Key
	public int key2;

	@Measures
	public long metric2;
	@Measures
	public long metric3;

	public DataItemString2() {
	}

	public DataItemString2(String key1, int key2, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataItemString2 that = (DataItemString2) o;

		if (key2 != that.key2) return false;
		if (metric2 != that.metric2) return false;
		if (metric3 != that.metric3) return false;
		return Objects.equals(key1, that.key1);

	}

	@Override
	public int hashCode() {
		int result = key1 != null ? key1.hashCode() : 0;
		result = 31 * result + key2;
		result = 31 * result + (int) (metric2 ^ (metric2 >>> 32));
		result = 31 * result + (int) (metric3 ^ (metric3 >>> 32));
		return result;
	}
}
