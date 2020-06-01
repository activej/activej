package io.activej.cube.bean;

import io.activej.aggregation.measure.Measure;

import java.util.Map;
import java.util.stream.Stream;

import static io.activej.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.aggregation.measure.Measures.sum;
import static io.activej.common.collection.CollectionUtils.keysToMap;

public class DataItemResult2 {
	public int key2;

	public long metric1;
	public long metric2;
	public long metric3;

	public DataItemResult2() {
	}

	public DataItemResult2(int key2, long metric1, long metric2, long metric3) {
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	public static final Map<String, Class<?>> DIMENSIONS =
			keysToMap(Stream.of("key2"), k -> int.class);

	public static final Map<String, Measure> METRICS =
			keysToMap(Stream.of("metric1", "metric2", "metric3"), k -> sum(ofLong()));

	@Override
	public boolean equals(Object o) {
		DataItemResult2 that = (DataItemResult2) o;

		if (key2 != that.key2) return false;
		if (metric1 != that.metric1) return false;
		if (metric2 != that.metric2) return false;
		if (metric3 != that.metric3) return false;

		return true;
	}

	@Override
	public String toString() {
		return "DataItemResult2{" +
				"key2=" + key2 +
				", metric1=" + metric1 +
				", metric2=" + metric2 +
				", metric3=" + metric3 +
				'}';
	}
}
