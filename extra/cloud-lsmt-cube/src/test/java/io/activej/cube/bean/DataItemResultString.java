package io.activej.cube.bean;

import io.activej.cube.aggregation.measure.Measure;

import java.util.Map;
import java.util.stream.Stream;

import static io.activej.common.collection.CollectorUtils.toLinkedHashMap;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.ofLong;
import static io.activej.cube.aggregation.measure.Measures.sum;

public class DataItemResultString {
	public String key1;
	public int key2;

	public long metric1;
	public long metric2;
	public long metric3;

	public DataItemResultString() {
	}

	public DataItemResultString(String key1, int key2, long metric1, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	public static final Map<String, Class<?>> DIMENSIONS =
		Stream.of("key1", "key2")
			.collect(toLinkedHashMap(k -> k.equals("key1") ? String.class : int.class));

	public static final Map<String, Measure> METRICS =
		Stream.of("metric1", "metric2", "metric3")
			.collect(toLinkedHashMap(k -> sum(ofLong())));

	@Override
	@SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "RedundantIfStatement"})
	public boolean equals(Object o) {
		DataItemResultString that = (DataItemResultString) o;

		if (!key1.equals(that.key1)) return false;
		if (key2 != that.key2) return false;
		if (metric1 != that.metric1) return false;
		if (metric2 != that.metric2) return false;
		if (metric3 != that.metric3) return false;

		return true;
	}

	@Override
	public String toString() {
		return
			"DataItemResultString{" +
			"key1='" + key1 + '\'' +
			", key2=" + key2 +
			", metric1=" + metric1 +
			", metric2=" + metric2 +
			", metric3=" + metric3 +
			'}';
	}
}
