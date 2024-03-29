package io.activej.cube.bean;

import io.activej.cube.aggregation.annotation.Key;
import io.activej.cube.aggregation.annotation.Measures;

import java.util.List;

public class DataItem2 {
	@Key
	public int key1;
	@Key
	public int key2;

	@Measures
	public long metric2;
	@Measures
	public long metric3;

	public DataItem2() {
	}

	public DataItem2(int key1, int key2, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	public static final List<String> DIMENSIONS = List.of("key1", "key2");

	public static final List<String> METRICS = List.of("metric2", "metric3");

}
