package io.activej.cube.bean;

import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;

import java.util.List;

import static java.util.Arrays.asList;

public class DataItem3 {
	@Key
	public int key1;
	@Key
	public int key2;
	@Key
	public int key3;
	@Key
	public int key4;
	@Key
	public int key5;

	@Measures
	public long metric1;
	@Measures
	public long metric2;

	public DataItem3() {
	}

	public DataItem3(int key1, int key2, int key3, int key4, int key5, long metric1, long metric2) {
		this.key1 = key1;
		this.key2 = key2;
		this.key3 = key3;
		this.key4 = key4;
		this.key5 = key5;
		this.metric1 = metric1;
		this.metric2 = metric2;
	}

	public static final List<String> DIMENSIONS = asList("key1", "key2", "key3", "key4", "key5");

	public static final List<String> METRICS = asList("metric1", "metric2");
}
