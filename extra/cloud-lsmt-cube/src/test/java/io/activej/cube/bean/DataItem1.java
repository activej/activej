package io.activej.cube.bean;

import io.activej.aggregation.annotation.Key;
import io.activej.aggregation.annotation.Measures;

public class DataItem1 {
	@Key
	public int key1;
	@Key
	public int key2;

	@Measures
	public long metric1;
	@Measures
	public long metric2;

	public DataItem1() {
	}

	public DataItem1(int key1, int key2, long metric1, long metric2) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
	}
}
