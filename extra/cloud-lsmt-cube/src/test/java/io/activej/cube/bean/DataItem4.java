package io.activej.cube.bean;

import io.activej.cube.aggregation.annotation.Key;
import io.activej.cube.aggregation.annotation.Measures;

public class DataItem4 {
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
	public long metric2;
	@Measures
	public long metric3;

	public DataItem4() {
	}

	public DataItem4(int key1, int key2, int key3, int key4, int key5, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.key3 = key3;
		this.key4 = key4;
		this.key5 = key5;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}
}
