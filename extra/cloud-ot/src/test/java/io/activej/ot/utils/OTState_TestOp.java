package io.activej.ot.utils;

import io.activej.ot.OTState;

public class OTState_TestOp implements OTState<TestOp> {
	private int value;

	@Override
	public void init() {
		value = 0;
	}

	@Override
	public void apply(TestOp testOp) {
		value = testOp.apply(value);
	}

	public int getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "" + value;
	}
}
