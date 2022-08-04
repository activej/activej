package io.activej.dataflow.calcite.utils;

import io.activej.record.Record;

import java.util.function.Function;

public final class ToZeroFunction implements Function<Record, Integer> {
	private static final ToZeroFunction INSTANCE = new ToZeroFunction();

	private ToZeroFunction() {
	}

	public static ToZeroFunction getInstance(){
		return INSTANCE;
	}

	@Override
	public Integer apply(Record record) {
		return 0;
	}
}
