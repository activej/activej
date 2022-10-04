package io.activej.dataflow.calcite.join;

import io.activej.record.Record;

import java.util.function.Function;

public final class RecordKeyFunction<K extends Comparable<K>> implements Function<Record, K> {
	private final int index;

	public RecordKeyFunction(int index) {
		this.index = index;
	}

	@Override
	public K apply(Record record) {
		return record.get(index);
	}

	public int getIndex() {
		return index;
	}
}
