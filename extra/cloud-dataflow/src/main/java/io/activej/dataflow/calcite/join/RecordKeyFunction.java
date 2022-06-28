package io.activej.dataflow.calcite.join;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.util.function.Function;

public final class RecordKeyFunction<K extends Comparable<K>> implements Function<Record, K> {
	private final int index;

	public RecordKeyFunction(@Deserialize("index") int index) {
		this.index = index;
	}

	@Override
	public K apply(Record record) {
		return record.get(index);
	}

	@Serialize(order = 1)
	public int getIndex() {
		return index;
	}
}
