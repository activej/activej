package io.activej.dataflow.calcite.join;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

public final class RecordKeyFunctionSerializer<K extends Comparable<K>> implements BinarySerializer<RecordKeyFunction<K>> {
	@Override
	public void encode(BinaryOutput out, RecordKeyFunction<K> item) {
		out.writeVarInt(item.getIndex());
	}

	@Override
	public RecordKeyFunction<K> decode(BinaryInput in) throws CorruptedDataException {
		return new RecordKeyFunction<>(in.readVarInt());
	}
}
