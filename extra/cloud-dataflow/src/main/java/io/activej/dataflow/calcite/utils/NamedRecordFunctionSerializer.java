package io.activej.dataflow.calcite.utils;

import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class NamedRecordFunctionSerializer implements BinarySerializer<NamedRecordFunction<?>> {
	private final DataflowSchema dataflowSchema;

	public NamedRecordFunctionSerializer(DataflowSchema dataflowSchema) {
		this.dataflowSchema = dataflowSchema;
	}

	@Override
	public void encode(BinaryOutput out, NamedRecordFunction<?> namedRecordFunction) {
		UTF8_SERIALIZER.encode(out, namedRecordFunction.getTableName());
	}

	@Override
	public NamedRecordFunction<?> decode(BinaryInput in) throws CorruptedDataException {
		String tableName = UTF8_SERIALIZER.decode(in);
		DataflowTable dataflowTable = dataflowSchema.getDataflowTableMap().get(tableName);
		if (dataflowTable == null) {
			throw new CorruptedDataException("Unknown table: " + tableName);
		}
		return new NamedRecordFunction<>(tableName, dataflowTable.getRecordFunction());
	}
}
