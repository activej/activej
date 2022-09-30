package io.activej.dataflow.calcite.utils;

import io.activej.dataflow.DataflowPartitionedTable;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class NamedReducerFunctionSerializer implements BinarySerializer<NamedReducer> {
	private final DataflowSchema dataflowSchema;

	public NamedReducerFunctionSerializer(DataflowSchema dataflowSchema) {
		this.dataflowSchema = dataflowSchema;
	}

	@Override
	public void encode(BinaryOutput out, NamedReducer namedReducer) {
		UTF8_SERIALIZER.encode(out, namedReducer.getTableName());
	}

	@Override
	public NamedReducer decode(BinaryInput in) throws CorruptedDataException {
		String tableName = UTF8_SERIALIZER.decode(in);
		DataflowTable<?> dataflowTable = dataflowSchema.getDataflowTableMap().get(tableName);
		if (dataflowTable == null) {
			throw new CorruptedDataException("Unknown table: " + tableName);
		}
		if (!(dataflowTable instanceof DataflowPartitionedTable<?> dataflowPartitionedTable)) {
			throw new CorruptedDataException("Not a partitioned table: " + tableName);
		}
		return new NamedReducer(tableName, dataflowPartitionedTable.getReducer());
	}
}
