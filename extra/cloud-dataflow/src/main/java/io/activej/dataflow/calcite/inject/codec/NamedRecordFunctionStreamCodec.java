package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.table.AbstractDataflowTable;
import io.activej.dataflow.calcite.utils.RecordFunction_Named;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.serializer.stream.StreamInput;
import io.activej.serializer.stream.StreamOutput;

import java.io.IOException;

final class NamedRecordFunctionStreamCodec implements StreamCodec<RecordFunction_Named<?>> {
	private static final StreamCodec<String> STRING_STREAM_CODEC = StreamCodecs.ofString();

	private final DataflowSchema dataflowSchema;

	public NamedRecordFunctionStreamCodec(DataflowSchema dataflowSchema) {
		this.dataflowSchema = dataflowSchema;
	}

	@Override
	public void encode(StreamOutput out, RecordFunction_Named<?> namedRecordFunction) throws IOException {
		STRING_STREAM_CODEC.encode(out, namedRecordFunction.getTableName());
	}

	@Override
	public RecordFunction_Named<?> decode(StreamInput in) throws IOException {
		String tableName = STRING_STREAM_CODEC.decode(in);
		AbstractDataflowTable<?> dataflowTable = dataflowSchema.getDataflowTableMap().get(tableName);
		if (dataflowTable == null) {
			throw new CorruptedDataException("Unknown table: " + tableName);
		}
		return new RecordFunction_Named<>(tableName, dataflowTable.getRecordFunction());
	}
}
