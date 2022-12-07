package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.calcite.DataflowPartitionedTable;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.dataflow.calcite.utils.NamedReducer;
import io.activej.dataflow.codec.Subtype;
import io.activej.datastream.processor.StreamReducers;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.record.Record;
import io.activej.serializer.CorruptedDataException;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;
import io.activej.streamcodecs.StructuredStreamCodec;

final class ReducerCodecModule extends AbstractModule {
	@Provides
	@Subtype(6)
	StreamCodec<NamedReducer> namedReducer(DataflowSchema dataflowSchema) {
		return StructuredStreamCodec.create(tableName -> {
					DataflowTable dataflowTable = dataflowSchema.getDataflowTableMap().get(tableName);
					if (dataflowTable == null) {
						throw new CorruptedDataException("Unknown table: " + tableName);
					}
					if (!(dataflowTable instanceof DataflowPartitionedTable dataflowPartitionedTable)) {
						throw new CorruptedDataException("Not a partitioned table: " + tableName);
					}
					//noinspection unchecked
					return new NamedReducer(tableName, (StreamReducers.Reducer<Record, Record, Record, Object>) dataflowPartitionedTable.getReducer());
				},
				NamedReducer::getTableName, StreamCodecs.ofString());
	}
}
