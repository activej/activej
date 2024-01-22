package io.activej.cube.aggregation;

import java.util.List;

public record ProtoAggregationChunk(String protoChunkId, List<String> fields, PrimaryKey minPrimaryKey, PrimaryKey maxPrimaryKey, int count) {
	public AggregationChunk toAggregationChunk(long chunkId) {
		return AggregationChunk.create(
			chunkId,
			fields,
			minPrimaryKey,
			maxPrimaryKey,
			count
		);
	}
}
