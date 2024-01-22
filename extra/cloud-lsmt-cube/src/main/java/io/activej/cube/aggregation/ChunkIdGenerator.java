package io.activej.cube.aggregation;

import io.activej.promise.Promise;

import java.util.List;

public interface ChunkIdGenerator {
	Promise<String> createProtoChunkId();

	Promise<List<Long>> convertToActualChunkIds(List<String> protoChunkIds);
}
