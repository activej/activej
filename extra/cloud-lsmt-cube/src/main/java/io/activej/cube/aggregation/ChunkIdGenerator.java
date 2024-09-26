package io.activej.cube.aggregation;

import io.activej.promise.Promise;

import java.util.Map;
import java.util.Set;

public interface ChunkIdGenerator {
	Promise<String> createProtoChunkId();

	Promise<Map<String, Long>> convertToActualChunkIds(Set<String> protoChunkIds);
}
