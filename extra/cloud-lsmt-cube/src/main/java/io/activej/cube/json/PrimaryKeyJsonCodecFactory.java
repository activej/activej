package io.activej.cube.json;

import io.activej.cube.CubeStructure;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.json.JsonCodec;

import static io.activej.cube.aggregation.json.JsonCodecs.ofPrimaryKey;

public interface PrimaryKeyJsonCodecFactory {
	JsonCodec<PrimaryKey> getJsonCodec(String aggregationId);

	static PrimaryKeyJsonCodecFactory ofCubeStructure(CubeStructure cubeStructure) {
		return aggregationId -> ofPrimaryKey(cubeStructure.getAggregationStructure(aggregationId));
	}
}
