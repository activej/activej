package io.activej.cube.json;

import io.activej.aggregation.PrimaryKey;
import io.activej.cube.CubeStructure;
import io.activej.json.JsonCodec;

import static io.activej.aggregation.json.JsonCodecs.ofPrimaryKey;

public interface PrimaryKeyJsonCodecFactory {
	JsonCodec<PrimaryKey> getJsonCodec(String aggregationId);

	static PrimaryKeyJsonCodecFactory ofCubeStructure(CubeStructure cubeStructure) {
		return aggregationId -> ofPrimaryKey(cubeStructure.getAggregationStructure(aggregationId));
	}
}
