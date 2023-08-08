package io.activej.cube.json;

import io.activej.aggregation.PrimaryKey;
import io.activej.cube.Cube;
import io.activej.json.JsonCodec;

import static io.activej.aggregation.json.JsonCodecs.ofPrimaryKey;

public interface PrimaryKeyJsonCodecFactory {
	JsonCodec<PrimaryKey> getJsonCodec(String aggregationId);

	static PrimaryKeyJsonCodecFactory ofCube(Cube cube) {
		return aggregationId -> ofPrimaryKey(cube.getAggregation(aggregationId).getStructure());
	}
}
