/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.aggregation.json;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.json.JsonCodec;
import io.activej.json.JsonValidationException;
import io.activej.json.ObjectJsonCodec;

import java.util.Set;

import static io.activej.json.JsonCodecs.*;

public class JsonCodecs {

	public static JsonCodec<AggregationChunk> ofAggregationChunk(
		JsonCodec<Object> chunkIdCodec,
		JsonCodec<PrimaryKey> primaryKeyCodec,
		Set<String> allowedMeasures
	) {
		return ofObject((chunkId, minPrimaryKey, maxPrimaryKey, count, fields) -> {
				if (!allowedMeasures.containsAll(fields)) {
					throw new JsonValidationException(
						"Unknown fields: " +
						fields.stream().filter(measure -> !allowedMeasures.contains(measure)).toList()
					);
				}
				return AggregationChunk.create(chunkId, fields, minPrimaryKey, maxPrimaryKey, count);
			},
			"id", AggregationChunk::getChunkId, chunkIdCodec,
			"min", AggregationChunk::getMinPrimaryKey, primaryKeyCodec,
			"max", AggregationChunk::getMaxPrimaryKey, primaryKeyCodec,
			"count", AggregationChunk::getCount, ofInteger(),
			"measures", AggregationChunk::getMeasures, ofList(ofString())
		);
	}

	public static JsonCodec<PrimaryKey> ofPrimaryKey(AggregationStructure structure) {
		return ofArrayObject(structure.getKeyTypes().values().stream().map(FieldType::getInternalJsonCodec).toArray(JsonCodec[]::new))
			.transform(PrimaryKey::getArray, PrimaryKey::ofArray);
	}

	public static JsonCodec<AggregationDiff> ofAggregationDiff(AggregationStructure structure) {
		//noinspection unchecked
		JsonCodec<Set<AggregationChunk>> chunksCodec = ofSet(
			ofAggregationChunk(
				(JsonCodec<Object>) structure.getChunkIdJsonCodec(),
				ofPrimaryKey(structure),
				structure.getMeasureTypes().keySet()));
		//noinspection unchecked
		return ObjectJsonCodec.builder(params -> AggregationDiff.of((Set<AggregationChunk>) params[0], (Set<AggregationChunk>) params[1]))
			.with("added", AggregationDiff::getAddedChunks, chunksCodec)
			.with("removed", AggregationDiff::getRemovedChunks, chunksCodec, Set.of())
			.build();
	}
}
