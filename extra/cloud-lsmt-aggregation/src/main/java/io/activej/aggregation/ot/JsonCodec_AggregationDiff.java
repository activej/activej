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

package io.activej.aggregation.ot;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.JsonCodec_AggregationChunk;
import io.activej.aggregation.JsonCodec_PrimaryKey;
import io.activej.aggregation.PrimaryKey;
import io.activej.aggregation.util.JsonCodec;
import io.activej.common.initializer.WithInitializer;

import java.io.IOException;
import java.util.Set;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.ot.repository.JsonIndentUtils.oneline;

public class JsonCodec_AggregationDiff implements JsonCodec<AggregationDiff>, WithInitializer<JsonCodec_AggregationDiff> {
	public static final String ADDED = "added";
	public static final String REMOVED = "removed";

	private final JsonCodec<Set<AggregationChunk>> aggregationChunksCodec;

	private JsonCodec_AggregationDiff(JsonCodec_AggregationChunk aggregationChunkCodec) {
		this.aggregationChunksCodec = JsonCodec.of(
				reader -> ((JsonReader<?>) reader).readSet(aggregationChunkCodec),
				(writer, value) -> writer.serialize(value, oneline(aggregationChunkCodec))
		);
	}

	public static JsonCodec_AggregationDiff create(AggregationStructure structure) {
		Set<String> allowedMeasures = structure.getMeasureTypes().keySet();
		JsonCodec<PrimaryKey> primaryKeyCodec = JsonCodec_PrimaryKey.create(structure);
		return new JsonCodec_AggregationDiff(JsonCodec_AggregationChunk.create(structure.getChunkIdCodec(), primaryKeyCodec, allowedMeasures));
	}

	@Override
	public void write(JsonWriter writer, AggregationDiff diff) {
		assert diff != null;
		writer.writeByte(OBJECT_START);
		writer.writeString(ADDED);
		writer.writeByte(SEMI);
		aggregationChunksCodec.write(writer, diff.getAddedChunks());
		if (!diff.getRemovedChunks().isEmpty()) {
			writer.writeByte(COMMA);
			writer.writeString(REMOVED);
			writer.writeByte(SEMI);
			aggregationChunksCodec.write(writer, diff.getRemovedChunks());
		}
		writer.writeByte(OBJECT_END);
	}

	@Override
	public AggregationDiff read(JsonReader reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();
		String key = reader.readKey();
		if (!key.equals(ADDED)) throw reader.newParseError("Expected key '" + ADDED + '\'');
		Set<AggregationChunk> added = aggregationChunksCodec.read(reader);
		Set<AggregationChunk> removed = Set.of();
		assert added != null;
		byte next = reader.getNextToken();
		if (next == COMMA) {
			reader.getNextToken();
			String removedKey = reader.readKey();
			if (!removedKey.equals(REMOVED))
				throw reader.newParseError("Expected key '" + REMOVED + '\'');
			removed = aggregationChunksCodec.read(reader);
			assert removed != null;
			reader.endObject();
		} else if (next != OBJECT_END) {
			throw reader.newParseError("Expected '}'");
		}
		return AggregationDiff.of(added, removed);
	}
}
