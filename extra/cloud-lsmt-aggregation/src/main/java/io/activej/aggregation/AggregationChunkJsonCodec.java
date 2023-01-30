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

package io.activej.aggregation;

import com.dslplatform.json.*;
import com.dslplatform.json.JsonReader.ReadObject;
import io.activej.aggregation.util.JsonCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.dslplatform.json.JsonWriter.*;

public final class AggregationChunkJsonCodec implements JsonCodec<AggregationChunk> {
	public static final String ID = "id";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String COUNT = "count";
	public static final String MEASURES = "measures";

	private final ChunkIdJsonCodec<Object> chunkIdCodec;
	private final JsonCodec<PrimaryKey> primaryKeyFormat;
	private final Set<String> allowedMeasures;

	@SuppressWarnings("unchecked")
	private AggregationChunkJsonCodec(ChunkIdJsonCodec<?> chunkIdCodec,
			JsonCodec<PrimaryKey> primaryKeyFormat,
			Set<String> allowedMeasures) {
		this.chunkIdCodec = (ChunkIdJsonCodec<Object>) chunkIdCodec;
		this.primaryKeyFormat = primaryKeyFormat;
		this.allowedMeasures = allowedMeasures;
	}

	public static AggregationChunkJsonCodec create(ChunkIdJsonCodec<?> chunkIdCodec,
			JsonCodec<PrimaryKey> primaryKeyCodec,
			Set<String> allowedMeasures) {
		return new AggregationChunkJsonCodec(chunkIdCodec, primaryKeyCodec, allowedMeasures);
	}

	@Override
	public AggregationChunk read(JsonReader reader) throws IOException {
		if (reader.wasNull()) return null;

		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

		Object id = readValue(reader, ID, chunkIdCodec);
		reader.comma();

		PrimaryKey from = readValue(reader, MIN, primaryKeyFormat);
		reader.comma();

		PrimaryKey to = readValue(reader, MAX, primaryKeyFormat);
		reader.comma();

		Integer count = readValue(reader, COUNT, NumberConverter::deserializeInt);
		reader.comma();

		List<String> measures = readValue(reader, MEASURES, $ -> ((JsonReader<?>) reader).readCollection(JsonReader::readString));

		reader.endObject();

		List<String> invalidMeasures = getInvalidMeasures(measures);
		if (!invalidMeasures.isEmpty()) throw ParsingException.create("Unknown fields: " + invalidMeasures, true);
		return AggregationChunk.create(id, measures, from, to, count);
	}

	@SuppressWarnings("NullableProblems")
	@Override
	public void write(JsonWriter writer, AggregationChunk chunk) {
		if (chunk == null) {
			writer.writeNull();
			return;
		}
		writer.writeByte(OBJECT_START);

		writer.writeString(ID);
		writer.writeByte(SEMI);
		chunkIdCodec.write(writer, chunk.getChunkId());
		writer.writeByte(COMMA);

		writer.writeString(MIN);
		writer.writeByte(SEMI);
		primaryKeyFormat.write(writer, chunk.getMinPrimaryKey());
		writer.writeByte(COMMA);

		writer.writeString(MAX);
		writer.writeByte(SEMI);
		primaryKeyFormat.write(writer, chunk.getMaxPrimaryKey());
		writer.writeByte(COMMA);

		writer.writeString(COUNT);
		writer.writeByte(SEMI);
		NumberConverter.serialize(chunk.getCount(), writer);
		writer.writeByte(COMMA);

		writer.writeString(MEASURES);
		writer.writeByte(SEMI);
		StringConverter.serialize(chunk.getMeasures(), writer);

		writer.writeByte(OBJECT_END);
	}

	private List<String> getInvalidMeasures(List<String> measures) {
		List<String> invalidMeasures = new ArrayList<>();
		for (String measure : measures) {
			if (!allowedMeasures.contains(measure)) {
				invalidMeasures.add(measure);
			}
		}
		return invalidMeasures;
	}

	private static <T> T readValue(JsonReader<?> reader, String key, ReadObject<T> readObject) throws IOException {
		reader.getNextToken();
		String readKey = reader.readKey();
		if (!readKey.equals(key)) throw reader.newParseError("Expected key '" + key + '\'');
		return readObject.read(reader);
	}

}
