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

import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredInput;
import io.activej.codec.StructuredOutput;
import io.activej.common.exception.MalformedDataException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.activej.codec.StructuredCodecs.STRING_CODEC;

public class AggregationChunkCodec implements StructuredCodec<AggregationChunk> {
	private static final StructuredCodec<List<String>> MEASURES_CODEC = STRING_CODEC.ofList();
	public static final String ID = "id";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String COUNT = "count";
	public static final String MEASURES = "measures";

	private final ChunkIdCodec<Object> chunkIdCodec;
	private final StructuredCodec<PrimaryKey> primaryKeyCodec;
	private final Set<String> allowedMeasures;

	@SuppressWarnings("unchecked")
	private AggregationChunkCodec(ChunkIdCodec<?> chunkIdCodec,
			StructuredCodec<PrimaryKey> primaryKeyCodec,
			Set<String> allowedMeasures) {
		this.chunkIdCodec = (ChunkIdCodec<Object>) chunkIdCodec;
		this.primaryKeyCodec = primaryKeyCodec;
		this.allowedMeasures = allowedMeasures;
	}

	public static AggregationChunkCodec create(ChunkIdCodec<?> chunkIdCodec,
			StructuredCodec<PrimaryKey> primaryKeyCodec,
			Set<String> allowedMeasures) {
		return new AggregationChunkCodec(chunkIdCodec, primaryKeyCodec, allowedMeasures);
	}

	@Override
	public void encode(StructuredOutput out, AggregationChunk chunk) {
		out.writeObject(() -> {
			out.writeKey(ID);
			chunkIdCodec.encode(out, chunk.getChunkId());
			out.writeKey(MIN);
			primaryKeyCodec.encode(out, chunk.getMinPrimaryKey());
			out.writeKey(MAX);
			primaryKeyCodec.encode(out, chunk.getMaxPrimaryKey());
			out.writeKey(COUNT);
			out.writeInt(chunk.getCount());
			out.writeKey(MEASURES);
			MEASURES_CODEC.encode(out, chunk.getMeasures());
		});
	}

	@Override
	public AggregationChunk decode(StructuredInput in) throws MalformedDataException {
		return in.readObject($ -> {
			in.readKey(ID);
			Object id = chunkIdCodec.decode(in);
			in.readKey(MIN);
			PrimaryKey from = primaryKeyCodec.decode(in);
			in.readKey(MAX);
			PrimaryKey to = primaryKeyCodec.decode(in);
			in.readKey(COUNT);
			int count = in.readInt();
			in.readKey(MEASURES);
			List<String> measures = MEASURES_CODEC.decode(in);
			List<String> invalidMeasures = getInvalidMeasures(measures);
			if (!invalidMeasures.isEmpty()) throw new MalformedDataException("Unknown fields: " + invalidMeasures);
			return AggregationChunk.create(id, measures, from, to, count);
		});
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

}
