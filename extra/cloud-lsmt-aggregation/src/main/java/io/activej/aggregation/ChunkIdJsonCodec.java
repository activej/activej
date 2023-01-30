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

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.NumberConverter;
import io.activej.aggregation.util.JsonCodec;
import io.activej.common.exception.MalformedDataException;

import java.io.IOException;

public interface ChunkIdJsonCodec<C> extends JsonCodec<C> {
	String toFileName(C chunkId);

	C fromFileName(String chunkFileName) throws MalformedDataException;

	@SuppressWarnings("NullableProblems")
	static ChunkIdJsonCodec<Long> ofLong() {
		return new ChunkIdJsonCodec<>() {
			@Override
			public String toFileName(Long chunkId) {
				return chunkId.toString();
			}

			@Override
			public Long fromFileName(String chunkFileName) throws MalformedDataException {
				try {
					return Long.parseLong(chunkFileName);
				} catch (NumberFormatException e) {
					throw new MalformedDataException(e);
				}
			}

			@Override
			public Long read(JsonReader reader) throws IOException {
				return NumberConverter.deserializeLong(reader);
			}

			@Override
			public void write(JsonWriter writer, Long value) {
				NumberConverter.serialize(value, writer);
			}
		};
	}

	@SuppressWarnings("NullableProblems")
	static ChunkIdJsonCodec<String> ofString() {
		return new ChunkIdJsonCodec<>() {
			@Override
			public String toFileName(String chunkId) {
				return chunkId;
			}

			@Override
			public String fromFileName(String chunkFileName) {
				return chunkFileName;
			}

			@Override
			public String read(JsonReader reader) throws IOException {
				return reader.readString();
			}

			@Override
			public void write(JsonWriter writer, String value) {
				writer.writeString(value);
			}
		};
	}
}
