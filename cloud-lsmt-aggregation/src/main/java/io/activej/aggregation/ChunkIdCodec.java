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

public interface ChunkIdCodec<C> extends StructuredCodec<C> {
	String toFileName(C chunkId);

	C fromFileName(String chunkFileName) throws MalformedDataException;

	@Override
	void encode(StructuredOutput out, C value);

	@Override
	C decode(StructuredInput in) throws MalformedDataException;

	static ChunkIdCodec<Long> ofLong() {
		return new ChunkIdCodec<Long>() {
			@Override
			public String toFileName(Long chunkId) {
				return chunkId.toString();
			}

			@Override
			public Long fromFileName(String chunkFileName) throws MalformedDataException {
				try {
					return Long.parseLong(chunkFileName);
				} catch (NumberFormatException e){
					throw new MalformedDataException(e);
				}
			}

			@Override
			public void encode(StructuredOutput out, Long value) {
				out.writeLong(value);
			}

			@Override
			public Long decode(StructuredInput in) throws MalformedDataException {
				return in.readLong();
			}
		};
	}

	static ChunkIdCodec<String> ofString() {
		return new ChunkIdCodec<String>() {
			@Override
			public String toFileName(String chunkId) {
				return chunkId;
			}

			@Override
			public String fromFileName(String chunkFileName) {
				return chunkFileName;
			}

			@Override
			public void encode(StructuredOutput out, String value) {
				out.writeString(value);
			}

			@Override
			public String decode(StructuredInput in) throws MalformedDataException {
				return in.readString();
			}
		};
	}
}
