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

import io.activej.common.exception.MalformedDataException;
import io.activej.json.ForwardingJsonCodec;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;

public abstract class ChunkIdJsonCodec<C> extends ForwardingJsonCodec<C> {
	protected ChunkIdJsonCodec(JsonCodec<C> codec) {
		super(codec);
	}

	public abstract String toFileName(C chunkId);

	public abstract C fromFileName(String chunkFileName) throws MalformedDataException;

	public static ChunkIdJsonCodec<Long> ofLong() {
		return new ChunkIdJsonCodec<>(JsonCodecs.ofLong()) {
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
		};
	}

	public static ChunkIdJsonCodec<String> ofString() {
		return new ChunkIdJsonCodec<>(JsonCodecs.ofString()) {
			@Override
			public String toFileName(String chunkId) {
				return chunkId;
			}

			@Override
			public String fromFileName(String chunkFileName) {
				return chunkFileName;
			}
		};
	}
}
