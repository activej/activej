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

package io.activej.cube.aggregation;

import io.activej.common.exception.MalformedDataException;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;

public final class ChunkIdJsonCodec {
	public static final JsonCodec<Long> CODEC = JsonCodecs.ofLong();

	private ChunkIdJsonCodec() {
		throw new AssertionError();
	}

	public static String toFileName(long chunkId) {
		return String.valueOf(chunkId);
	}

	public static long fromFileName(String chunkFileName) throws MalformedDataException {
		try {
			return Long.parseLong(chunkFileName);
		} catch (NumberFormatException e) {
			throw new MalformedDataException(e);
		}
	}
}
