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

package io.activej.crdt.util;

import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtTombstone;
import io.activej.serializer.*;

public final class CrdtDataBinarySerializer<K extends Comparable<K>, S> implements BinarySerializer<CrdtData<K, S>> {
	public static final BinarySerializer<Long> TIMESTAMP_SERIALIZER = BinarySerializers.LONG_SERIALIZER;

	private final BinarySerializer<K> keySerializer;
	private final BinarySerializer<S> stateSerializer;
	private final BinarySerializer<CrdtTombstone<K>> tombstoneSerializer;

	public CrdtDataBinarySerializer(BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
		this.tombstoneSerializer = createTombstoneSerializer(keySerializer);
	}

	public BinarySerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public BinarySerializer<S> getStateSerializer() {
		return stateSerializer;
	}

	public BinarySerializer<CrdtTombstone<K>> getTombstoneSerializer() {
		return tombstoneSerializer;
	}

	@Override
	public void encode(BinaryOutput out, CrdtData<K, S> item) {
		keySerializer.encode(out, item.getKey());
		TIMESTAMP_SERIALIZER.encode(out, item.getTimestamp());
		stateSerializer.encode(out, item.getState());
	}

	@Override
	public CrdtData<K, S> decode(BinaryInput in) {
		return new CrdtData<>(keySerializer.decode(in), TIMESTAMP_SERIALIZER.decode(in), stateSerializer.decode(in));
	}

	private static <K extends Comparable<K>> BinarySerializer<CrdtTombstone<K>> createTombstoneSerializer(BinarySerializer<K> keySerializer) {
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, CrdtTombstone<K> item) {
				keySerializer.encode(out, item.getKey());
				TIMESTAMP_SERIALIZER.encode(out, item.getTimestamp());
			}

			@Override
			public CrdtTombstone<K> decode(BinaryInput in) throws CorruptedDataException {
				return new CrdtTombstone<>(keySerializer.decode(in), TIMESTAMP_SERIALIZER.decode(in));
			}
		};
	}
}
