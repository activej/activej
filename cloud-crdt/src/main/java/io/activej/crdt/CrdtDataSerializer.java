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

package io.activej.crdt;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;

public final class CrdtDataSerializer<K extends Comparable<K>, S> implements BinarySerializer<CrdtData<K, S>> {
	private final BinarySerializer<K> keySerializer;
	private final BinarySerializer<S> stateSerializer;

	public CrdtDataSerializer(BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		this.keySerializer = keySerializer;
		this.stateSerializer = stateSerializer;
	}

	public BinarySerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public BinarySerializer<S> getStateSerializer() {
		return stateSerializer;
	}

	@Override
	public void encode(BinaryOutput out, CrdtData<K, S> item) {
		keySerializer.encode(out, item.getKey());
		stateSerializer.encode(out, item.getState());
	}

	@Override
	public CrdtData<K, S> decode(BinaryInput in) {
		return new CrdtData<>(keySerializer.decode(in), stateSerializer.decode(in));
	}
}
