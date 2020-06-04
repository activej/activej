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

package io.activej.launchers.crdt;

import io.activej.codec.StructuredCodec;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.util.CrdtDataSerializer;

public final class CrdtDescriptor<K extends Comparable<K>, S> {
	private final CrdtFunction<S> crdtFunction;
	private final CrdtDataSerializer<K, S> serializer;
	private final StructuredCodec<K> keyCodec;
	private final StructuredCodec<S> stateCodec;

	public CrdtDescriptor(CrdtFunction<S> crdtFunction, CrdtDataSerializer<K, S> serializer, StructuredCodec<K> keyCodec, StructuredCodec<S> stateCodec) {
		this.crdtFunction = crdtFunction;
		this.serializer = serializer;
		this.keyCodec = keyCodec;
		this.stateCodec = stateCodec;
	}

	public CrdtFunction<S> getCrdtFunction() {
		return crdtFunction;
	}

	public CrdtDataSerializer<K, S> getSerializer() {
		return serializer;
	}

	public StructuredCodec<K> getKeyCodec() {
		return keyCodec;
	}

	public StructuredCodec<S> getStateCodec() {
		return stateCodec;
	}
}
