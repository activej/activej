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

import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.util.CrdtDataSerializer;

import java.lang.reflect.Type;

public final class CrdtDescriptor<K extends Comparable<K>, S> {
	private final CrdtFunction<S> crdtFunction;
	private final CrdtDataSerializer<K, S> serializer;
	private final Type keyManifest;
	private final Type stateManifest;

	public CrdtDescriptor(CrdtFunction<S> crdtFunction, CrdtDataSerializer<K, S> serializer, Type keyManifest, Type stateManifest) {
		this.crdtFunction = crdtFunction;
		this.serializer = serializer;
		this.keyManifest = keyManifest;
		this.stateManifest = stateManifest;
	}

	public CrdtFunction<S> getCrdtFunction() {
		return crdtFunction;
	}

	public CrdtDataSerializer<K, S> getSerializer() {
		return serializer;
	}

	public Type getKeyManifest() {
		return keyManifest;
	}

	public Type getStateManifest() {
		return stateManifest;
	}
}
