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

package io.activej.dataflow.proto.serializer;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.util.HashMap;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.max;

public final class FunctionSubtypeSerializer<T> implements BinarySerializer<T> {
	private final Map<String, BinarySerializer<? extends T>> namesToAdapters = new HashMap<>();
	private final Map<Class<? extends T>, String> subtypesToNames = new HashMap<>();

	private FunctionSubtypeSerializer() {
	}

	public static <T> FunctionSubtypeSerializer<T> create() {
		return new FunctionSubtypeSerializer<>();
	}

	@SuppressWarnings("unchecked")
	public void setSubtypeCodec(Class<? extends T> type, String name, BinarySerializer<? extends T> codec) {
		checkArgument(!namesToAdapters.containsKey(name), () -> "Conflicting subtype: " + name);

		namesToAdapters.put(name, codec);
		subtypesToNames.put(type, name);
		if (codec instanceof FunctionSubtypeSerializer) {
			namesToAdapters.putAll(((FunctionSubtypeSerializer<? extends T>) codec).namesToAdapters);
			subtypesToNames.putAll(((FunctionSubtypeSerializer<? extends T>) codec).subtypesToNames);
		}
	}

	public void setSubtypeCodec(Class<? extends T> type, BinarySerializer<? extends T> codec) {
		String name = type.getTypeName();
		name = name.substring(max(name.lastIndexOf('.'), name.lastIndexOf('$')) + 1);
		setSubtypeCodec(type, name, codec);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void encode(BinaryOutput out, T item) {
		String tag = subtypesToNames.get(item.getClass());
		if (tag == null) {
			throw new IllegalArgumentException("Unregistered data type: " + item.getClass().getName());
		}
		BinarySerializer<T> codec = (BinarySerializer<T>) namesToAdapters.get(tag);
		out.writeUTF8(tag);
		codec.encode(out, item);
	}

	@Override
	public T decode(BinaryInput in) throws CorruptedDataException {
		String key = in.readUTF8();
		BinarySerializer<? extends T> codec = namesToAdapters.get(key);
		if (codec == null) {
			throw new CorruptedDataException("Could not find codec for: " + key);
		}
		return codec.decode(in);
	}
}
