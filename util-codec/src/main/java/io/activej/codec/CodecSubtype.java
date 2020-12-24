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

package io.activej.codec;

import io.activej.common.api.WithInitializer;
import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.max;

/**
 * This is a {@link StructuredCodec codec} that stores subtypes of T with different codecs
 * as a <a href="https://en.wikipedia.org/wiki/Tagged_union">tagged union</a> with string tags.
 */
public final class CodecSubtype<T> implements WithInitializer<CodecSubtype<T>>, StructuredCodec<T> {
	private final Map<String, StructuredCodec<? extends T>> namesToAdapters = new HashMap<>();
	private final Map<Type, String> subtypesToNames = new HashMap<>();

	@Nullable
	private String tagName = null;
	private String dataName = "data";

	private CodecSubtype() {
	}

	public static <T> CodecSubtype<T> create() {
		return new CodecSubtype<>();
	}

	public CodecSubtype<T> withTagName(String tagName) {
		this.tagName = tagName;
		return this;
	}

	public CodecSubtype<T> withTagName(String tagName, String dataName) {
		this.tagName = tagName;
		this.dataName = dataName;
		return this;
	}

	/**
	 * Add a subtype along with its codec and custom string tag
	 */
	@SuppressWarnings("unchecked")
	public CodecSubtype<T> with(Type type, String name, StructuredCodec<? extends T> adapter) {
		namesToAdapters.put(name, adapter);
		subtypesToNames.put(type, name);
		if (adapter instanceof CodecSubtype) {
			namesToAdapters.putAll(((CodecSubtype<? extends T>) adapter).namesToAdapters);
			subtypesToNames.putAll(((CodecSubtype<? extends T>) adapter).subtypesToNames);
		}
		return this;
	}

	/**
	 * Add a subtype along with its codec and string tag which is extracted from the class name
	 */
	public CodecSubtype<T> with(Type type, StructuredCodec<? extends T> adapter) {
		String name = type.getTypeName();
		name = name.substring(max(name.lastIndexOf('.'), name.lastIndexOf('$')) + 1);
		return with(type, name, adapter);
	}

	@SuppressWarnings({"unchecked"})
	@Override
	public void encode(StructuredOutput out, T value) {
		out.writeObject(() -> {
			String tag = subtypesToNames.get(value.getClass());
			if (tag == null) {
				throw new IllegalArgumentException("Unregistered data type: " + value.getClass().getName());
			}
			StructuredCodec<T> codec = (StructuredCodec<T>) namesToAdapters.get(tag);

			if (tagName != null) {
				out.writeKey(tagName);
				out.writeString(tag);
				out.writeKey(dataName);
			} else {
				out.writeKey(tag);
			}
			codec.encode(out, value);
		});
	}

	@Override
	public T decode(StructuredInput in) throws MalformedDataException {
		return in.readObject($ -> {
			String key;
			if (tagName != null) {
				in.readKey(tagName);
				key = in.readString();
				in.readKey(dataName);
			} else {
				key = in.readKey();
			}

			StructuredCodec<? extends T> codec = namesToAdapters.get(key);
			if (codec == null) {
				throw new MalformedDataException("Could not find codec for: " + key);
			}
			return codec.decode(in);
		});
	}
}
