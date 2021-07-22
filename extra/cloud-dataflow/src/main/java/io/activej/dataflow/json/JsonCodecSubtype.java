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

package io.activej.dataflow.json;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;
import static java.lang.Math.max;

final class JsonCodecSubtype<T> implements JsonCodec<T> {
	private final Map<String, JsonCodec<? extends T>> namesToAdapters = new HashMap<>();
	private final Map<Class<? extends T>, String> subtypesToNames = new HashMap<>();

	private JsonCodecSubtype() {
	}

	public static <T> JsonCodecSubtype<T> create() {
		return new JsonCodecSubtype<>();
	}

	@SuppressWarnings("unchecked")
	public void setSubtypeCodec(Class<? extends T> type, String name, JsonCodec<? extends T> codec) {
		namesToAdapters.put(name, codec);
		subtypesToNames.put(type, name);
		if (codec instanceof JsonCodecSubtype) {
			namesToAdapters.putAll(((JsonCodecSubtype<? extends T>) codec).namesToAdapters);
			subtypesToNames.putAll(((JsonCodecSubtype<? extends T>) codec).subtypesToNames);
		}
	}

	public void setSubtypeCodec(Class<? extends T> type, JsonCodec<? extends T> codec) {
		String name = type.getTypeName();
		name = name.substring(max(name.lastIndexOf('.'), name.lastIndexOf('$')) + 1);
		setSubtypeCodec(type, name, codec);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(@NotNull JsonWriter writer, T value) {
		if (value == null) {
			writer.writeNull();
			return;
		}

		writer.writeByte(OBJECT_START);
		String tag = subtypesToNames.get(value.getClass());
		if (tag == null) {
			throw new IllegalArgumentException("Unregistered data type: " + value.getClass().getName());
		}
		JsonCodec<T> codec = (JsonCodec<T>) namesToAdapters.get(tag);
		writer.writeString(tag);
		writer.writeByte(SEMI);
		codec.write(writer, value);

		writer.writeByte(OBJECT_END);
	}

	@Override
	public T read(@NotNull JsonReader reader) throws IOException {
		if (reader.wasNull()) return null;

		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();
		String key = reader.readKey();
		JsonCodec<? extends T> codec = namesToAdapters.get(key);
		if (codec == null) {
			throw ParsingException.create("Could not find codec for: " + key, true);
		}
		T result = codec.read(reader);
		reader.endObject();
		return result;
	}
}
