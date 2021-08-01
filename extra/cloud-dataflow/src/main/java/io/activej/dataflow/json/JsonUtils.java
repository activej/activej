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

import com.dslplatform.json.*;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter.WriteObject;
import com.dslplatform.json.runtime.Settings;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.dataflow.graph.StreamId;
import io.activej.inject.Key;
import io.activej.inject.util.Constructors;
import io.activej.types.Types;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Instant;

import static com.dslplatform.json.JsonWriter.OBJECT_END;
import static com.dslplatform.json.JsonWriter.OBJECT_START;
import static com.dslplatform.json.NumberConverter.LONG_READER;
import static com.dslplatform.json.NumberConverter.LONG_WRITER;
import static io.activej.types.Types.parameterizedType;

public final class JsonUtils {
	static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());

	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(DSL_JSON::newReader);

	public static <T> ByteBuf toJsonBuf(@NotNull WriteObject<? super T> writeObject, @Nullable T object) {
		return ByteBuf.wrapForReading(toJsonWriter(writeObject, object).toByteArray());
	}

	public static <T> String toJson(@NotNull WriteObject<? super T> writeObject, @Nullable T object) {
		return toJsonWriter(writeObject, object).toString();
	}

	private static <T> JsonWriter toJsonWriter(@NotNull WriteObject<? super T> writeObject, @Nullable T object) {
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		writeObject.write(jsonWriter, object);
		return jsonWriter;
	}

	public static <T> T fromJson(@NotNull ReadObject<? extends T> readObject, @NotNull ByteBuf json) throws MalformedDataException {
		return fromJson(readObject, json.asArray());
	}

	private static <T> T fromJson(@NotNull ReadObject<? extends T> readObject, byte[] jsonBytes) throws MalformedDataException {
		try {
			JsonReader<?> jsonReader = READERS.get();
			jsonReader.process(jsonBytes, jsonBytes.length);
			jsonReader.getNextToken();
			T deserialized = readObject.read(jsonReader);
			if (jsonReader.length() != jsonReader.getCurrentIndex()) {
				String unexpectedData = jsonReader.toString().substring(jsonReader.getCurrentIndex());
				throw new MalformedDataException("Unexpected JSON data: " + unexpectedData);
			}
			return deserialized;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	public static <T> Key<JsonCodec<T>> codec(Class<? extends T> aClass) {
		return Key.ofType(parameterizedType(JsonCodec.class, aClass));
	}

	public static <T> JsonCodec<T> ofObject(Constructors.Constructor0<T> constructor) {
		return JsonCodec.of(
				reader -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					reader.endObject();
					return constructor.create();
				},
				(writer, value) -> {
					writer.writeByte(OBJECT_START);
					writer.writeByte(OBJECT_END);
				});
	}

	@JsonConverter(target = StreamId.class)
	public static class StreamIdConverter {
		public static final ReadObject<StreamId> JSON_READER = reader -> new StreamId(NumberConverter.deserializeLong(reader));
		public static final WriteObject<StreamId> JSON_WRITER = (writer, value) -> {
			if (value == null) {
				writer.writeNull();
			} else {
				NumberConverter.serialize(value.getId(), writer);
			}
		};
	}

	@SuppressWarnings("ConstantConditions")
	@JsonConverter(target = Instant.class)
	public static class InstantConverter {
		public static final ReadObject<Instant> JSON_READER = reader -> Instant.ofEpochMilli(LONG_READER.read(reader));
		public static final WriteObject<Instant> JSON_WRITER = (writer, value) -> LONG_WRITER.write(writer, value.toEpochMilli());
	}
}
