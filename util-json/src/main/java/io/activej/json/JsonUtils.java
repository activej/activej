package io.activej.json;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import io.activej.common.exception.MalformedDataException;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonUtils {
	private static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(DSL_JSON::newReader);

	public static <T> String toJson(JsonCodec<T> codec, T object) {
		return toJsonWriter(codec, object).toString();
	}

	public static <T> byte[] toJsonBytes(JsonCodec<T> codec, T object) {
		return toJsonWriter(codec, object).toByteArray();
	}

	public static <T> JsonWriter toJsonWriter(JsonCodec<T> codec, T object) {
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		codec.write(jsonWriter, object);
		return jsonWriter;
	}

	public static <T> T fromJson(JsonCodec<T> codec, String json) throws MalformedDataException {
		return fromJsonBytes(codec, json.getBytes(UTF_8));
	}

	public static <T> T fromJsonBytes(JsonCodec<T> codec, byte[] bytes) throws MalformedDataException {
		JsonReader<?> jsonReader = READERS.get().process(bytes, bytes.length);
		return fromJsonReader(codec, jsonReader);
	}

	private static <T> T fromJsonReader(JsonCodec<T> codec, JsonReader<?> jsonReader) throws MalformedDataException {
		try {
			jsonReader.getNextToken();
			T deserialized = codec.read(jsonReader);
			if (jsonReader.length() != jsonReader.getCurrentIndex()) {
				String unexpectedData = jsonReader.toString().substring(jsonReader.getCurrentIndex());
				throw new MalformedDataException("Unexpected JSON data: " + unexpectedData);
			}
			return deserialized;
		} catch (ParsingException | JsonValidationException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}



}
