package io.activej.fs.util;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.types.TypeT;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;

public final class JsonUtils {
	private static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(DSL_JSON::newReader);

	public static <T> ByteBuf toJson(@Nullable T object) {
		if (object == null) return ByteBuf.wrap(new byte[]{'n', 'u', 'l', 'l'}, 0, 4);
		return toJson(object.getClass(), object);
	}

	public static <T> ByteBuf toJson(Type manifest, @Nullable T object) {
		if (object == null) return ByteBuf.wrap(new byte[]{'n', 'u', 'l', 'l'}, 0, 4);
		JsonWriter writer = toJsonWriter(manifest, object);
		return ByteBuf.wrapForReading(writer.toByteArray());
	}

	public static <T> ByteBuf toJson(Class<? super T> manifest, @Nullable T object) {
		return toJson((Type) manifest, object);
	}

	private static <T> JsonWriter toJsonWriter(Type manifest, @Nullable T object) {
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		if (!DSL_JSON.serialize(jsonWriter, manifest, object)) {
			throw new IllegalArgumentException("Cannot serialize " + manifest);
		}
		return jsonWriter;
	}

	public static <T> T fromJson(Class<T> type, ByteBuf buf) throws MalformedDataException {
		return fromJson((Type) type, buf);
	}

	public static <T> T fromJson(TypeT<T> type, ByteBuf buf) throws MalformedDataException {
		return fromJson(type.getType(), buf);
	}

	public static <T> T fromJson(Type manifest, ByteBuf buf) throws MalformedDataException {
		byte[] bytes = buf.getArray();
		return fromJson(manifest, bytes);
	}

	public static <T> T fromJson(TypeT<T> type, byte [] bytes) throws MalformedDataException {
		return fromJson(type.getType(), bytes);
	}

	public static <T> T fromJson(Type manifest, byte[] bytes) throws MalformedDataException {
		try {
			//noinspection unchecked
			JsonReader.ReadObject<T> readObject = (JsonReader.ReadObject<T>) DSL_JSON.tryFindReader(manifest);
			if (readObject == null) {
				throw new IllegalArgumentException("Unknown type: " + manifest);
			}
			JsonReader<?> jsonReader = READERS.get().process(bytes, bytes.length);
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
}
