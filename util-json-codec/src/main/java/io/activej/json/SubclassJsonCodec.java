package io.activej.json;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import io.activej.common.builder.AbstractBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;

public final class SubclassJsonCodec<T> implements JsonCodec<T> {
	private final Map<String, JsonCodec<? extends T>> tagToCodec = new HashMap<>();
	private final Map<Class<? extends T>, String> classToTag = new HashMap<>();

	private SubclassJsonCodec() {
	}

	public static <T> SubclassJsonCodec<T>.Builder builder() {
		return new SubclassJsonCodec<T>().new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, SubclassJsonCodec<T>> {
		private Builder() {}

		public <S extends T> Builder with(Class<S> type, JsonCodec<S> codec) {
			return with(type, type.getSimpleName(), codec);
		}

		public <S extends T> Builder with(Class<S> type, String name, JsonCodec<S> codec) {
			tagToCodec.put(name, codec);
			classToTag.put(type, name);
			if (codec instanceof SubclassJsonCodec) {
				tagToCodec.putAll(((SubclassJsonCodec<? extends T>) codec).tagToCodec);
				classToTag.putAll(((SubclassJsonCodec<? extends T>) codec).classToTag);
			}
			return this;
		}

		@Override
		protected SubclassJsonCodec<T> doBuild() {
			return SubclassJsonCodec.this;
		}
	}

	@Override
	public void write(JsonWriter writer, T value) {
		writer.writeByte(OBJECT_START);
		String tag = classToTag.get(value.getClass());
		writer.writeString(tag);
		writer.writeByte(SEMI);

		//noinspection unchecked
		JsonCodec<T> codec = (JsonCodec<T>) tagToCodec.get(tag);

		codec.write(writer, value);

		writer.writeByte(OBJECT_END);
	}

	@Override
	public T read(JsonReader<?> reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();
		String key = reader.readKey();
		JsonCodec<? extends T> codec = tagToCodec.get(key);
		if (codec == null) throw reader.newParseError("Unexpected key: '" + key + "'");
		T result = codec.read(reader);
		if (reader.getNextToken() != OBJECT_END) throw reader.newParseError("Expected '}'");
		return result;
	}
}
