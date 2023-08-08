package io.activej.json;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;

import java.io.IOException;

public abstract class ForwardingJsonCodec<T> implements JsonCodec<T> {
	protected final JsonCodec<T> codec;

	protected ForwardingJsonCodec(JsonCodec<T> codec) {this.codec = codec;}

	@Override
	public T read(JsonReader<?> reader) throws IOException {
		return codec.read(reader);
	}

	@Override
	public void write(JsonWriter writer, T value) {
		codec.write(writer, value);
	}
}
