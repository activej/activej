package io.activej.json;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Iterator;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.common.Checks.checkNotNull;

public abstract class AbstractArrayJsonCodec<T, A, V> implements JsonCodec<T> {
	protected abstract Iterator<V> iterate(T item);

	protected abstract @Nullable JsonEncoder<V> encoder(int index, T item, V value);

	protected abstract @Nullable JsonDecoder<V> decoder(int index, A accumulator) throws JsonValidationException;

	protected abstract A accumulator();

	protected abstract void accumulate(A accumulator, int index, V value) throws JsonValidationException;

	protected abstract T result(A accumulator, int count) throws JsonValidationException;

	@Override
	public void write(JsonWriter writer, T item) {
		checkNotNull(item);
		writer.writeByte(ARRAY_START);
		boolean comma = false;
		int i = 0;
		Iterator<V> iterator = iterate(item);
		while (iterator.hasNext()) {
			V value = iterator.next();
			JsonEncoder<V> encoder = encoder(i++, item, value);
			if (encoder == null) continue;
			if (comma) writer.writeByte(COMMA);
			encoder.write(writer, value);
			comma = true;
		}
		writer.writeByte(ARRAY_END);
	}

	@Override
	public T read(JsonReader<?> reader) throws IOException {
		if (reader.last() != ARRAY_START) throw reader.newParseError("Expected '['");
		A accumulator = this.accumulator();
		int i = 0;
		if (reader.getNextToken() != ARRAY_END) {
			while (true) {
				JsonDecoder<V> decoder = decoder(i, accumulator);
				if (decoder == null) {
					reader.skip();
					continue;
				}
				V value = decoder.read(reader);
				accumulate(accumulator, i++, value);
				if (reader.getNextToken() != COMMA) break;
				reader.getNextToken();
			}
			reader.checkArrayEnd();
		}
		return result(accumulator, i);
	}
}
