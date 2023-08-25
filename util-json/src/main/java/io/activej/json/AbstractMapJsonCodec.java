package io.activej.json;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.common.Checks.checkNotNull;

public abstract class AbstractMapJsonCodec<T, A, V> implements JsonCodec<T> {
	public record JsonMapEntry<V>(String key, V value) {
		public static <V> JsonMapEntry<V> of(Map.Entry<String, V> mapEntry) {
			return new JsonMapEntry<>(mapEntry.getKey(), mapEntry.getValue());
		}

		public static <K, V> JsonMapEntry<V> of(Map.Entry<K, V> mapEntry, JsonKeyEncoder<K> keyEncoder) {
			return new JsonMapEntry<>(keyEncoder.encode(mapEntry.getKey()), mapEntry.getValue());
		}
	}

	protected abstract Iterator<JsonMapEntry<V>> iterate(T item);

	protected abstract @Nullable JsonEncoder<V> encoder(String key, int index, T item, V value);

	protected abstract @Nullable JsonDecoder<V> decoder(String key, int index, A accumulator) throws JsonValidationException;

	protected abstract A accumulator();

	protected abstract void accumulate(A accumulator, String key, int index, V value) throws JsonValidationException;

	protected abstract T result(A accumulator, int count) throws JsonValidationException;

	@Override
	public final void write(JsonWriter writer, T item) {
		checkNotNull(item);
		writer.writeByte(OBJECT_START);
		boolean comma = false;
		Iterator<JsonMapEntry<V>> iterator = iterate(item);
		int i = 0;
		while (iterator.hasNext()) {
			JsonMapEntry<V> entry = iterator.next();
			String key = entry.key;
			V value = entry.value;
			JsonEncoder<V> encoder = encoder(key, i++, item, value);
			if (encoder == null) continue;
			if (comma) writer.writeByte(COMMA);
			writer.writeString(key);
			writer.writeByte(SEMI);
			encoder.write(writer, value);
			comma = true;
		}
		writer.writeByte(OBJECT_END);
	}

	@Override
	public final T read(JsonReader<?> reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		A accumulator = accumulator();
		int i = 0;
		if (reader.getNextToken() != OBJECT_END) {
			while (true) {
				String key = reader.readKey();
				JsonDecoder<V> decoder = decoder(key, i, accumulator);
				if (decoder == null) {
					reader.skip();
					continue;
				}
				V value = decoder.read(reader);
				accumulate(accumulator, key, i++, value);
				if (reader.getNextToken() != COMMA) break;
				reader.getNextToken();
			}
			reader.checkObjectEnd();
		}
		return result(accumulator, i);
	}
}
