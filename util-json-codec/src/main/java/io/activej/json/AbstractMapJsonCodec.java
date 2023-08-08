package io.activej.json;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.common.Checks.checkNotNull;

@SuppressWarnings("unchecked")
public abstract class AbstractMapJsonCodec<T, A> implements JsonCodec<T> {
	public record JsonMapEntry<V>(String key, V value) {
		public JsonMapEntry {}

		public static <V> JsonMapEntry<V> of(Map.Entry<String, V> mapEntry) {
			return new JsonMapEntry<>(mapEntry.getKey(), mapEntry.getValue());
		}

		public static <K, V> JsonMapEntry<V> of(Map.Entry<K, V> mapEntry, JsonKeyEncoder<K> keyEncoder) {
			return new JsonMapEntry<>(keyEncoder.encode(mapEntry.getKey()), mapEntry.getValue());
		}
	}

	protected abstract Iterator<JsonMapEntry<?>> iterate(T item);

	protected abstract @Nullable JsonEncoder<?> encoder(String key, int index, T item, Object value);

	protected abstract @Nullable JsonDecoder<?> decoder(String key, int index, A accumulator) throws JsonValidationException;

	protected abstract A accumulator();

	protected abstract void accumulate(A accumulator, String key, int index, Object value) throws JsonValidationException;

	protected abstract T result(A accumulator, int count) throws JsonValidationException;

	@Override
	public final void write(JsonWriter writer, T item) {
		checkNotNull(item);
		writer.writeByte(OBJECT_START);
		boolean comma = false;
		Iterator<JsonMapEntry<?>> iterator = iterate(item);
		int i = 0;
		while (iterator.hasNext()) {
			JsonMapEntry<?> entry = iterator.next();
			if (comma) writer.writeByte(COMMA);
			String key = entry.key;
			Object value = entry.value;
			JsonEncoder<Object> encoder = (JsonEncoder<Object>) encoder(key, i++, item, value);
			if (encoder == null) continue;
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
				JsonDecoder<Object> decoder = (JsonDecoder<Object>) decoder(key, i, accumulator);
				if (decoder == null) {
					reader.skip();
					continue;
				}
				Object value = decoder.read(reader);
				accumulate(accumulator, key, i++, value);
				if (reader.getNextToken() != COMMA) break;
				reader.getNextToken();
			}
			reader.checkObjectEnd();
		}
		return result(accumulator, i);
	}
}
