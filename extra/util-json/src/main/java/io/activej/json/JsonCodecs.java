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

package io.activej.json;

import com.dslplatform.json.BoolConverter;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.NumberConverter;
import io.activej.common.annotation.StaticFactories;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.collection.CollectionUtils.newLinkedHashMap;
import static io.activej.common.collection.IteratorUtils.transformIterator;
import static io.activej.json.JsonKeyCodec.ofStringKey;
import static io.activej.json.JsonValidationUtils.validateNotNull;

@SuppressWarnings({"ConstantConditions", "unchecked"})
@StaticFactories(JsonCodec.class)
public class JsonCodecs {

	public static JsonCodec<String> ofString() {
		return new JsonCodecString();
	}

	public static JsonCodec<Byte> ofByte() {
		return new JsonCodecByte();
	}

	public static JsonCodec<Short> ofShort() {
		return new JsonCodecShort();
	}

	public static JsonCodec<Integer> ofInteger() {
		return new JsonCodecInteger();
	}

	public static JsonCodec<Long> ofLong() {
		return new JsonCodecLong();
	}

	public static JsonCodec<Float> ofFloat() {
		return new JsonCodecFloat();
	}

	public static JsonCodec<Double> ofDouble() {
		return new JsonCodecDouble();
	}

	public static JsonCodec<Boolean> ofBoolean() {
		return new JsonCodecBoolean();
	}

	public static JsonCodec<Character> ofCharacter() {
		return new JsonCodecCharacter();
	}

	public static JsonCodec<LocalDate> ofLocalDate() {
		return new JsonCodecLocalDate();
	}

	private static final class JsonCodecString implements JsonCodec<String> {
		@Override
		public String read(JsonReader<?> reader) throws IOException {
			return validateNotNull(reader.readString());
		}

		@Override
		public void write(JsonWriter writer, String value) {
			writer.writeString(checkNotNull(value));
		}
	}

	private static final class JsonCodecByte implements JsonCodec<Byte> {
		@Override
		public Byte read(JsonReader<?> reader) throws IOException {
			int result = NumberConverter.deserializeInt(reader);
			if (result < 0 || result > 255) {
				throw reader.newParseError("Read an int not in range [0, 255] while trying to read a byte");
			}
			return (byte) result;
		}

		@Override
		public void write(JsonWriter writer, Byte value) {
			NumberConverter.serialize(checkNotNull(value) & 0xFF, writer);
		}
	}

	private static final class JsonCodecShort implements JsonCodec<Short> {
		@Override
		public Short read(JsonReader<?> reader) throws IOException {
			return NumberConverter.deserializeShort(reader);
		}

		@Override
		public void write(JsonWriter writer, Short value) {
			NumberConverter.serialize(checkNotNull(value), writer);
		}
	}

	private static final class JsonCodecInteger implements JsonCodec<Integer> {
		@Override
		public Integer read(JsonReader<?> reader) throws IOException {
			return NumberConverter.deserializeInt(reader);
		}

		@Override
		public void write(JsonWriter writer, Integer value) {
			NumberConverter.serialize(checkNotNull(value), writer);
		}
	}

	private static final class JsonCodecLong implements JsonCodec<Long> {
		@Override
		public Long read(JsonReader<?> reader) throws IOException {
			return NumberConverter.deserializeLong(reader);
		}

		@Override
		public void write(JsonWriter writer, Long value) {
			NumberConverter.serialize(checkNotNull(value), writer);
		}
	}

	private static final class JsonCodecFloat implements JsonCodec<Float> {
		@Override
		public Float read(JsonReader<?> reader) throws IOException {
			return NumberConverter.deserializeFloat(reader);
		}

		@Override
		public void write(JsonWriter writer, Float value) {
			NumberConverter.serialize(checkNotNull(value), writer);
		}
	}

	private static final class JsonCodecDouble implements JsonCodec<Double> {
		@Override
		public Double read(JsonReader<?> reader) throws IOException {
			return NumberConverter.deserializeDouble(reader);
		}

		@Override
		public void write(JsonWriter writer, Double value) {
			NumberConverter.serialize(checkNotNull(value), writer);
		}
	}

	private static final class JsonCodecBoolean implements JsonCodec<Boolean> {
		@Override
		public Boolean read(JsonReader<?> reader) throws IOException {
			return BoolConverter.deserialize(reader);
		}

		@Override
		public void write(JsonWriter writer, Boolean value) {
			BoolConverter.serialize(checkNotNull(value), writer);
		}
	}

	private static final class JsonCodecCharacter implements JsonCodec<Character> {
		@Override
		public Character read(JsonReader<?> reader) throws IOException {
			String string = reader.readString();
			if (string.length() != 1) {
				throw reader.newParseError("Read a string with length != 1 while trying to read a character");
			}
			return string.charAt(0);
		}

		@Override
		public void write(JsonWriter writer, Character value) {
			writer.writeString(checkNotNull(value).toString());
		}
	}

	private static final class JsonCodecLocalDate implements JsonCodec<LocalDate> {
		@Override
		public LocalDate read(JsonReader<?> reader) throws IOException {
			try {
				return LocalDate.parse(validateNotNull(reader.readString()));
			} catch (DateTimeParseException e) {
				throw reader.newParseError(e.getMessage());
			}
		}

		@Override
		public void write(JsonWriter writer, LocalDate value) {
			writer.writeString(checkNotNull(value).toString());
		}
	}

	public static <E extends Enum<E>> JsonCodec<E> ofEnum(Class<E> enumClass) {
		return new JsonCodec<>() {
			@Override
			public E read(JsonReader<?> reader) throws IOException {
				try {
					return Enum.valueOf(enumClass, reader.readString());
				} catch (IllegalArgumentException e) {
					throw reader.newParseError(e.getMessage());
				}
			}

			@Override
			public void write(JsonWriter writer, E value) {
				writer.writeString(checkNotNull(value).name());
			}
		};
	}

	static JsonCodec<Map<String, ?>> ofMapObject(Map<String, JsonCodec<?>> codecs) {
		return new AbstractMapJsonCodec<Map<String, ?>, LinkedHashMap<String, Object>, Object>() {
			@Override
			protected Iterator<JsonMapEntry<Object>> iterate(Map<String, ?> item) {
				checkArgument(item.size() == codecs.size());
				return item instanceof LinkedHashMap || !(codecs instanceof LinkedHashMap) ?
					transformIterator(item.entrySet().iterator(), entry -> (JsonMapEntry<Object>) JsonMapEntry.of(entry)) :
					transformIterator(codecs.keySet().iterator(), key -> {
						checkArgument(item.containsKey(key));
						return new JsonMapEntry<>(key, item.get(key));
					});
			}

			@Override
			protected @Nullable JsonEncoder<Object> encoder(String key, int index, Map<String, ?> item, Object value) {
				return (JsonEncoder<Object>) codecs.get(key);
			}

			@Override
			protected @Nullable JsonDecoder<Object> decoder(String key, int index, LinkedHashMap<String, Object> accumulator) throws JsonValidationException {
				JsonCodec<?> codec = codecs.get(key);
				if (codec == null) throw new JsonValidationException("Key not found: " + key);
				return (JsonDecoder<Object>) codec;
			}

			@Override
			protected LinkedHashMap<String, Object> accumulator() {
				return newLinkedHashMap(codecs.size());
			}

			@Override
			protected void accumulate(LinkedHashMap<String, Object> accumulator, String key, int index, Object value) throws JsonValidationException {
				if (index >= codecs.size()) throw new JsonValidationException();
				accumulator.put(key, value);
			}

			@Override
			protected Map<String, ?> result(LinkedHashMap<String, Object> accumulator, int count) throws JsonValidationException {
				if (count != codecs.size()) throw new JsonValidationException();
				return accumulator;
			}
		};
	}

	public static <V> JsonCodec<Map<String, V>> ofMap(Function<String, JsonCodec<V>> codecsFn) {
		return new AbstractMapJsonCodec<Map<String, V>, LinkedHashMap<String, V>, V>() {
			@Override
			protected Iterator<JsonMapEntry<V>> iterate(Map<String, V> item) {
				return transformIterator(item.entrySet().iterator(), JsonMapEntry::of);
			}

			@Override
			protected @Nullable JsonEncoder<V> encoder(String key, int index, Map<String, V> item, V value) {
				return codecsFn.apply(key);
			}

			@Override
			protected @Nullable JsonDecoder<V> decoder(String key, int index, LinkedHashMap<String, V> accumulator) throws JsonValidationException {
				JsonCodec<V> codec = codecsFn.apply(key);
				if (codec == null) throw new JsonValidationException("Key not found: " + key);
				return codec;
			}

			@Override
			protected LinkedHashMap<String, V> accumulator() {
				return new LinkedHashMap<>();
			}

			@Override
			protected void accumulate(LinkedHashMap<String, V> accumulator, String key, int index, V value) {
				accumulator.put(key, value);
			}

			@Override
			protected Map<String, V> result(LinkedHashMap<String, V> accumulator, int count) {
				return accumulator;
			}
		};
	}

	public static <T> JsonCodec<T[]> ofArray(JsonCodec<T> codec, Supplier<T[]> supplier) {
		return new AbstractArrayJsonCodec<T[], T[], T>() {
			@Override
			protected Iterator<T> iterate(T[] item) {
				return Arrays.asList(item).iterator();
			}

			@Override
			protected @Nullable JsonEncoder<T> encoder(int index, T[] item, T value) {
				return codec;
			}

			@Override
			protected @Nullable JsonDecoder<T> decoder(int index, T[] accumulator) {
				return codec;
			}

			@Override
			protected T[] accumulator() {
				return supplier.get();
			}

			@Override
			protected void accumulate(T[] accumulator, int index, T value) throws JsonValidationException {
				if (index >= accumulator.length) throw new JsonValidationException();
				accumulator[index] = value;
			}

			@Override
			protected T[] result(T[] accumulator, int count) throws JsonValidationException {
				if (accumulator.length != count) throw new JsonValidationException();
				return accumulator;
			}
		};
	}

	public static <T> JsonCodec<List<T>> ofList(JsonCodec<T> codec) {
		return new AbstractArrayJsonCodec<List<T>, ArrayList<T>, T>() {
			@Override
			protected Iterator<T> iterate(List<T> item) {
				return item.iterator();
			}

			@Override
			protected @Nullable JsonEncoder<T> encoder(int index, List<T> item, T value) {
				return codec;
			}

			@Override
			protected @Nullable JsonDecoder<T> decoder(int index, ArrayList<T> accumulator) {
				return codec;
			}

			@Override
			protected ArrayList<T> accumulator() {
				return new ArrayList<>();
			}

			@Override
			protected void accumulate(ArrayList<T> accumulator, int index, T value) {
				accumulator.add(value);
			}

			@Override
			protected List<T> result(ArrayList<T> accumulator, int count) {
				return accumulator;
			}
		};
	}

	public static <T> JsonCodec<Set<T>> ofSet(JsonCodec<T> codec) {
		return new AbstractArrayJsonCodec<Set<T>, LinkedHashSet<T>, T>() {
			@Override
			protected Iterator<T> iterate(Set<T> item) {
				return item.iterator();
			}

			@Override
			protected @Nullable JsonEncoder<T> encoder(int index, Set<T> item, T value) {
				return codec;
			}

			@Override
			protected @Nullable JsonDecoder<T> decoder(int index, LinkedHashSet<T> accumulator) {
				return codec;
			}

			@Override
			protected LinkedHashSet<T> accumulator() {
				return new LinkedHashSet<>();
			}

			@Override
			protected void accumulate(LinkedHashSet<T> accumulator, int index, T value) throws JsonValidationException {
				if (accumulator.contains(value)) throw new JsonValidationException();
				accumulator.add(value);
			}

			@Override
			protected Set<T> result(LinkedHashSet<T> accumulator, int count) {
				return accumulator;
			}
		};
	}

	public static JsonCodec<Object[]> ofArrayObject(JsonCodec<?>... codecs) {
		return new AbstractArrayJsonCodec<Object[], Object[], Object>() {
			@Override
			protected Iterator<Object> iterate(Object[] item) {
				checkArgument(item.length == codecs.length);
				return Arrays.asList(item).iterator();
			}

			@Override
			protected @Nullable JsonEncoder<Object> encoder(int index, Object[] item, Object value) {
				return (JsonEncoder<Object>) codecs[index];
			}

			@Override
			protected @Nullable JsonDecoder<Object> decoder(int index, Object[] accumulator) throws JsonValidationException {
				if (index >= accumulator.length) throw new JsonValidationException();
				return (JsonDecoder<Object>) codecs[index];
			}

			@Override
			protected Object[] accumulator() {
				return new Object[codecs.length];
			}

			@Override
			protected void accumulate(Object[] accumulator, int index, Object value) {
				accumulator[index] = value;
			}

			@Override
			protected Object[] result(Object[] accumulator, int count) throws JsonValidationException {
				if (count != codecs.length) throw new JsonValidationException();
				return accumulator;
			}
		};
	}

	public static <V> JsonCodec<Map<String, V>> ofMap(JsonCodec<V> codec) {
		return ofMap(ofStringKey(), codec);
	}

	public static <K, V> JsonCodec<Map<K, V>> ofMap(JsonKeyCodec<K> keyCodec, JsonCodec<V> codec) {
		return new AbstractMapJsonCodec<Map<K, V>, LinkedHashMap<K, V>, V>() {
			@Override
			protected Iterator<JsonMapEntry<V>> iterate(Map<K, V> item) {
				return transformIterator(item.entrySet().iterator(), entry -> JsonMapEntry.of(entry, keyCodec));
			}

			@Override
			protected @Nullable JsonEncoder<V> encoder(String key, int index, Map<K, V> item, V value) {
				return codec;
			}

			@Override
			protected @Nullable JsonDecoder<V> decoder(String key, int index, LinkedHashMap<K, V> accumulator) {
				return codec;
			}

			@Override
			protected LinkedHashMap<K, V> accumulator() {
				return new LinkedHashMap<>();
			}

			@Override
			protected void accumulate(LinkedHashMap<K, V> accumulator, String key, int index, V value) throws JsonValidationException {
				accumulator.put(keyCodec.decode(key), value);
			}

			@Override
			protected Map<K, V> result(LinkedHashMap<K, V> accumulator, int count) {
				return accumulator;
			}
		};
	}

	public static <T> JsonCodec<@Nullable T> ofNullable(JsonCodec<T> codec) {
		return new NullableJsonCodec<>(codec);
	}

	public static <T, F1> JsonCodec<T> ofObject(JsonConstructor1<F1, T> constructor,
		String field1, Function<T, F1> getter1, JsonCodec<F1> codec1
	) {
		//noinspection unchecked,rawtypes
		return ObjectJsonCodec.builder(params ->
				(T) ((JsonConstructor1) constructor).create(params[0]))
			.with(field1, getter1, codec1)
			.build();
	}

	public static <T, F1, F2> JsonCodec<T> ofObject(JsonConstructor2<F1, F2, T> constructor,
		String field1, Function<T, F1> getter1, JsonCodec<F1> codec1,
		String field2, Function<T, F2> getter2, JsonCodec<F2> codec2
	) {
		//noinspection unchecked,rawtypes
		return ObjectJsonCodec.builder(params ->
				(T) ((JsonConstructor2) constructor).create(params[0], params[1]))
			.with(field1, getter1, codec1)
			.with(field2, getter2, codec2)
			.build();
	}

	public static <T, F1, F2, F3> JsonCodec<T> ofObject(JsonConstructor3<F1, F2, F3, T> constructor,
		String field1, Function<T, F1> getter1, JsonCodec<F1> codec1,
		String field2, Function<T, F2> getter2, JsonCodec<F2> codec2,
		String field3, Function<T, F3> getter3, JsonCodec<F3> codec3
	) {
		//noinspection unchecked,rawtypes
		return ObjectJsonCodec.builder(params ->
				(T) ((JsonConstructor3) constructor).create(params[0], params[1], params[2]))
			.with(field1, getter1, codec1)
			.with(field2, getter2, codec2)
			.with(field3, getter3, codec3)
			.build();
	}

	public static <T, F1, F2, F3, F4> JsonCodec<T> ofObject(JsonConstructor4<F1, F2, F3, F4, T> constructor,
		String field1, Function<T, F1> getter1, JsonCodec<F1> codec1,
		String field2, Function<T, F2> getter2, JsonCodec<F2> codec2,
		String field3, Function<T, F3> getter3, JsonCodec<F3> codec3,
		String field4, Function<T, F4> getter4, JsonCodec<F4> codec4
	) {
		//noinspection unchecked,rawtypes
		return ObjectJsonCodec.builder(params ->
				(T) ((JsonConstructor4) constructor).create(params[0], params[1], params[2], params[3]))
			.with(field1, getter1, codec1)
			.with(field2, getter2, codec2)
			.with(field3, getter3, codec3)
			.with(field4, getter4, codec4)
			.build();
	}

	public static <T, F1, F2, F3, F4, F5> JsonCodec<T> ofObject(JsonConstructor5<F1, F2, F3, F4, F5, T> constructor,
		String field1, Function<T, F1> getter1, JsonCodec<F1> codec1,
		String field2, Function<T, F2> getter2, JsonCodec<F2> codec2,
		String field3, Function<T, F3> getter3, JsonCodec<F3> codec3,
		String field4, Function<T, F4> getter4, JsonCodec<F4> codec4,
		String field5, Function<T, F5> getter5, JsonCodec<F5> codec5
	) {
		//noinspection unchecked,rawtypes
		return ObjectJsonCodec.builder(params ->
				(T) ((JsonConstructor5) constructor).create(params[0], params[1], params[2], params[3], params[4]))
			.with(field1, getter1, codec1)
			.with(field2, getter2, codec2)
			.with(field3, getter3, codec3)
			.with(field4, getter4, codec4)
			.with(field5, getter5, codec5)
			.build();
	}

	public static <T, F1, F2, F3, F4, F5, F6> JsonCodec<T> ofObject(JsonConstructor6<F1, F2, F3, F4, F5, F6, T> constructor,
		String field1, Function<T, F1> getter1, JsonCodec<F1> codec1,
		String field2, Function<T, F2> getter2, JsonCodec<F2> codec2,
		String field3, Function<T, F3> getter3, JsonCodec<F3> codec3,
		String field4, Function<T, F4> getter4, JsonCodec<F4> codec4,
		String field5, Function<T, F5> getter5, JsonCodec<F5> codec5,
		String field6, Function<T, F6> getter6, JsonCodec<F6> codec6
	) {
		//noinspection unchecked,rawtypes
		return ObjectJsonCodec.builder(params ->
				(T) ((JsonConstructor6) constructor).create(params[0], params[1], params[2], params[3], params[4], params[5]))
			.with(field1, getter1, codec1)
			.with(field2, getter2, codec2)
			.with(field3, getter3, codec3)
			.with(field4, getter4, codec4)
			.with(field5, getter5, codec5)
			.with(field6, getter6, codec6)
			.build();
	}

	public static <T, R> JsonCodec<R> transform(JsonCodec<T> codec, Function<R, T> to, JsonFunction<T, R> from) {
		return new JsonCodec<>() {
			@Override
			public R read(JsonReader<?> reader) throws IOException {
				return from.apply(codec.read(reader));
			}

			@Override
			public void write(JsonWriter writer, R value) {
				codec.write(writer, to.apply(value));
			}
		};
	}

	public static class NullableJsonCodec<T> implements JsonCodec<T> {
		private final JsonCodec<T> codec;

		public NullableJsonCodec(JsonCodec<T> codec) {this.codec = codec;}

		@Override
		public @Nullable T read(JsonReader<?> reader) throws IOException {
			if (reader.wasNull()) return null;
			return codec.read(reader);
		}

		@Override
		public void write(JsonWriter writer, @Nullable T value) {
			if (value == null) writer.writeNull();
			else codec.write(writer, value);
		}
	}
}
