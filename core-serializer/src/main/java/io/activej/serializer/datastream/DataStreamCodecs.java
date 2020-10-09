package io.activej.serializer.datastream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.IntFunction;

public final class DataStreamCodecs {

	public static DataStreamCodec<Boolean> ofBoolean() {
		return new DataStreamCodec<Boolean>() {
			@Override
			public void encode(DataOutputStreamEx stream, Boolean item) throws IOException {
				stream.writeBoolean(item);
			}

			@Override
			public Boolean decode(DataInputStreamEx stream) throws IOException {
				return stream.readBoolean();
			}
		};
	}

	public static DataStreamCodec<Character> ofChar() {
		return new DataStreamCodec<Character>() {
			@Override
			public void encode(DataOutputStreamEx stream, Character item) throws IOException {
				stream.writeChar(item);
			}

			@Override
			public Character decode(DataInputStreamEx stream) throws IOException {
				return stream.readChar();
			}
		};
	}

	public static DataStreamCodec<Byte> ofByte() {
		return new DataStreamCodec<Byte>() {
			@Override
			public void encode(DataOutputStreamEx stream, Byte item) throws IOException {
				stream.writeByte(item);
			}

			@Override
			public Byte decode(DataInputStreamEx stream) throws IOException {
				return stream.readByte();
			}
		};
	}

	public static DataStreamCodec<Short> ofShort() {
		return new DataStreamCodec<Short>() {
			@Override
			public void encode(DataOutputStreamEx stream, Short item) throws IOException {
				stream.writeShort(item);
			}

			@Override
			public Short decode(DataInputStreamEx stream) throws IOException {
				return stream.readShort();
			}
		};
	}

	public static DataStreamCodec<Integer> ofInt() {
		return new DataStreamCodec<Integer>() {
			@Override
			public void encode(DataOutputStreamEx stream, Integer item) throws IOException {
				stream.writeInt(item);
			}

			@Override
			public Integer decode(DataInputStreamEx stream) throws IOException {
				return stream.readInt();
			}
		};
	}

	public static DataStreamCodec<Integer> ofVarInt() {
		return new DataStreamCodec<Integer>() {
			@Override
			public void encode(DataOutputStreamEx stream, Integer item) throws IOException {
				stream.writeVarInt(item);
			}

			@Override
			public Integer decode(DataInputStreamEx stream) throws IOException {
				return stream.readVarInt();
			}
		};
	}

	public static DataStreamCodec<Long> ofLong() {
		return new DataStreamCodec<Long>() {
			@Override
			public void encode(DataOutputStreamEx stream, Long item) throws IOException {
				stream.writeLong(item);
			}

			@Override
			public Long decode(DataInputStreamEx stream) throws IOException {
				return stream.readLong();
			}
		};
	}

	public static DataStreamCodec<Long> ofVarLong() {
		return new DataStreamCodec<Long>() {
			@Override
			public void encode(DataOutputStreamEx stream, Long item) throws IOException {
				stream.writeVarLong(item);
			}

			@Override
			public Long decode(DataInputStreamEx stream) throws IOException {
				return stream.readVarLong();
			}
		};
	}

	public static DataStreamCodec<Float> ofFloat() {
		return new DataStreamCodec<Float>() {
			@Override
			public void encode(DataOutputStreamEx stream, Float item) throws IOException {
				stream.writeFloat(item);
			}

			@Override
			public Float decode(DataInputStreamEx stream) throws IOException {
				return stream.readFloat();
			}
		};
	}

	public static DataStreamCodec<Double> ofDouble() {
		return new DataStreamCodec<Double>() {
			@Override
			public void encode(DataOutputStreamEx stream, Double item) throws IOException {
				stream.writeDouble(item);
			}

			@Override
			public Double decode(DataInputStreamEx stream) throws IOException {
				return stream.readDouble();
			}
		};
	}

	public static DataStreamCodec<String> ofString() {
		return new DataStreamCodec<String>() {
			@Override
			public void encode(DataOutputStreamEx stream, @NotNull String item) throws IOException {
				stream.writeString(item);
			}

			@Override
			public @NotNull String decode(DataInputStreamEx stream) throws IOException {
				return stream.readString();
			}
		};
	}

	public static <E extends Enum<E>> DataStreamCodec<E> ofEnum(Class<E> enumType) {
		return new DataStreamCodec<E>() {
			@Override
			public void encode(DataOutputStreamEx out, E value) throws IOException {
				out.writeVarInt(value.ordinal());
			}

			@Override
			public E decode(DataInputStreamEx in) throws IOException {
				return enumType.getEnumConstants()[in.readVarInt()];
			}
		};
	}

	public static <T> DataStreamCodec<Optional<T>> ofOptional(DataStreamCodec<T> codec) {
		return new DataStreamCodec<Optional<T>>() {
			@Override
			public void encode(DataOutputStreamEx stream, Optional<T> item) throws IOException {
				if (!item.isPresent()) {
					stream.writeByte((byte) 0);
				} else {
					stream.writeByte((byte) 1);
					codec.encode(stream, item.get());
				}
			}

			@Override
			public Optional<T> decode(DataInputStreamEx in) throws IOException {
				if (in.readByte() == 0) return Optional.empty();
				return Optional.of(codec.decode(in));
			}
		};
	}

	public static <T> DataStreamCodec<List<T>> ofList(DataStreamCodec<T> itemCodec) {
		return ofList($ -> itemCodec);
	}

	public static <T> DataStreamCodec<List<T>> ofList(IntFunction<? extends DataStreamCodec<? extends T>> itemCodecFn) {
		return new DataStreamCodec<List<T>>() {
			@Override
			public void encode(DataOutputStreamEx out, List<T> list) throws IOException {
				out.writeVarInt(list.size());
				for (int i = 0; i < list.size(); i++) {
					//noinspection unchecked
					DataStreamCodec<T> codec = (DataStreamCodec<T>) itemCodecFn.apply(i);
					codec.encode(out, list.get(i));
				}
			}

			@Override
			public List<T> decode(DataInputStreamEx in) throws IOException {
				Object[] array = new Object[in.readVarInt()];
				for (int i = 0; i < array.length; i++) {
					array[i] = itemCodecFn.apply(i).decode(in);
				}
				//noinspection unchecked
				return (List<T>) Arrays.asList(array);
			}
		};
	}

	public static <K, V> DataStreamCodec<Map<K, V>> ofMap(DataStreamCodec<K> keyCodec, DataStreamCodec<V> valueCodec) {
		return ofMap(keyCodec, $ -> valueCodec);
	}

	public static <K, V> DataStreamCodec<Map<K, V>> ofMap(DataStreamCodec<K> keyCodec,
			Function<? super K, ? extends DataStreamCodec<? extends V>> valueCodecFn) {
		return new DataStreamCodec<Map<K, V>>() {
			@Override
			public void encode(DataOutputStreamEx out, Map<K, V> map) throws IOException {
				out.writeVarInt(map.size());
				for (Map.Entry<K, V> entry : map.entrySet()) {
					keyCodec.encode(out, entry.getKey());
					//noinspection unchecked
					DataStreamCodec<V> codec = (DataStreamCodec<V>) valueCodecFn.apply(entry.getKey());
					codec.encode(out, entry.getValue());
				}
			}

			@Override
			public Map<K, V> decode(DataInputStreamEx in) throws IOException {
				int size = in.readVarInt();
				LinkedHashMap<K, V> map = new LinkedHashMap<>(size * 4 / 3);
				for (int i = 0; i < size; i++) {
					K key = keyCodec.decode(in);
					V value = valueCodecFn.apply(key).decode(in);
					map.put(key, value);
				}
				return map;
			}
		};

	}

	public static <T> DataStreamCodec<Set<T>> ofSet(DataStreamCodec<T> codec) {
		return transform(ofList(codec), LinkedHashSet::new, ArrayList::new);
	}

	public static <T> DataStreamCodec<@Nullable T> ofNullable(DataStreamCodec<@NotNull T> codec) {
		return new DataStreamCodec<T>() {
			@Override
			public void encode(DataOutputStreamEx stream, @Nullable T item) throws IOException {
				if (item == null) {
					stream.writeByte((byte) 0);
				} else {
					stream.writeByte((byte) 1);
					codec.encode(stream, item);
				}
			}

			@Override
			public @Nullable T decode(DataInputStreamEx stream) throws IOException {
				if (stream.readByte() == 0) return null;
				return codec.decode(stream);
			}
		};
	}

	public static <T, R> DataStreamCodec<R> transform(DataStreamCodec<T> codec, Function<T, ? extends R> reader, Function<R, T> writer) {
		return new DataStreamCodec<R>() {
			@Override
			public void encode(DataOutputStreamEx out, R value) throws IOException {
				T result = writer.apply(value);
				codec.encode(out, result);
			}

			@Override
			public R decode(DataInputStreamEx in) throws IOException {
				T result = codec.decode(in);
				return reader.apply(result);
			}
		};
	}

}
