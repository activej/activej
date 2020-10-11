package io.activej.serializer.datastream;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.IntFunction;

import static io.activej.codegen.expression.Expressions.*;
import static java.util.Arrays.asList;

@SuppressWarnings("ForLoopReplaceableByForEach")
public final class DataStreamCodecs {
	private static final ConcurrentHashMap<Class<?>, DataStreamCodec<?>> CODECS = new ConcurrentHashMap<>();

	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	public static DataStreamCodec<Void> ofVoid() {
		return new DataStreamCodec<Void>() {
			@Override
			public void encode(DataOutputStreamEx stream, Void item) throws IOException {
			}

			@Override
			public Void decode(DataInputStreamEx stream) throws IOException {
				return null;
			}
		};
	}

	public static DataStreamCodec<Boolean> ofBoolean() {
		return buildCodec(boolean.class, "writeBoolean", "readBoolean");
	}

	public static DataStreamCodec<Character> ofChar() {
		return buildCodec(char.class, "writeChar", "readChar");
	}

	public static DataStreamCodec<Byte> ofByte() {
		return buildCodec(byte.class, "writeByte", "readByte");
	}

	public static DataStreamCodec<Short> ofShort() {
		return buildCodec(short.class, "writeShort", "readShort");
	}

	public static DataStreamCodec<Integer> ofInt() {
		return buildCodec(int.class, "writeInt", "readInt");
	}

	public static DataStreamCodec<Integer> ofVarInt() {
		return buildCodec(int.class, "writeVarInt", "readVarInt");
	}

	public static DataStreamCodec<Long> ofLong() {
		return buildCodec(long.class, "writeLong", "readLong");
	}

	public static DataStreamCodec<Long> ofVarLong() {
		return buildCodec(long.class, "writeVarLong", "readVarLong");
	}

	public static DataStreamCodec<Float> ofFloat() {
		return buildCodec(float.class, "writeFloat", "readFloat");
	}

	public static DataStreamCodec<Double> ofDouble() {
		return buildCodec(double.class, "writeDouble", "readDouble");
	}

	public static DataStreamCodec<String> ofString() {
		return buildCodec(String.class, "writeString", "readString");
	}

	private static <T> DataStreamCodec<T> buildCodec(Class<T> itemType, String encode, String decode) {
		//noinspection unchecked
		return (DataStreamCodec<T>) CODECS.computeIfAbsent(itemType, $ ->
				(DataStreamCodec<T>) ClassBuilder.create(CLASS_LOADER, DataStreamCodec.class)
//						.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath())
						.withMethod("encode", call(arg(0), encode, cast(arg(1), itemType)))
						.withMethod("decode", call(arg(0), decode))
						.buildClassAndCreateNewInstance());
	}

	public static DataStreamCodec<boolean[]> ofBooleanArray() {
		return buildArrayCodec(boolean[].class, 1, "writeBoolean", "readBoolean");
	}

	public static DataStreamCodec<char[]> ofCharArray() {
		return buildArrayCodec(char[].class, 2, "writeChar", "readChar");
	}

	public static DataStreamCodec<byte[]> ofByteArray() {
		return new DataStreamCodec<byte[]>() {
			@Override
			public void encode(DataOutputStreamEx stream, byte[] item) throws IOException {
				stream.writeVarLong(item.length);
				stream.write(item);
			}

			@Override
			public byte[] decode(DataInputStreamEx stream) throws IOException {
				byte[] array = new byte[stream.readVarInt()];
				stream.read(array);
				return array;
			}
		};
	}

	public static DataStreamCodec<short[]> ofShortArray() {
		return buildArrayCodec(short[].class, 2, "writeShort", "readShort");
	}

	public static DataStreamCodec<int[]> ofIntArray() {
		return buildArrayCodec(int[].class, 4, "writeInt", "readInt");
	}

	public static DataStreamCodec<long[]> ofLongArray() {
		return buildArrayCodec(long[].class, 8, "writeLong", "readLong");
	}

	public static DataStreamCodec<float[]> ofFloatArray() {
		return buildArrayCodec(float[].class, 4, "writeFloat", "readFloat");
	}

	public static DataStreamCodec<double[]> ofDoubleArray() {
		return buildArrayCodec(double[].class, 8, "writeDouble", "readDouble");
	}

	private static <T> DataStreamCodec<T> buildArrayCodec(Class<T> arrayType, int elementSize, String encode, String decode) {
		Variable stream = arg(0);
		//noinspection unchecked
		return (DataStreamCodec<T>) CODECS.computeIfAbsent(arrayType, $ ->
				(DataStreamCodec<T>) ClassBuilder.create(CLASS_LOADER, Object.class, DataStreamCodec.class)
//						.withBytecodeSaveDir(Paths.get("tmp").toAbsolutePath())
						.withMethod("encode", let(cast(arg(1), arrayType),
								array -> sequence(
										call(stream, "writeVarInt", length(array)),
										call(stream, "ensure", mul(value(elementSize), length(array))),
										let(call(stream, "getBinaryOutput"),
												out -> loop(value(0), length(array),
														i -> call(out, encode, arrayGet(array, i)))))))
						.withMethod("decode",
								let(call(stream, "readVarInt"),
										length -> sequence(
												call(stream, "ensure", mul(value(elementSize), length)),
												let(arrayNew(arrayType, length),
														array -> let(call(stream, "getBinaryInput"),
																in -> sequence(
																		loop(value(0), length(array),
																				i -> arraySet(array, i, call(in, decode))),
																		array))))))
						.buildClassAndCreateNewInstance());
	}

	public static DataStreamCodec<int[]> ofVarIntArray() {
		return new DataStreamCodec<int[]>() {
			@Override
			public void encode(DataOutputStreamEx stream, int[] array) throws IOException {
				stream.writeVarInt(array.length);
				for (int i = 0; i < array.length; i++) {
					stream.writeVarInt(array[i]);
				}
			}

			@Override
			public int[] decode(DataInputStreamEx stream) throws IOException {
				int[] array = new int[stream.readVarInt()];
				for (int i = 0; i < array.length; i++) {
					array[i] = stream.readVarInt();
				}
				return array;
			}
		};
	}

	public static DataStreamCodec<int[]> ofVarLongArray() {
		return new DataStreamCodec<int[]>() {
			@Override
			public void encode(DataOutputStreamEx stream, int[] array) throws IOException {
				stream.writeVarInt(array.length);
				for (int i = 0; i < array.length; i++) {
					stream.writeVarInt(array[i]);
				}
			}

			@Override
			public int[] decode(DataInputStreamEx stream) throws IOException {
				int[] array = new int[stream.readVarInt()];
				for (int i = 0; i < array.length; i++) {
					array[i] = stream.readVarInt();
				}
				return array;
			}
		};
	}

	public static <T> DataStreamCodec<T[]> ofArray(DataStreamCodec<? extends T> itemCodec) {
		return ofArray($ -> itemCodec);
	}

	public static <T> DataStreamCodec<T[]> ofArray(IntFunction<? extends DataStreamCodec<? extends T>> itemCodecFn) {
		return new DataStreamCodec<T[]>() {
			@Override
			public void encode(DataOutputStreamEx out, T[] list) throws IOException {
				out.writeVarInt(list.length);
				for (int i = 0; i < list.length; i++) {
					//noinspection unchecked
					DataStreamCodec<T> codec = (DataStreamCodec<T>) itemCodecFn.apply(i);
					codec.encode(out, list[i]);
				}
			}

			@Override
			public T[] decode(DataInputStreamEx in) throws IOException {
				Object[] array = new Object[in.readVarInt()];
				for (int i = 0; i < array.length; i++) {
					array[i] = itemCodecFn.apply(i).decode(in);
				}
				//noinspection unchecked
				return (T[]) array;
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
				return (List<T>) asList(array);
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

	private static final class SubclassEntry<T> {
		final int idx;
		final DataStreamCodec<? extends T> codec;

		private SubclassEntry(int idx, DataStreamCodec<? extends T> codec) {
			this.idx = idx;
			this.codec = codec;
		}
	}

	public static <T> DataStreamCodec<? extends T> ofSubtype(LinkedHashMap<Class<? extends T>, DataStreamCodec<? extends T>> codecs) {
		HashMap<Class<? extends T>, SubclassEntry<T>> encoders = new HashMap<>();
		//noinspection unchecked
		DataStreamDecoder<? extends T>[] decoders = new DataStreamDecoder[codecs.size()];
		for (Class<? extends T> aClass : codecs.keySet()) {
			DataStreamCodec<? extends T> codec = codecs.get(aClass);
			int idx = encoders.size();
			encoders.put(aClass, new SubclassEntry<>(idx, codec));
			decoders[idx] = codec;
		}
		return new DataStreamCodec<T>() {
			@Override
			public void encode(DataOutputStreamEx stream, T item) throws IOException {
				Class<?> type = item.getClass();
				SubclassEntry<T> entry = encoders.get(type);
				if (entry == null) throw new IllegalArgumentException();
				stream.writeByte((byte) entry.idx);
				//noinspection unchecked
				((DataStreamCodec<T>) entry.codec).encode(stream, item);
			}

			@Override
			public T decode(DataInputStreamEx stream) throws IOException {
				int idx = stream.readByte();
				if (idx < 0 || idx >= decoders.length) throw new CorruptedDataException();
				return decoders[idx].decode(stream);
			}
		};
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
