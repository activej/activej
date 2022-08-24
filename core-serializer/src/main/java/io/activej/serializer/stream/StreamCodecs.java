package io.activej.serializer.stream;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntFunction;

import static io.activej.codegen.expression.Expressions.*;
import static java.util.Arrays.asList;

@SuppressWarnings("ForLoopReplaceableByForEach")
public final class StreamCodecs {
	private static final ConcurrentHashMap<Class<?>, StreamCodec<?>> CODECS = new ConcurrentHashMap<>();

	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	public static StreamCodec<Void> ofVoid() {
		return new StreamCodec<Void>() {
			@Override
			public void encode(StreamOutput output, Void item) {
			}

			@Override
			public Void decode(StreamInput input) {
				return null;
			}
		};
	}

	public static StreamCodec<Boolean> ofBoolean() {
		return buildCodec(boolean.class, "writeBoolean", "readBoolean");
	}

	public static StreamCodec<Character> ofChar() {
		return buildCodec(char.class, "writeChar", "readChar");
	}

	public static StreamCodec<Byte> ofByte() {
		return buildCodec(byte.class, "writeByte", "readByte");
	}

	public static StreamCodec<Short> ofShort() {
		return buildCodec(short.class, "writeShort", "readShort");
	}

	public static StreamCodec<Integer> ofInt() {
		return buildCodec(int.class, "writeInt", "readInt");
	}

	public static StreamCodec<Integer> ofVarInt() {
		return buildCodec(int.class, "writeVarInt", "readVarInt");
	}

	public static StreamCodec<Long> ofLong() {
		return buildCodec(long.class, "writeLong", "readLong");
	}

	public static StreamCodec<Long> ofVarLong() {
		return buildCodec(long.class, "writeVarLong", "readVarLong");
	}

	public static StreamCodec<Float> ofFloat() {
		return buildCodec(float.class, "writeFloat", "readFloat");
	}

	public static StreamCodec<Double> ofDouble() {
		return buildCodec(double.class, "writeDouble", "readDouble");
	}

	public static StreamCodec<String> ofString() {
		return buildCodec(String.class, "writeString", "readString");
	}

	private static <T> StreamCodec<T> buildCodec(Class<T> itemType, String encode, String decode) {
		//noinspection unchecked
		return (StreamCodec<T>) CODECS.computeIfAbsent(itemType, $ ->
				(StreamCodec<T>) ClassBuilder.create(StreamCodec.class)
						.withMethod("encode", call(arg(0), encode, cast(arg(1), itemType)))
						.withMethod("decode", call(arg(0), decode))
						.defineClassAndCreateInstance(CLASS_LOADER));
	}

	public static StreamCodec<boolean[]> ofBooleanArray() {
		return buildArrayCodec(boolean[].class, 1, "writeBoolean", "readBoolean");
	}

	public static StreamCodec<char[]> ofCharArray() {
		return buildArrayCodec(char[].class, 2, "writeChar", "readChar");
	}

	public static StreamCodec<byte[]> ofByteArray() {
		return new StreamCodec<byte[]>() {
			@Override
			public void encode(StreamOutput output, byte[] item) throws IOException {
				output.writeVarLong(item.length);
				output.write(item);
			}

			@Override
			public byte[] decode(StreamInput input) throws IOException {
				byte[] array = new byte[input.readVarInt()];
				input.read(array);
				return array;
			}
		};
	}

	public static StreamCodec<short[]> ofShortArray() {
		return buildArrayCodec(short[].class, 2, "writeShort", "readShort");
	}

	public static StreamCodec<int[]> ofIntArray() {
		return buildArrayCodec(int[].class, 4, "writeInt", "readInt");
	}

	public static StreamCodec<long[]> ofLongArray() {
		return buildArrayCodec(long[].class, 8, "writeLong", "readLong");
	}

	public static StreamCodec<float[]> ofFloatArray() {
		return buildArrayCodec(float[].class, 4, "writeFloat", "readFloat");
	}

	public static StreamCodec<double[]> ofDoubleArray() {
		return buildArrayCodec(double[].class, 8, "writeDouble", "readDouble");
	}

	private static <T> StreamCodec<T> buildArrayCodec(Class<T> arrayType, int elementSize, String encode, String decode) {
		Variable stream = arg(0);
		//noinspection unchecked
		return (StreamCodec<T>) CODECS.computeIfAbsent(arrayType, $ ->
				(StreamCodec<T>) ClassBuilder.create(StreamCodec.class)
						.withMethod("encode", let(cast(arg(1), arrayType),
								array -> sequence(
										call(stream, "writeVarInt", length(array)),
										iterateArray(array, it -> call(stream, encode, it)))))
						.withMethod("decode",
								let(call(stream, "readVarInt"),
										length -> sequence(
												let(arrayNew(arrayType, length),
														array -> sequence(
																iterate(value(0), length,
																		i -> arraySet(array, i, call(stream, decode))),
																array)))))
						.defineClassAndCreateInstance(CLASS_LOADER));
	}

	public static StreamCodec<int[]> ofVarIntArray() {
		return new StreamCodec<int[]>() {
			@Override
			public void encode(StreamOutput output, int[] array) throws IOException {
				output.writeVarInt(array.length);
				for (int i = 0; i < array.length; i++) {
					output.writeVarInt(array[i]);
				}
			}

			@Override
			public int[] decode(StreamInput input) throws IOException {
				int[] array = new int[input.readVarInt()];
				for (int i = 0; i < array.length; i++) {
					array[i] = input.readVarInt();
				}
				return array;
			}
		};
	}

	public static StreamCodec<long[]> ofVarLongArray() {
		return new StreamCodec<long[]>() {
			@Override
			public void encode(StreamOutput output, long[] array) throws IOException {
				output.writeVarInt(array.length);
				for (int i = 0; i < array.length; i++) {
					output.writeVarLong(array[i]);
				}
			}

			@Override
			public long[] decode(StreamInput input) throws IOException {
				long[] array = new long[input.readVarInt()];
				for (int i = 0; i < array.length; i++) {
					array[i] = input.readVarLong();
				}
				return array;
			}
		};
	}

	public static <T> StreamCodec<T[]> ofArray(StreamCodec<T> itemCodec, IntFunction<T[]> factory) {
		return ofArray($ -> itemCodec, factory);
	}

	public static <T> StreamCodec<T[]> ofArray(IntFunction<? extends StreamCodec<? extends T>> itemCodecFn, IntFunction<T[]> factory) {
		return new StreamCodec<T[]>() {
			@Override
			public void encode(StreamOutput output, T[] list) throws IOException {
				output.writeVarInt(list.length);
				for (int i = 0; i < list.length; i++) {
					//noinspection unchecked
					StreamCodec<T> codec = (StreamCodec<T>) itemCodecFn.apply(i);
					codec.encode(output, list[i]);
				}
			}

			@Override
			public T[] decode(StreamInput input) throws IOException {
				int size = input.readVarInt();
				T[] array = factory.apply(size);
				for (int i = 0; i < size; i++) {
					array[i] = itemCodecFn.apply(i).decode(input);
				}
				return array;
			}
		};
	}

	public static <E extends Enum<E>> StreamCodec<E> ofEnum(Class<E> enumType) {
		return new StreamCodec<E>() {
			@Override
			public void encode(StreamOutput output, E value) throws IOException {
				output.writeVarInt(value.ordinal());
			}

			@Override
			public E decode(StreamInput input) throws IOException {
				return enumType.getEnumConstants()[input.readVarInt()];
			}
		};
	}

	public static <T> StreamCodec<Optional<T>> ofOptional(StreamCodec<T> codec) {
		return new StreamCodec<Optional<T>>() {
			@Override
			public void encode(StreamOutput output, Optional<T> item) throws IOException {
				if (!item.isPresent()) {
					output.writeByte((byte) 0);
				} else {
					output.writeByte((byte) 1);
					codec.encode(output, item.get());
				}
			}

			@Override
			public Optional<T> decode(StreamInput input) throws IOException {
				if (input.readByte() == 0) return Optional.empty();
				return Optional.of(codec.decode(input));
			}
		};
	}

	public static <T, C extends Collection<T>> StreamCodec<C> ofCollection(StreamCodec<T> itemCodec, IntFunction<C> factory) {
		return new StreamCodec<C>() {
			@Override
			public void encode(StreamOutput output, C c) throws IOException {
				output.writeVarInt(c.size());
				for (T e : c) {
					itemCodec.encode(output, e);
				}
			}

			@Override
			public C decode(StreamInput input) throws IOException {
				int size = input.readVarInt();
				C c = factory.apply(size);
				for (int i = 0; i < size; i++) {
					T e = itemCodec.decode(input);
					c.add(e);
				}
				return c;
			}
		};
	}

	public static <T> StreamCodec<Collection<T>> ofCollection(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, ArrayList::new);
	}

	public static <T> StreamCodec<Set<T>> ofSet(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, length -> new HashSet<>(hashInitialSize(length)));
	}

	public static <E extends Enum<E>> StreamCodec<Set<E>> ofEnumSet(Class<E> type) {
		return ofCollection(StreamCodecs.ofEnum(type), $ -> EnumSet.noneOf(type));
	}

	public static <T> StreamCodec<List<T>> ofList(StreamCodec<T> itemCodec) {
		return ofList($ -> itemCodec);
	}

	public static <T> StreamCodec<List<T>> ofList(IntFunction<? extends StreamCodec<? extends T>> itemCodecFn) {
		return new StreamCodec<List<T>>() {
			@Override
			public void encode(StreamOutput output, List<T> list) throws IOException {
				output.writeVarInt(list.size());
				for (int i = 0; i < list.size(); i++) {
					//noinspection unchecked
					StreamCodec<T> codec = (StreamCodec<T>) itemCodecFn.apply(i);
					codec.encode(output, list.get(i));
				}
			}

			@Override
			public List<T> decode(StreamInput input) throws IOException {
				int length = input.readVarInt();
				Object[] array = new Object[length];
				for (int i = 0; i < length; i++) {
					array[i] = itemCodecFn.apply(i).decode(input);
				}
				//noinspection unchecked
				return (List<T>) asList(array);
			}
		};
	}

	public static <K, V> StreamCodec<Map<K, V>> ofMap(StreamCodec<K> keyCodec, StreamCodec<V> valueCodec) {
		return ofMap(keyCodec, $ -> valueCodec);
	}

	public static <K, V> StreamCodec<Map<K, V>> ofMap(StreamCodec<K> keyCodec,
	                                                  Function<? super K, ? extends StreamCodec<? extends V>> valueCodecFn) {
		return ofMap(keyCodec, valueCodecFn, length -> new HashMap<>(hashInitialSize(length)));
	}

	public static <E extends Enum<E>, V> StreamCodec<Map<E, V>> ofEnumMap(Class<E> type, StreamCodec<V> valueCodec) {
		return ofMap(StreamCodecs.ofEnum(type), $ -> valueCodec, $ -> new EnumMap<>(type));
	}

	public static <K, V, M extends Map<K, V>> StreamCodec<M> ofMap(StreamCodec<K> keyCodec, Function<? super K, ? extends StreamCodec<? extends V>> valueCodecFn, IntFunction<M> factory) {
		return new StreamCodec<M>() {
			@Override
			public void encode(StreamOutput output, M map) throws IOException {
				output.writeVarInt(map.size());
				for (Map.Entry<K, V> entry : map.entrySet()) {
					keyCodec.encode(output, entry.getKey());
					//noinspection unchecked
					StreamCodec<V> valueCodec = (StreamCodec<V>) valueCodecFn.apply(entry.getKey());
					valueCodec.encode(output, entry.getValue());
				}
			}

			@Override
			public M decode(StreamInput input) throws IOException {
				int length = input.readVarInt();
				M map = factory.apply(length);
				for (int i = 0; i < length; i++) {
					K key = keyCodec.decode(input);
					V value = valueCodecFn.apply(key).decode(input);
					map.put(key, value);
				}
				return map;
			}
		};
	}

	public static class SubtypeBuilder<T> {
		private static final class SubclassEntry<T> {
			final byte idx;
			final StreamCodec<T> codec;

			private SubclassEntry(byte idx, StreamCodec<T> codec) {
				this.idx = idx;
				this.codec = codec;
			}
		}

		private final Map<Class<?>, SubclassEntry<? extends T>> encoders = new HashMap<>();
		private final List<StreamDecoder<? extends T>> decoders = new ArrayList<>();

		public <E extends T> SubtypeBuilder<T> add(Class<E> type, StreamCodec<E> codec) {
			byte idx = (byte) encoders.size();
			encoders.put(type, new SubclassEntry<>(idx, codec));
			decoders.add(codec);
			return this;
		}

		public StreamCodec<T> build() {
			if (encoders.isEmpty()) throw new IllegalStateException("No subtype codec has been specified");

			return new StreamCodec<T>() {
				@Override
				public void encode(StreamOutput output, T item) throws IOException {
					Class<?> type = item.getClass();
					//noinspection unchecked
					SubclassEntry<T> entry = (SubclassEntry<T>) encoders.get(type);
					if (entry == null) throw new IllegalArgumentException("Unsupported type " + type);
					output.writeByte(entry.idx);
					entry.codec.encode(output, item);
				}

				@Override
				public T decode(StreamInput input) throws IOException {
					int idx = input.readByte();
					if (idx < 0 || idx >= decoders.size()) throw new CorruptedDataException();
					return decoders.get(idx).decode(input);
				}
			};
		}
	}

	public static <T> StreamCodec<T> ofSubtype(LinkedHashMap<Class<? extends T>, StreamCodec<? extends T>> codecs) {
		SubtypeBuilder<T> builder = new SubtypeBuilder<>();
		for (Map.Entry<Class<? extends T>, StreamCodec<? extends T>> e : codecs.entrySet()) {
			//noinspection unchecked
			builder.add((Class<T>) e.getKey(), (StreamCodec<T>) e.getValue());
		}
		return builder.build();
	}

	public static <T> StreamCodec<@Nullable T> ofNullable(StreamCodec<@NotNull T> codec) {
		return new StreamCodec<T>() {
			@Override
			public void encode(StreamOutput output, @Nullable T item) throws IOException {
				if (item == null) {
					output.writeByte((byte) 0);
				} else {
					output.writeByte((byte) 1);
					codec.encode(output, item);
				}
			}

			@Override
			public @Nullable T decode(StreamInput input) throws IOException {
				if (input.readByte() == 0) return null;
				return codec.decode(input);
			}
		};
	}

	public static <T, R> StreamCodec<R> transform(StreamCodec<T> codec, Function<T, ? extends R> reader, Function<R, T> writer) {
		return new StreamCodec<R>() {
			@Override
			public void encode(StreamOutput output, R value) throws IOException {
				T result = writer.apply(value);
				codec.encode(output, result);
			}

			@Override
			public R decode(StreamInput input) throws IOException {
				T result = codec.decode(input);
				return reader.apply(result);
			}
		};
	}

	public static <T> StreamCodec<T> singleton(T instance) {
		return new StreamCodec<T>() {
			@Override
			public void encode(StreamOutput output, T item) {
			}

			@Override
			public T decode(StreamInput input) {
				return instance;
			}
		};
	}

	private static int hashInitialSize(int length) {
		return (length + 2) / 3 * 4;
	}

	static class OfBinarySerializer<T> implements StreamCodec<T> {
		private static final int MAX_SIZE = 1 << 28; // 256MB
		public static final int DEFAULT_ESTIMATED_SIZE = 1;

		private final BinarySerializer<T> serializer;

		private final AtomicInteger estimation = new AtomicInteger();

		public OfBinarySerializer(BinarySerializer<T> serializer) {
			this(serializer, DEFAULT_ESTIMATED_SIZE);
		}

		public OfBinarySerializer(BinarySerializer<T> serializer, int estimatedSize) {
			this.serializer = serializer;
			this.estimation.set(estimatedSize);
		}

		@Override
		public final void encode(StreamOutput output, T item) throws IOException {
			int positionBegin;
			int positionData;
			final int estimatedDataSize = estimation.get();
			final int estimatedHeaderSize = varIntSize(estimatedDataSize);
			output.ensure(estimatedHeaderSize + estimatedDataSize + (estimatedDataSize >>> 2));
			BinaryOutput out;
			for (; ; ) {
				out = output.out();
				positionBegin = out.pos();
				positionData = positionBegin + estimatedHeaderSize;
				out.pos(positionData);
				try {
					serializer.encode(out, item);
				} catch (ArrayIndexOutOfBoundsException e) {
					int dataSize = out.array().length - positionData;
					out.pos(positionBegin);
					output.ensure(estimatedHeaderSize + dataSize + 1 + (dataSize >>> 1));
					continue;
				}
				break;
			}

			int positionEnd = out.pos();
			int dataSize = positionEnd - positionData;
			int headerSize;
			if (dataSize > estimatedDataSize) {
				headerSize = varIntSize(dataSize);
				estimateMore(output, positionBegin, positionData, dataSize, headerSize);
			} else {
				headerSize = estimatedHeaderSize;
			}
			writeSize(output.array(), positionBegin, dataSize, headerSize);
		}

		private void estimateMore(StreamOutput output, int positionBegin, int positionData, int dataSize, int headerSize) {
			if (!(dataSize < MAX_SIZE)) throw new IllegalArgumentException("Unsupported size");

			while (true) {
				int estimationOld = estimation.get();
				int estimationNew = Math.max(estimationOld, dataSize);
				if (estimation.compareAndSet(estimationOld, estimationNew)) {
					break;
				}
			}

			ensureHeaderSize(output, positionBegin, positionData, dataSize, headerSize);
		}

		private void ensureHeaderSize(StreamOutput output, int positionBegin, int positionData,
		                              int dataSize, int headerSize) {
			int previousHeaderSize = positionData - positionBegin;
			if (previousHeaderSize == headerSize) return; // offset is enough for header

			int headerDelta = headerSize - previousHeaderSize;
			assert headerDelta > 0;
			int newPositionData = positionData + headerDelta;
			int newPositionEnd = newPositionData + dataSize;
			byte[] array = output.array();
			if (newPositionEnd < array.length) {
				System.arraycopy(array, positionData, array, newPositionData, dataSize);
			} else {
				// rare case when data overflows array
				byte[] oldArray = array;

				// ensured size without flush
				output.out(new BinaryOutput(allocate(newPositionEnd)));

				array = output.array();
				System.arraycopy(oldArray, 0, array, 0, positionBegin);
				System.arraycopy(oldArray, positionData, array, newPositionData, dataSize);
				recycle(oldArray);
			}
			output.pos(newPositionEnd);
		}

		private static int varIntSize(int dataSize) {
			return 1 + (31 - Integer.numberOfLeadingZeros(dataSize)) / 7;
		}

		private void writeSize(byte[] array, int pos, int size, int headerSize) {
			if (headerSize == 1) {
				array[pos] = (byte) size;
				return;
			}

			array[pos] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			if (headerSize == 2) {
				array[pos + 1] = (byte) size;
				return;
			}

			array[pos + 1] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			if (headerSize == 3) {
				array[pos + 2] = (byte) size;
				return;
			}

			assert headerSize == 4;
			array[pos + 2] = (byte) ((size & 0x7F) | 0x80);
			size >>>= 7;
			array[pos + 3] = (byte) size;
		}

		protected byte[] allocate(int size) {
			return new byte[size];
		}

		protected void recycle(byte[] array) {
		}

		@Override
		public T decode(StreamInput input) throws IOException {
			int messageSize = readSize(input);

			input.ensure(messageSize);

			BinaryInput in = input.in();
			int oldPos = in.pos();
			try {
				T item = serializer.decode(in);
				if (in.pos() - oldPos != messageSize) {
					throw new CorruptedDataException("Deserialized size != decoded data size");
				}
				return item;
			} catch (CorruptedDataException e) {
				input.close();
				throw e;
			}
		}

		private int readSize(StreamInput input) throws IOException {
			int result;
			byte b = input.readByte();
			if (b >= 0) {
				result = b;
			} else {
				result = b & 0x7f;
				if ((b = input.readByte()) >= 0) {
					result |= b << 7;
				} else {
					result |= (b & 0x7f) << 7;
					if ((b = input.readByte()) >= 0) {
						result |= b << 14;
					} else {
						result |= (b & 0x7f) << 14;
						if ((b = input.readByte()) >= 0) {
							result |= b << 21;
						} else {
							input.close();
							throw new CorruptedDataException("Invalid size");
						}
					}
				}
			}
			return result;
		}

	}
}
