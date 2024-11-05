package io.activej.serializer.stream;

import io.activej.common.annotation.StaticFactories;
import io.activej.common.builder.AbstractBuilder;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkState;
import static io.activej.serializer.stream.SizedCollectors.*;
import static io.activej.serializer.stream.SizedCollectorsKV.*;
import static io.activej.serializer.util.ZeroArrayUtils.*;
import static java.util.Objects.requireNonNullElseGet;

@SuppressWarnings("unused")
@StaticFactories(StreamCodec.class)
public class StreamCodecs {
	public static StreamCodec<Void> ofVoid() {
		return new StreamCodec<>() {
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
		return new StreamCodec<>() {
			@Override
			public Boolean decode(StreamInput input) throws IOException {
				return input.readBoolean();
			}

			@Override
			public void encode(StreamOutput output, Boolean item) throws IOException {
				output.writeBoolean(item);
			}
		};
	}

	public static StreamCodec<Character> ofChar() {
		return new StreamCodec<>() {
			@Override
			public Character decode(StreamInput input) throws IOException {
				return input.readChar();
			}

			@Override
			public void encode(StreamOutput output, Character item) throws IOException {
				output.writeChar(item);
			}
		};
	}

	public static StreamCodec<Byte> ofByte() {
		return new StreamCodec<>() {
			@Override
			public Byte decode(StreamInput input) throws IOException {
				return input.readByte();
			}

			@Override
			public void encode(StreamOutput output, Byte item) throws IOException {
				output.writeByte(item);
			}
		};
	}

	public static StreamCodec<Short> ofShort() {
		return new StreamCodec<>() {
			@Override
			public Short decode(StreamInput input) throws IOException {
				return input.readShort();
			}

			@Override
			public void encode(StreamOutput output, Short item) throws IOException {
				output.writeShort(item);
			}
		};
	}

	public static StreamCodec<Integer> ofInt() {
		return new StreamCodec<>() {
			@Override
			public Integer decode(StreamInput input) throws IOException {
				return input.readInt();
			}

			@Override
			public void encode(StreamOutput output, Integer item) throws IOException {
				output.writeInt(item);
			}
		};
	}

	public static StreamCodec<Integer> ofVarInt() {
		return new StreamCodec<>() {
			@Override
			public Integer decode(StreamInput input) throws IOException {
				return input.readVarInt();
			}

			@Override
			public void encode(StreamOutput output, Integer item) throws IOException {
				output.writeVarInt(item);
			}
		};
	}

	public static StreamCodec<Long> ofLong() {
		return new StreamCodec<>() {
			@Override
			public Long decode(StreamInput input) throws IOException {
				return input.readLong();
			}

			@Override
			public void encode(StreamOutput output, Long item) throws IOException {
				output.writeLong(item);
			}
		};
	}

	public static StreamCodec<Long> ofVarLong() {
		return new StreamCodec<>() {
			@Override
			public Long decode(StreamInput input) throws IOException {
				return input.readVarLong();
			}

			@Override
			public void encode(StreamOutput output, Long item) throws IOException {
				output.writeVarLong(item);
			}
		};
	}

	public static StreamCodec<Float> ofFloat() {
		return new StreamCodec<>() {
			@Override
			public Float decode(StreamInput input) throws IOException {
				return input.readFloat();
			}

			@Override
			public void encode(StreamOutput output, Float item) throws IOException {
				output.writeFloat(item);
			}
		};
	}

	public static StreamCodec<Double> ofDouble() {
		return new StreamCodec<>() {
			@Override
			public Double decode(StreamInput input) throws IOException {
				return input.readDouble();
			}

			@Override
			public void encode(StreamOutput output, Double item) throws IOException {
				output.writeDouble(item);
			}
		};
	}

	public static StreamCodec<String> ofString() {
		return new StreamCodec<>() {
			@Override
			public String decode(StreamInput input) throws IOException {
				return input.readString();
			}

			@Override
			public void encode(StreamOutput output, String item) throws IOException {
				output.writeString(item);
			}
		};
	}

	public static StreamCodec<boolean[]> ofBooleanArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_BOOLEANS, 1) {
			@Override
			protected int getArrayLength(boolean[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, boolean[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeBoolean(array[offset]);
				}
			}

			@Override
			protected boolean[] createArray(int length) {
				return new boolean[length];
			}

			@Override
			protected void doRead(BinaryInput input, boolean[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readBoolean();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, boolean[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readBoolean();
				}
			}
		};
	}

	public static StreamCodec<char[]> ofCharArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_CHARS, 2) {
			@Override
			protected int getArrayLength(char[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, char[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeChar(array[offset]);
				}
			}

			@Override
			protected char[] createArray(int length) {
				return new char[length];
			}

			@Override
			protected void doRead(BinaryInput input, char[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readChar();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, char[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readChar();
				}
			}
		};
	}

	public static StreamCodec<byte[]> ofByteArray() {
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, byte[] item) throws IOException {
				output.writeVarInt(item.length);
				output.write(item);
			}

			@Override
			public byte[] decode(StreamInput input) throws IOException {
				int size = input.readVarInt();
				if (size == 0) return ZERO_ARRAY_BYTES;
				byte[] array = new byte[size];
				input.read(array);
				return array;
			}
		};
	}

	public static StreamCodec<short[]> ofShortArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_SHORTS, 2) {
			@Override
			protected int getArrayLength(short[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, short[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeShort(array[offset]);
				}
			}

			@Override
			protected short[] createArray(int length) {
				return new short[length];
			}

			@Override
			protected void doRead(BinaryInput input, short[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readShort();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, short[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readShort();
				}
			}
		};
	}

	public static StreamCodec<int[]> ofIntArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_INTS, 4) {
			@Override
			protected int getArrayLength(int[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, int[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeInt(array[offset]);
				}
			}

			@Override
			protected int[] createArray(int length) {
				return new int[length];
			}

			@Override
			protected void doRead(BinaryInput input, int[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readInt();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, int[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readInt();
				}
			}
		};
	}

	public static StreamCodec<long[]> ofLongArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_LONGS, 8) {
			@Override
			protected int getArrayLength(long[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, long[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeLong(array[offset]);
				}
			}

			@Override
			protected long[] createArray(int length) {
				return new long[length];
			}

			@Override
			protected void doRead(BinaryInput input, long[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readLong();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, long[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readLong();
				}
			}
		};
	}

	public static StreamCodec<float[]> ofFloatArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_FLOATS, 4) {
			@Override
			protected int getArrayLength(float[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, float[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeFloat(array[offset]);
				}
			}

			@Override
			protected float[] createArray(int length) {
				return new float[length];
			}

			@Override
			protected void doRead(BinaryInput input, float[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readFloat();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, float[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readFloat();
				}
			}
		};
	}

	public static StreamCodec<double[]> ofDoubleArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_DOUBLES, 8) {
			@Override
			protected int getArrayLength(double[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, double[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeDouble(array[offset]);
				}
			}

			@Override
			protected double[] createArray(int length) {
				return new double[length];
			}

			@Override
			protected void doRead(BinaryInput input, double[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readDouble();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, double[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readDouble();
				}
			}
		};
	}

	public static StreamCodec<int[]> ofVarIntArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_INTS, 1, 5) {
			@Override
			protected int getArrayLength(int[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, int[] array, int offset, int limit) {
				for (int i = offset; i < limit; i++) {
					output.writeVarInt(array[i]);
				}
			}

			@Override
			protected int[] createArray(int length) {
				return new int[length];
			}

			@Override
			protected void doRead(BinaryInput input, int[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[offset++] = input.readVarInt();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, int[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readVarInt();
				}
			}
		};
	}

	public static StreamCodec<long[]> ofVarLongArray() {
		return new AbstractArbitraryLengthArrayStreamCodec<>(ZERO_ARRAY_LONGS, 1, 10) {
			@Override
			protected int getArrayLength(long[] array) {
				return array.length;
			}

			@Override
			protected void doWrite(BinaryOutput output, long[] array, int offset, int limit) {
				for (int i = offset; i < limit; i++) {
					output.writeVarLong(array[i]);
				}
			}

			@Override
			protected long[] createArray(int length) {
				return new long[length];
			}

			@Override
			protected void doRead(BinaryInput input, long[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[offset++] = input.readVarLong();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, long[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readVarLong();
				}
			}
		};
	}

	public static StreamCodec<boolean[]> ofFixedLengthBooleanArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_BOOLEANS);
		return new AbstractFixedLengthArrayStreamCodec<>(1, length) {
			@Override
			protected void doWrite(BinaryOutput output, boolean[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeBoolean(array[offset]);
				}
			}

			@Override
			protected boolean[] createArray(int length) {
				return new boolean[length];
			}

			@Override
			protected void doRead(BinaryInput input, boolean[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readBoolean();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, boolean[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readBoolean();
				}
			}
		};
	}

	public static StreamCodec<char[]> ofFixedLengthCharArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_CHARS);
		return new AbstractFixedLengthArrayStreamCodec<>(2, length) {
			@Override
			protected void doWrite(BinaryOutput output, char[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeChar(array[offset]);
				}
			}

			@Override
			protected char[] createArray(int length) {
				return new char[length];
			}

			@Override
			protected void doRead(BinaryInput input, char[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readChar();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, char[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readChar();
				}
			}
		};
	}

	public static StreamCodec<byte[]> ofFixedLengthByteArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_BYTES);
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, byte[] array) throws IOException {
				output.write(array);
			}

			@Override
			public byte[] decode(StreamInput input) throws IOException {
				byte[] array = new byte[length];
				input.read(array);
				return array;
			}
		};
	}

	public static StreamCodec<short[]> ofFixedLengthShortArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_SHORTS);
		return new AbstractFixedLengthArrayStreamCodec<>(2, length) {
			@Override
			protected void doWrite(BinaryOutput output, short[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeShort(array[offset]);
				}
			}

			@Override
			protected short[] createArray(int length) {
				return new short[length];
			}

			@Override
			protected void doRead(BinaryInput input, short[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readShort();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, short[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readShort();
				}
			}
		};
	}

	public static StreamCodec<int[]> ofFixedLengthIntArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_INTS);
		return new AbstractFixedLengthArrayStreamCodec<>(4, length) {
			@Override
			protected void doWrite(BinaryOutput output, int[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeInt(array[offset]);
				}
			}

			@Override
			protected int[] createArray(int length) {
				return new int[length];
			}

			@Override
			protected void doRead(BinaryInput input, int[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readInt();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, int[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readInt();
				}
			}
		};
	}

	public static StreamCodec<long[]> ofFixedLengthLongArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_LONGS);
		return new AbstractFixedLengthArrayStreamCodec<>(8, length) {
			@Override
			protected void doWrite(BinaryOutput output, long[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeLong(array[offset]);
				}
			}

			@Override
			protected long[] createArray(int length) {
				return new long[length];
			}

			@Override
			protected void doRead(BinaryInput input, long[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readLong();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, long[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readLong();
				}
			}
		};
	}

	public static StreamCodec<float[]> ofFixedLengthFloatArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_FLOATS);
		return new AbstractFixedLengthArrayStreamCodec<>(4, length) {
			@Override
			protected void doWrite(BinaryOutput output, float[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeFloat(array[offset]);
				}
			}

			@Override
			protected float[] createArray(int length) {
				return new float[length];
			}

			@Override
			protected void doRead(BinaryInput input, float[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readFloat();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, float[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readFloat();
				}
			}
		};
	}

	public static StreamCodec<double[]> ofFixedLengthDoubleArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_DOUBLES);
		return new AbstractFixedLengthArrayStreamCodec<>(8, length) {
			@Override
			protected void doWrite(BinaryOutput output, double[] array, int offset, int limit) {
				for (; offset < limit; offset++) {
					output.writeDouble(array[offset]);
				}
			}

			@Override
			protected double[] createArray(int length) {
				return new double[length];
			}

			@Override
			protected void doRead(BinaryInput input, double[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[i + offset] = input.readDouble();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, double[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readDouble();
				}
			}
		};
	}

	public static StreamCodec<int[]> ofFixedLengthVarIntArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_INTS);
		return new AbstractFixedLengthArrayStreamCodec<>(1, 5, length) {
			@Override
			protected void doWrite(BinaryOutput output, int[] array, int offset, int limit) {
				for (int i = offset; i < limit; i++) {
					output.writeVarInt(array[i]);
				}
			}

			@Override
			protected int[] createArray(int length) {
				return new int[length];
			}

			@Override
			protected void doRead(BinaryInput input, int[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[offset++] = input.readVarInt();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, int[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readVarInt();
				}
			}
		};
	}

	public static StreamCodec<long[]> ofFixedLengthVarLongArray(int length) {
		if (length == 0) return StreamCodec.of(($1, $2) -> {}, $ -> ZERO_ARRAY_LONGS);
		return new AbstractFixedLengthArrayStreamCodec<>(1, 10, length) {
			@Override
			protected void doWrite(BinaryOutput output, long[] array, int offset, int limit) {
				for (int i = offset; i < limit; i++) {
					output.writeVarLong(array[i]);
				}
			}

			@Override
			protected long[] createArray(int length) {
				return new long[length];
			}

			@Override
			protected void doRead(BinaryInput input, long[] array, int offset, int count) {
				for (int i = 0; i < count; i++) {
					array[offset++] = input.readVarLong();
				}
			}

			@Override
			protected void doReadRemaining(StreamInput input, long[] array, int offset, int limit) throws IOException {
				for (; offset < limit; offset++) {
					array[offset] = input.readVarLong();
				}
			}
		};
	}

	public static <T> StreamCodec<T[]> ofArray(StreamCodec<T> itemCodec, IntFunction<T[]> factory) {
		return ofArray($ -> itemCodec, factory);
	}

	public static <T> StreamCodec<T[]> ofArray(IntFunction<? extends StreamCodec<? extends T>> itemCodecFn, IntFunction<T[]> factory) {
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, T[] array) throws IOException {
				output.writeVarInt(array.length);
				for (int i = 0; i < array.length; i++) {
					//noinspection unchecked
					StreamCodec<T> codec = (StreamCodec<T>) itemCodecFn.apply(i);
					codec.encode(output, array[i]);
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

	public static <T> StreamCodec<T[]> ofFixedLengthArray(StreamCodec<T> itemCodec, Supplier<T[]> factory, int length) {
		return ofFixedLengthArray($ -> itemCodec, factory, length);
	}

	public static <T> StreamCodec<T[]> ofFixedLengthArray(IntFunction<? extends StreamCodec<? extends T>> itemCodecFn, Supplier<T[]> factory, int length) {
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, T[] array) throws IOException {
				for (int i = 0; i < length; i++) {
					//noinspection unchecked
					StreamCodec<T> codec = (StreamCodec<T>) itemCodecFn.apply(i);
					codec.encode(output, array[i]);
				}
			}

			@Override
			public T[] decode(StreamInput input) throws IOException {
				T[] array = factory.get();
				for (int i = 0; i < length; i++) {
					array[i] = itemCodecFn.apply(i).decode(input);
				}
				return array;
			}
		};
	}

	public static <E extends Enum<E>> StreamCodec<E> ofEnum(Class<E> enumType) {
		return new StreamCodec<>() {
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
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, Optional<T> item) throws IOException {
				if (item.isEmpty()) {
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

	public static <T, C extends Collection<T>> StreamCodec<C> ofCollection(StreamCodec<T> itemCodec, SizedCollector<T, ?, ? extends C> collector) {
		return ofCollection($ -> itemCodec, collector);
	}

	public static <T, C extends Collection<T>> StreamCodec<C> ofCollection(IntFunction<? extends StreamCodec<? extends T>> itemCodecFn, SizedCollector<T, ?, ? extends C> collector) {
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, C collection) throws IOException {
				output.writeVarInt(collection.size());
				int i = 0;
				for (T item : collection) {
					//noinspection unchecked
					StreamCodec<T> codec = (StreamCodec<T>) itemCodecFn.apply(i++);
					codec.encode(output, item);
				}
			}

			@Override
			public C decode(StreamInput input) throws IOException {
				int size = input.readVarInt();
				if (size == 0) {
					return collector.create0();
				} else if (size == 1) {
					return collector.create1(itemCodecFn.apply(0).decode(input));
				} else {
					Object acc = collector.accumulator(size);
					for (int i = 0; i < size; i++) {
						T item = itemCodecFn.apply(i).decode(input);
						//noinspection unchecked
						((SizedCollector<T, Object, C>) collector).accumulate(acc, i, item);
					}
					//noinspection unchecked
					return ((SizedCollector<T, Object, C>) collector).result(acc);
				}
			}
		};
	}

	public static <T> StreamCodec<Collection<T>> ofCollection(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, toCollection());
	}

	public static <E extends Enum<E>> StreamCodec<EnumSet<E>> ofEnumSet(Class<E> type) {
		return ofCollection(StreamCodecs.ofEnum(type), toEnumSet(type));
	}

	public static <T> StreamCodec<List<T>> ofList(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, toList());
	}

	public static <T> StreamCodec<List<T>> ofList(IntFunction<? extends StreamCodec<? extends T>> itemCodecFn) {
		return ofCollection(itemCodecFn, toList());
	}

	public static <T> StreamCodec<ArrayList<T>> ofArrayList(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, toArrayList());
	}

	public static <T> StreamCodec<LinkedList<T>> ofLinkedList(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, toLinkedList());
	}

	public static <T> StreamCodec<Set<T>> ofSet(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, toSet());
	}

	public static <T> StreamCodec<HashSet<T>> ofHashSet(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, toHashSet());
	}

	public static <T> StreamCodec<LinkedHashSet<T>> ofLinkedHashSet(StreamCodec<T> itemCodec) {
		return ofCollection(itemCodec, toLinkedHashSet());
	}

	public static <K, V> StreamCodec<Map<K, V>> ofMap(StreamCodec<K> keyCodec, StreamCodec<V> valueCodec) {
		return ofMap(keyCodec, valueCodec, toMap());
	}

	public static <K, V> StreamCodec<Map<K, V>> ofMap(StreamCodec<K> keyCodec, Function<? super K, ? extends StreamCodec<? extends V>> valueCodecFn) {
		return ofMap(keyCodec, valueCodecFn, toMap());
	}

	public static <K, V> StreamCodec<HashMap<K, V>> ofHashMap(StreamCodec<K> keyCodec, StreamCodec<V> valueCodec) {
		return ofMap(keyCodec, valueCodec, toHashMap());
	}

	public static <K, V> StreamCodec<LinkedHashMap<K, V>> ofLinkedHashMap(StreamCodec<K> keyCodec, StreamCodec<V> valueCodec) {
		return ofMap(keyCodec, valueCodec, toLinkedHashMap());
	}

	public static <K extends Enum<K>, V> StreamCodec<EnumMap<K, V>> ofEnumMap(Class<K> type, StreamCodec<V> valueCodec) {
		return ofMap(StreamCodecs.ofEnum(type), valueCodec, toEnumMap(type));
	}

	public static <K extends Enum<K>, V> StreamCodec<EnumMap<K, V>> ofEnumMap(Class<K> type, Function<K, StreamCodec<? extends V>> valueCodecFn) {
		return ofMap(StreamCodecs.ofEnum(type), valueCodecFn, toEnumMap(type));
	}

	public static <K, V, M extends Map<K, V>> StreamCodec<M> ofMap(StreamCodec<K> keyCodec, StreamCodec<? extends V> valueCodec, SizedCollectorKV<K, V, ?, ? extends M> collector) {
		return ofMap(keyCodec, $ -> valueCodec, collector);
	}

	public static <K, V, M extends Map<K, V>> StreamCodec<M> ofMap(
		StreamCodec<K> keyCodec, Function<? super K, ? extends StreamCodec<? extends V>> valueCodecFn,
		SizedCollectorKV<K, V, ?, ? extends M> collector
	) {
		return new StreamCodec<>() {
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
				if (length == 0) {
					return collector.create0();
				} else if (length == 1) {
					K key = keyCodec.decode(input);
					V value = valueCodecFn.apply(key).decode(input);
					return collector.create1(key, value);
				} else {
					Object acc = collector.accumulator(length);
					for (int i = 0; i < length; i++) {
						K key = keyCodec.decode(input);
						V value = valueCodecFn.apply(key).decode(input);
						//noinspection unchecked
						((SizedCollectorKV<K, V, Object, M>) collector).accumulate(acc, i, key, value);
					}
					//noinspection unchecked
					return ((SizedCollectorKV<K, V, Object, M>) collector).result(acc);
				}
			}
		};
	}

	public static class SubtypeStreamCodec<T> implements StreamCodec<T> {
		public record SubclassEntry<T>(byte idx, StreamCodec<T> codec) {}

		private final Map<Class<?>, SubclassEntry<? extends T>> encoders = new HashMap<>();
		private final List<StreamDecoder<? extends T>> decoders = new ArrayList<>();

		private SubtypeStreamCodec() {}

		public static <T> SubtypeStreamCodec<T>.Builder builder() {
			return new SubtypeStreamCodec<T>().new Builder();
		}

		public final class Builder extends AbstractBuilder<Builder, SubtypeStreamCodec<T>> {
			private Builder() {}

			public <E extends T> Builder withSubtype(Class<E> type, StreamCodec<E> codec) {
				checkNotBuilt(this);
				byte idx = (byte) encoders.size();
				encoders.put(type, new SubclassEntry<>(idx, codec));
				decoders.add(codec);
				return this;
			}

			@Override
			protected SubtypeStreamCodec<T> doBuild() {
				checkState(!encoders.isEmpty(), "No subtype codec has been specified");
				return SubtypeStreamCodec.this;
			}
		}

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
	}

	public static <T> StreamCodec<T> ofSubtype(LinkedHashMap<Class<? extends T>, StreamCodec<? extends T>> codecs) {
		SubtypeStreamCodec<T>.Builder builder = SubtypeStreamCodec.builder();
		for (Map.Entry<Class<? extends T>, StreamCodec<? extends T>> e : codecs.entrySet()) {
			//noinspection unchecked
			builder.withSubtype((Class<T>) e.getKey(), (StreamCodec<T>) e.getValue());
		}
		return builder.build();
	}

	public static <T> StreamCodec<@Nullable T> ofNullable(StreamCodec<T> codec) {
		return new StreamCodec<>() {
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
		return new StreamCodec<>() {
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
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, T item) {
			}

			@Override
			public T decode(StreamInput input) {
				return instance;
			}
		};
	}

	public static class OfBinarySerializer<T> implements StreamCodec<T> {
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
			writeSize(output.out().array(), positionBegin, dataSize, headerSize);
		}

		private void estimateMore(StreamOutput output, int positionBegin, int positionData, int dataSize, int headerSize) {
			if (dataSize >= MAX_SIZE) throw new IllegalArgumentException("Unsupported size");

			while (true) {
				int estimationOld = estimation.get();
				int estimationNew = Math.max(estimationOld, dataSize);
				if (estimation.compareAndSet(estimationOld, estimationNew)) {
					break;
				}
			}

			ensureHeaderSize(output, positionBegin, positionData, dataSize, headerSize);
		}

		private void ensureHeaderSize(
			StreamOutput output, int positionBegin, int positionData, int dataSize, int headerSize
		) {
			int previousHeaderSize = positionData - positionBegin;
			if (previousHeaderSize == headerSize) return; // offset is enough for header

			int headerDelta = headerSize - previousHeaderSize;
			assert headerDelta > 0;
			int newPositionData = positionData + headerDelta;
			int newPositionEnd = newPositionData + dataSize;
			byte[] array = output.out().array();
			if (newPositionEnd <= array.length) {
				System.arraycopy(array, positionData, array, newPositionData, dataSize);
				output.out().pos(newPositionEnd);
			} else {
				// rare case when data overflows array
				byte[] newArray = allocate(newPositionEnd);
				System.arraycopy(array, 0, newArray, 0, positionBegin);
				System.arraycopy(array, positionData, newArray, newPositionData, dataSize);
				output.out(new BinaryOutput(newArray, newPositionEnd));
				recycle(array);
			}
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
					throw new CorruptedDataException(String.format("Deserialized size (%d) != decoded data size (%d)", in.pos() - oldPos, messageSize));
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

	public static abstract class AbstractRawArrayStreamCodec<T> implements StreamCodec<T> {
		protected final int minElementSize;
		protected final int maxElementSize;

		protected AbstractRawArrayStreamCodec(int minElementSize, int maxElementSize) {
			this.minElementSize = minElementSize;
			this.maxElementSize = maxElementSize;
		}

		protected AbstractRawArrayStreamCodec(int elementSize) {
			this(elementSize, elementSize);
		}

		public final void encodeRaw(StreamOutput output, T array, int length) throws IOException {
			output.ensure(maxElementSize);

			for (int i = 0; i < length; ) {
				int numberOfItems = output.remaining() / maxElementSize;
				if (numberOfItems == 0) {
					output.flush();
					continue;
				}
				int limit = i + Math.min(numberOfItems, length - i);
				doWrite(output.out(), array, i, limit);
				i += numberOfItems;
			}
		}

		protected abstract void doWrite(BinaryOutput output, T array, int offset, int limit);

		public final T decodeRaw(StreamInput input, int length) throws IOException {
			T array = createArray(length);
			int idx = 0;
			while (idx < length) {
				int safeRemaining = length - idx;
				int safeReadCount = Math.min(safeRemaining, input.remaining() / maxElementSize);
				if (safeReadCount == 0) {
					int safeEnsure = Math.min(input.in().array().length - input.remaining(), safeRemaining * minElementSize);
					input.ensure(safeEnsure);
					if (input.remaining() < maxElementSize) {
						break;
					}
					continue;
				}
				doRead(input.in(), array, idx, safeReadCount);
				idx += safeReadCount;
			}
			if (idx < length) {
				doReadRemaining(input, array, idx, length);
			}
			return array;
		}

		protected abstract T createArray(int length);

		protected abstract void doRead(BinaryInput input, T array, int offset, int count);

		protected abstract void doReadRemaining(StreamInput input, T array, int offset, int limit) throws IOException;

	}

	public static abstract class AbstractFixedLengthArrayStreamCodec<T> extends AbstractRawArrayStreamCodec<T> {
		private final int length;

		protected AbstractFixedLengthArrayStreamCodec(int minElementSize, int maxElementSize, int length) {
			super(minElementSize, maxElementSize);
			this.length = length;
		}

		protected AbstractFixedLengthArrayStreamCodec(int elementSize, int length) {
			super(elementSize);
			this.length = length;
		}

		@Override
		public final T decode(StreamInput input) throws IOException {
			return decodeRaw(input, length);
		}

		@Override
		public final void encode(StreamOutput output, T item) throws IOException {
			encodeRaw(output, item, length);
		}
	}

	public static abstract class AbstractArbitraryLengthArrayStreamCodec<T> extends AbstractRawArrayStreamCodec<T> {
		private final T array0;

		protected AbstractArbitraryLengthArrayStreamCodec(T array0, int minElementSize, int maxElementSize) {
			super(minElementSize, maxElementSize);
			this.array0 = array0;
		}

		protected AbstractArbitraryLengthArrayStreamCodec(T array0, int elementSize) {
			super(elementSize);
			this.array0 = array0;
		}

		@Override
		public final T decode(StreamInput input) throws IOException {
			int length = input.readVarInt();
			if (length == 0) return array0;
			return decodeRaw(input, length);
		}

		@Override
		public final void encode(StreamOutput output, T array) throws IOException {
			int length = getArrayLength(array);
			output.writeVarInt(length);
			encodeRaw(output, array, length);
		}

		protected abstract int getArrayLength(T array);
	}

	public static <T> StreamCodec<T> reference(StreamCodec<T> codec) {
		return new StreamCodec<>() {
			private final IdentityHashMap<T, Integer> mapEncode = new IdentityHashMap<>();
			private final ArrayList<T> mapDecode = new ArrayList<>();

			@Override
			public void encode(StreamOutput output, T item) throws IOException {
				int index = mapEncode.getOrDefault(item, 0);
				if (index == 0) {
					mapEncode.put(item, mapEncode.size() + 1);
					output.writeVarInt(0);
					codec.encode(output, item);
				} else {
					output.writeVarInt(index);
				}
			}

			@Override
			public T decode(StreamInput input) throws IOException {
				int index = input.readVarInt();
				if (index == 0) {
					T item = codec.decode(input);
					mapDecode.add(item);
					return item;
				} else {
					return mapDecode.get(index - 1);
				}
			}
		};
	}

	public static <T> StreamCodec<T> lazy(Supplier<StreamCodec<T>> codecSupplier) {
		return new StreamCodec<>() {
			private StreamCodec<T> codec;

			@Override
			public T decode(StreamInput input) throws IOException {
				return ensureCodec().decode(input);
			}

			@Override
			public void encode(StreamOutput output, T item) throws IOException {
				ensureCodec().encode(output, item);
			}

			private StreamCodec<T> ensureCodec() {
				return codec = requireNonNullElseGet(codec, codecSupplier);
			}
		};
	}
}
