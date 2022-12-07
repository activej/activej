package io.activej.streamcodecs;

import io.activej.streamcodecs.StreamCodecs.SubtypeBuilder;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

@RunWith(Theories.class)
public class StreamCodecsTest {

	@DataPoints("bufferSizes")
	public static int[] BUFFER_SIZES = new int[]{1, 2, 3, 5, 10, 100, StreamOutput.DEFAULT_BUFFER_SIZE, 2 * StreamOutput.DEFAULT_BUFFER_SIZE};

	@DataPoints("containerSizes")
	public static int[] CONTAINER_SIZES = new int[]{1, 2, 3, 5, 10, 1_000, 1_000_000};

	@Theory
	public void ofString(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize) {
		StreamCodec<String> codec = StreamCodecs.ofString();
		String string = "test";
		String result = doTest(codec, string, readBufferSize, writeBufferSize);

		assertEquals(string, result);
	}

	@Theory
	public void ofArray(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<String[]> codec = StreamCodecs.ofArray(StreamCodecs.ofString(), String[]::new);
		String[] strings = new String[arraySize];
		for (int i = 0; i < arraySize; i++) {
			strings[i] = String.valueOf(i);
		}
		String[] result = doTest(codec, strings, readBufferSize, writeBufferSize);

		assertArrayEquals(strings, result);
	}

	@Theory
	public void ofArrayWithAdditionalData(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize > 100) && writeBufferSize > 100);

		StreamCodec<String[]> codec = StreamCodecs.ofArray(StreamCodecs.ofString(), String[]::new);
		String[] strings = new String[arraySize];
		for (int i = 0; i < arraySize; i++) {
			strings[i] = String.valueOf(i);
		}
		String[] result = doTestWithAdditionalData(codec, strings, readBufferSize, writeBufferSize, 100, 100);

		assertArrayEquals(strings, result);
	}

	@Theory
	public void ofHeterogeneousArray(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize) {
		StreamCodec<Object[]> codec = StreamCodecs.ofArray(value -> switch (value % 3) {
			case 0 -> StreamCodecs.ofInt();
			case 1 -> StreamCodecs.ofString();
			case 2 -> StreamCodecs.ofBoolean();
			default -> throw new AssertionError();
		}, Object[]::new);
		Object[] objects = {0, "x", true, 12, "y", false, 34, "11241"};
		Object[] result = doTest(codec, objects, readBufferSize, writeBufferSize);

		assertArrayEquals(objects, result);
	}

	@Theory
	public void ofList(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int listSize) {
		assumeTrue((listSize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<List<String>> codec = StreamCodecs.ofList(StreamCodecs.ofString());
		List<String> strings = new ArrayList<>(listSize);
		for (int i = 0; i < listSize; i++) {
			strings.add(String.valueOf(i));
		}
		List<String> result = doTest(codec, strings, readBufferSize, writeBufferSize);

		assertEquals(strings, result);
	}

	@Theory
	public void ofHeterogeneousList(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize) {
		StreamCodec<List<Object>> codec = StreamCodecs.ofList(value -> switch (value % 3) {
			case 0 -> StreamCodecs.ofInt();
			case 1 -> StreamCodecs.ofString();
			case 2 -> StreamCodecs.ofBoolean();
			default -> throw new AssertionError();
		});
		List<Object> objects = List.of(0, "x", true, 12, "y", false, 34, "11241");
		List<Object> result = doTest(codec, objects, readBufferSize, writeBufferSize);

		assertEquals(objects, result);
	}

	@Theory
	public void ofSubtypeBuilder(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize) {
		SubtypeBuilder<Number> subtypeBuilder = new SubtypeBuilder<>();
		subtypeBuilder
				.add(Integer.class, StreamCodecs.ofInt())
				.add(Long.class, StreamCodecs.ofLong())
				.add(Float.class, StreamCodecs.ofFloat())
				.add(Byte.class, StreamCodecs.ofByte());

		StreamCodec<Number> codec = subtypeBuilder.build();

		byte b = Byte.MAX_VALUE;
		byte bResult = (byte) doTest(codec, b, readBufferSize, writeBufferSize);
		assertEquals(b, bResult);

		int i = Integer.MIN_VALUE;
		int iResult = (int) doTest(codec, i, readBufferSize, writeBufferSize);
		assertEquals(i, iResult);

		long l = Long.MAX_VALUE;
		long lResult = (long) doTest(codec, l, readBufferSize, writeBufferSize);
		assertEquals(l, lResult);

		float f = Float.MIN_VALUE;
		float fResult = (float) doTest(codec, f, readBufferSize, writeBufferSize);
		assertEquals(f, fResult, 1e-10);
	}

	@Theory
	public void ofSubtypeMap(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize) {
		LinkedHashMap<Class<? extends Number>, StreamCodec<? extends Number>> map = new LinkedHashMap<>();
		map.put(Integer.class, StreamCodecs.ofInt());
		map.put(Long.class, StreamCodecs.ofLong());
		map.put(Float.class, StreamCodecs.ofFloat());
		map.put(Byte.class, StreamCodecs.ofByte());

		StreamCodec<Number> codec = StreamCodecs.ofSubtype(map);

		byte b = Byte.MAX_VALUE;
		byte bResult = (byte) doTest(codec, b, readBufferSize, writeBufferSize);
		assertEquals(b, bResult);

		int i = Integer.MIN_VALUE;
		int iResult = (int) doTest(codec, i, readBufferSize, writeBufferSize);
		assertEquals(i, iResult);

		long l = Long.MAX_VALUE;
		long lResult = (long) doTest(codec, l, readBufferSize, writeBufferSize);
		assertEquals(l, lResult);

		float f = Float.MIN_VALUE;
		float fResult = (float) doTest(codec, f, readBufferSize, writeBufferSize);
		assertEquals(f, fResult, 1e-10);
	}

	@Theory
	public void ofVarIntArrayList(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize) {
		StreamCodec<List<int[]>> codec = StreamCodecs.ofList(StreamCodecs.ofVarIntArray());
		List<int[]> expected = List.of(
				new int[]{-1, -2, -3},
				new int[]{1, 2, 3},
				new int[]{-1, 0, 1},
				new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}
		);
		List<int[]> result = doTest(codec, expected, readBufferSize, writeBufferSize);

		assertEquals(expected.size(), result.size());

		for (int i = 0; i < expected.size(); i++) {
			assertArrayEquals(expected.get(i), result.get(i));
		}
	}

	@Theory
	public void ofVarLongArrayList(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize) {
		StreamCodec<List<long[]>> codec = StreamCodecs.ofList(StreamCodecs.ofVarLongArray());
		List<long[]> expected = List.of(
				new long[]{-1, -2, -3},
				new long[]{1, 2, 3},
				new long[]{-1, 0, 1},
				new long[]{Long.MIN_VALUE, Long.MAX_VALUE}
		);
		List<long[]> result = doTest(codec, expected, readBufferSize, writeBufferSize);

		assertEquals(expected.size(), result.size());

		for (int i = 0; i < expected.size(); i++) {
			assertArrayEquals(expected.get(i), result.get(i));
		}
	}

	@Theory
	public void ofIntArray(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<int[]> codec = StreamCodecs.ofIntArray();
		int[] ints = new int[arraySize];
		Random random = ThreadLocalRandom.current();
		for (int i = 0; i < ints.length; i++) {
			ints[i] = random.nextInt();
		}

		int[] result = doTest(codec, ints, readBufferSize, writeBufferSize);

		assertArrayEquals(ints, result);
	}

	@Theory
	public void ofIntArrayWithAdditionalData(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize > 100) && writeBufferSize > 100);

		StreamCodec<int[]> codec = StreamCodecs.ofIntArray();
		int[] ints = new int[arraySize];
		Random random = ThreadLocalRandom.current();
		for (int i = 0; i < ints.length; i++) {
			ints[i] = random.nextInt();
		}

		int[] result = doTestWithAdditionalData(codec, ints, readBufferSize, writeBufferSize, 4, 4);

		assertArrayEquals(ints, result);
	}

	@Theory
	public void ofByteArray(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<byte[]> codec = StreamCodecs.ofByteArray();
		byte[] bytes = new byte[arraySize];
		Random random = ThreadLocalRandom.current();
		random.nextBytes(bytes);

		byte[] result = doTest(codec, bytes, readBufferSize, writeBufferSize);

		assertArrayEquals(bytes, result);
	}

	@Theory
	public void ofByteArrayWithAdditionalData(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<byte[]> codec = StreamCodecs.ofByteArray();
		byte[] bytes = new byte[arraySize];
		Random random = ThreadLocalRandom.current();
		random.nextBytes(bytes);

		byte[] result = doTestWithAdditionalData(codec, bytes, readBufferSize, writeBufferSize, 4, 4);

		assertArrayEquals(bytes, result);
	}

	@Theory
	public void ofVarIntArray(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<int[]> codec = StreamCodecs.ofVarIntArray();
		int[] ints = new int[arraySize];
		Random random = ThreadLocalRandom.current();
		for (int i = 0; i < ints.length; i++) {
			ints[i] = random.nextInt();
		}

		int[] result = doTest(codec, ints, readBufferSize, writeBufferSize);

		assertArrayEquals(ints, result);
	}

	@Theory
	public void ofVarIntArrayWithAdditionalData(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<int[]> codec = StreamCodecs.ofVarIntArray();
		int[] ints = new int[arraySize];
		Random random = ThreadLocalRandom.current();
		for (int i = 0; i < ints.length; i++) {
			ints[i] = random.nextInt();
		}

		int[] result = doTestWithAdditionalData(codec, ints, readBufferSize, writeBufferSize, 5, 5);

		assertArrayEquals(ints, result);
	}

	@Theory
	public void ofVarLongArray(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<long[]> codec = StreamCodecs.ofVarLongArray();
		long[] longs = new long[arraySize];
		Random random = ThreadLocalRandom.current();
		for (int i = 0; i < longs.length; i++) {
			longs[i] = random.nextLong();
		}

		long[] result = doTest(codec, longs, readBufferSize, writeBufferSize);

		assertArrayEquals(longs, result);
	}

	@Theory
	public void ofVarLongArrayWithAdditionalData(@FromDataPoints("bufferSizes") int readBufferSize, @FromDataPoints("bufferSizes") int writeBufferSize,
			@FromDataPoints("containerSizes") int arraySize) {
		assumeTrue((arraySize != 1_000_000 || readBufferSize >= 100) && writeBufferSize >= 100);

		StreamCodec<long[]> codec = StreamCodecs.ofVarLongArray();
		long[] longs = new long[arraySize];
		Random random = ThreadLocalRandom.current();
		for (int i = 0; i < longs.length; i++) {
			longs[i] = random.nextLong();
		}

		long[] result = doTestWithAdditionalData(codec, longs, readBufferSize, writeBufferSize, 10, 10);

		assertArrayEquals(longs, result);
	}

	private <T> T doTest(StreamCodec<T> codec, T value, int readBufferSize, int writeBufferSize) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput output = StreamOutput.create(baos, writeBufferSize)) {
			codec.encode(output, value);
		} catch (IOException e) {
			throw new AssertionError(e);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput input = StreamInput.create(bais, readBufferSize)) {
			return codec.decode(input);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	private <T> T doTestWithAdditionalData(StreamCodec<T> codec, T value, int readBufferSize, int writeBufferSize,
			int additionalDataBefore, int additionalDataAfter) {
		StreamCodec<T> codecWithAdditionalData = new StreamCodecWithAdditionalData<>(codec, additionalDataBefore, additionalDataAfter);
		return doTest(codecWithAdditionalData, value, readBufferSize, writeBufferSize);
	}

	private static class StreamCodecWithAdditionalData<T> implements StreamCodec<T> {
		private final StreamCodec<T> codec;
		private final int additionalDataBefore;
		private final int additionalDataAfter;

		public StreamCodecWithAdditionalData(StreamCodec<T> codec, int additionalDataBefore, int additionalDataAfter) {
			this.codec = codec;
			this.additionalDataBefore = additionalDataBefore;
			this.additionalDataAfter = additionalDataAfter;
		}

		@Override
		public void encode(StreamOutput output, T item) throws IOException {
			for (int i = 0; i < additionalDataBefore; i++) {
				output.writeByte((byte) 1);
			}
			codec.encode(output, item);
			for (int i = 0; i < additionalDataAfter; i++) {
				output.writeByte((byte) 1);
			}
		}

		@Override
		public T decode(StreamInput input) throws IOException {
			for (int i = 0; i < additionalDataBefore; i++) {
				byte b = input.readByte();
				assertEquals(1, b);
			}
			T decoded = codec.decode(input);
			for (int i = 0; i < additionalDataAfter; i++) {
				byte b = input.readByte();
				assertEquals(1, b);
			}
			return decoded;
		}
	}

	@Test
	public void testReference() throws IOException {
		StreamCodec<String> codec = StreamCodecs.reference(StreamCodecs.ofString());
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try (StreamOutput output = StreamOutput.create(os)) {
			String s1 = "s1";
			String s2 = "s2";
			String s3 = "s3";
			codec.encode(output, s1);
			codec.encode(output, s2);
			codec.encode(output, s1);
			codec.encode(output, s3);
			codec.encode(output, s2);
		}

		codec = StreamCodecs.reference(StreamCodecs.ofString());
		byte[] buf = os.toByteArray();
		try (StreamInput input = StreamInput.create(new ByteArrayInputStream(buf))) {
			String s1 = codec.decode(input);
			assertEquals("s1", s1);
			String s2 = codec.decode(input);
			assertEquals("s2", s2);
			assertSame(s1, codec.decode(input));
			String s3 = codec.decode(input);
			assertEquals("s3", s3);
			assertSame(s2, codec.decode(input));
		}
	}
}
