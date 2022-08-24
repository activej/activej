package io.activej.serializer.stream;

import io.activej.serializer.stream.StreamCodecs.SubtypeBuilder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.*;

public class StreamCodecsTest {

	@Test
	public void ofString() {
		StreamCodec<String> codec = StreamCodecs.ofString();
		String string = "test";
		String result = doTest(codec, string);

		assertEquals(string, result);
	}

	@Test
	public void ofArray() {
		StreamCodec<String[]> codec = StreamCodecs.ofArray(StreamCodecs.ofString(), String[]::new);
		String[] strings = {"a", "b", "c"};
		String[] result = doTest(codec, strings);

		assertArrayEquals(strings, result);
	}

	@Test
	public void ofHeterogeneousArray() {
		StreamCodec<Object[]> codec = StreamCodecs.ofArray(value -> {
			switch (value % 3) {
				case 0:
					return StreamCodecs.ofInt();
				case 1:
					return StreamCodecs.ofString();
				case 2:
					return StreamCodecs.ofBoolean();
				default:
					throw new AssertionError();
			}
		}, Object[]::new);
		Object[] objects = {0, "x", true, 12, "y", false, 34, "11241"};
		Object[] result = doTest(codec, objects);

		assertArrayEquals(objects, result);
	}

	@Test
	public void ofList() {
		StreamCodec<List<String>> codec = StreamCodecs.ofList(StreamCodecs.ofString());
		List<String> strings = Arrays.asList("a", "b", "c");
		List<String> result = doTest(codec, strings);

		assertEquals(strings, result);
	}

	@Test
	public void ofHeterogeneousList() {
		StreamCodec<List<Object>> codec = StreamCodecs.ofList(value -> {
			switch (value % 3) {
				case 0:
					return StreamCodecs.ofInt();
				case 1:
					return StreamCodecs.ofString();
				case 2:
					return StreamCodecs.ofBoolean();
				default:
					throw new AssertionError();
			}
		});
		List<Object> objects = Arrays.asList(0, "x", true, 12, "y", false, 34, "11241");
		List<Object> result = doTest(codec, objects);

		assertEquals(objects, result);
	}

	@Test
	public void ofSubtypeBuilder() {
		SubtypeBuilder<Number> subtypeBuilder = new SubtypeBuilder<>();
		subtypeBuilder
				.add(Integer.class, StreamCodecs.ofInt())
				.add(Long.class, StreamCodecs.ofLong())
				.add(Float.class, StreamCodecs.ofFloat())
				.add(Byte.class, StreamCodecs.ofByte());

		StreamCodec<Number> codec = subtypeBuilder.build();

		byte b = Byte.MAX_VALUE;
		byte bResult = (byte) doTest(codec, b);
		assertEquals(b, bResult);

		int i = Integer.MIN_VALUE;
		int iResult = (int) doTest(codec, i);
		assertEquals(i, iResult);

		long l = Long.MAX_VALUE;
		long lResult = (long) doTest(codec, l);
		assertEquals(l, lResult);

		float f = Float.MIN_VALUE;
		float fResult = (float) doTest(codec, f);
		assertEquals(f, fResult, 1e-10);
	}

	@Test
	public void ofSubtypeMap() {
		LinkedHashMap<Class<? extends Number>, StreamCodec<? extends Number>> map = new LinkedHashMap<>();
		map.put(Integer.class, StreamCodecs.ofInt());
		map.put(Long.class, StreamCodecs.ofLong());
		map.put(Float.class, StreamCodecs.ofFloat());
		map.put(Byte.class, StreamCodecs.ofByte());

		StreamCodec<Number> codec = StreamCodecs.ofSubtype(map);

		byte b = Byte.MAX_VALUE;
		byte bResult = (byte) doTest(codec, b);
		assertEquals(b, bResult);

		int i = Integer.MIN_VALUE;
		int iResult = (int) doTest(codec, i);
		assertEquals(i, iResult);

		long l = Long.MAX_VALUE;
		long lResult = (long) doTest(codec, l);
		assertEquals(l, lResult);

		float f = Float.MIN_VALUE;
		float fResult = (float) doTest(codec, f);
		assertEquals(f, fResult, 1e-10);
	}

	@Test
	public void ofByteArray() {
		StreamCodec<byte[]> codec = StreamCodecs.ofByteArray();
		byte[] expected = new byte[1024];
		for (int i = 0; i < expected.length; i++) {
			expected[i] = (byte) i;
		}
		byte[] actual = doTest(codec, expected);
		assertArrayEquals(expected, actual);
	}

	@Test
	public void ofIntArray() {
		StreamCodec<int[]> codec = StreamCodecs.ofIntArray();
		int[] expected = new int[1024];
		for (int i = 0; i < expected.length; i++) {
			expected[i] = i;
		}
		int[] actual = doTest(codec, expected);
		assertArrayEquals(expected, actual);
	}

	@Test
	public void ofVarIntArrayList() {
		StreamCodec<List<int[]>> codec = StreamCodecs.ofList(StreamCodecs.ofVarIntArray());
		List<int[]> expected = Arrays.asList(
				new int[]{-1, -2, -3},
				new int[]{1, 2, 3},
				new int[]{-1, 0, 1},
				new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}
		);
		List<int[]> result = doTest(codec, expected);

		assertEquals(expected.size(), result.size());

		for (int i = 0; i < expected.size(); i++) {
			assertArrayEquals(expected.get(i), result.get(i));
		}
	}

	@Test
	public void ofVarLongArrayList() {
		StreamCodec<List<long[]>> codec = StreamCodecs.ofList(StreamCodecs.ofVarLongArray());
		List<long[]> expected = Arrays.asList(
				new long[]{-1, -2, -3},
				new long[]{1, 2, 3},
				new long[]{-1, 0, 1},
				new long[]{Long.MIN_VALUE, Long.MAX_VALUE}
		);
		List<long[]> result = doTest(codec, expected);

		assertEquals(expected.size(), result.size());

		for (int i = 0; i < expected.size(); i++) {
			assertArrayEquals(expected.get(i), result.get(i));
		}
	}

	private static <T> T doTest(StreamCodec<T> codec, T value) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput output = StreamOutput.create(baos, 1)) {
			codec.encode(output, value);
		} catch (IOException e) {
			throw new AssertionError(e);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput input = StreamInput.create(bais, 5)) {
			return codec.decode(input);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
}
