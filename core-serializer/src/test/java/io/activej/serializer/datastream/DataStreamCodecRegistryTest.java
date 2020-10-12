package io.activej.serializer.datastream;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DataStreamCodecRegistryTest {

	@Test
	public void test1() throws IOException {
		DataStreamCodecRegistry registry = DataStreamCodecRegistry.createDefault();
		DataStreamCodec<List<String>> codec = registry.get(new DataStreamCodecT<List<String>>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(baos, 1)) {
			codec.encode(dataOutputStream, asList("a", "b", "c"));
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(bais)) {
			assertEquals(asList("a", "b", "c"), codec.decode(dataInputStream));
		}
	}

	private static final class A {
		final int x;

		private A(int x) {this.x = x;}

		@Override
		public boolean equals(Object o) {
			return o != null && getClass() == o.getClass() && x == ((A) o).x;
		}

		@Override
		public int hashCode() {
			return Objects.hash(x);
		}
	}

	@Test
	public void test2() throws IOException {
		DataStreamCodecRegistry registry = DataStreamCodecRegistry.createDefault()
				.with(A.class, DataStreamCodecs.ofNullable(new DataStreamCodec<A>() {
					@Override
					public A decode(DataInputStreamEx stream) throws IOException {
						return new A(stream.readVarInt());
					}

					@Override
					public void encode(DataOutputStreamEx stream, A item) throws IOException {
						stream.writeVarInt(item.x);
					}
				}));
		DataStreamCodec<List<A>> codec = registry.get(new DataStreamCodecT<List<A>>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DataOutputStreamEx output = DataOutputStreamEx.create(baos, 1)) {
			codec.encode(output, asList(new A(1), new A(2), null, new A(3)));
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (DataInputStreamEx input = DataInputStreamEx.create(bais)) {
			assertEquals(asList(new A(1), new A(2), null, new A(3)), codec.decode(input));
		}
	}

	@Test
	public void test3() throws IOException {
		DataStreamCodecRegistry registry = DataStreamCodecRegistry.createDefault();
		DataStreamCodec<int[]> codec = registry.get(new DataStreamCodecT<int[]>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DataOutputStreamEx output = DataOutputStreamEx.create(baos, 1)) {
			codec.encode(output, new int[]{1, 2, 3});
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (DataInputStreamEx input = DataInputStreamEx.create(bais)) {
			assertArrayEquals(new int[]{1, 2, 3}, codec.decode(input));
		}
	}

	@Test
	public void test4() throws IOException {
		DataStreamCodecRegistry registry = DataStreamCodecRegistry.createDefault();
		DataStreamCodec<int[][]> codec = registry.get(new DataStreamCodecT<int[][]>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DataOutputStreamEx output = DataOutputStreamEx.create(baos, 1)) {
			codec.encode(output, new int[][]{new int[]{1, 2, 3}, new int[]{4, 5}});
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (DataInputStreamEx input = DataInputStreamEx.create(bais)) {
			assertArrayEquals(new int[][]{new int[]{1, 2, 3}, new int[]{4, 5}}, codec.decode(input));
		}
	}

}
