package io.activej.serializer.stream;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class StreamCodecRegistryTest {

	@Test
	public void test1() throws IOException {
		StreamCodecRegistry registry = StreamCodecRegistry.createDefault();
		StreamCodec<List<String>> codec = registry.get(new StreamCodecT<List<String>>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput output = StreamOutput.create(baos, 1)) {
			codec.encode(output, asList("a", "b", "c"));
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput input = StreamInput.create(bais)) {
			assertEquals(asList("a", "b", "c"), codec.decode(input));
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
		StreamCodecRegistry registry = StreamCodecRegistry.createDefault()
				.with(A.class, StreamCodecs.ofNullable(new StreamCodec<A>() {
					@Override
					public A decode(StreamInput input) throws IOException {
						return new A(input.readVarInt());
					}

					@Override
					public void encode(StreamOutput output, A item) throws IOException {
						output.writeVarInt(item.x);
					}
				}));
		StreamCodec<List<A>> codec = registry.get(new StreamCodecT<List<A>>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput output = StreamOutput.create(baos, 1)) {
			codec.encode(output, asList(new A(1), new A(2), null, new A(3)));
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput input = StreamInput.create(bais)) {
			assertEquals(asList(new A(1), new A(2), null, new A(3)), codec.decode(input));
		}
	}

	@Test
	public void test3() throws IOException {
		StreamCodecRegistry registry = StreamCodecRegistry.createDefault();
		StreamCodec<int[]> codec = registry.get(new StreamCodecT<int[]>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput output = StreamOutput.create(baos, 1)) {
			codec.encode(output, new int[]{1, 2, 3});
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput input = StreamInput.create(bais)) {
			assertArrayEquals(new int[]{1, 2, 3}, codec.decode(input));
		}
	}

	@Test
	public void test4() throws IOException {
		StreamCodecRegistry registry = StreamCodecRegistry.createDefault();
		StreamCodec<int[][]> codec = registry.get(new StreamCodecT<int[][]>() {});

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput output = StreamOutput.create(baos, 1)) {
			codec.encode(output, new int[][]{new int[]{1, 2, 3}, new int[]{4, 5}});
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput input = StreamInput.create(bais)) {
			assertArrayEquals(new int[][]{new int[]{1, 2, 3}, new int[]{4, 5}}, codec.decode(input));
		}
	}

}
