package io.activej.serializer;

import io.activej.serializer.stream.StreamInput;
import io.activej.serializer.stream.StreamOutput;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public final class StreamInputOutputTest {
	@Test
	public void bufferSizeOne() throws IOException {
		int int1 = 123;
		int int2 = -567;

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput streamOutput = StreamOutput.create(baos, 1)) {
			streamOutput.writeInt(int1);
			streamOutput.writeInt(int2);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput streamInput = StreamInput.create(bais)) {
			assertEquals(int1, streamInput.readInt());
			assertEquals(int2, streamInput.readInt());
		}
	}

	@Test
	public void bufferSizeOneWithBinarySerializer() throws IOException {
		BinarySerializer<byte[]> serializer = BinarySerializers.BYTES_SERIALIZER;
		int int1 = 123;
		int int2 = -567;
		byte[] array = new byte[10 * 1024];

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput streamOutput = StreamOutput.create(baos, 1)) {
			streamOutput.writeInt(int1);
			streamOutput.writeInt(int2);
			streamOutput.serialize(serializer, array);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput streamInput = StreamInput.create(bais)) {
			assertEquals(int1, streamInput.readInt());
			assertEquals(int2, streamInput.readInt());
			assertArrayEquals(array, streamInput.deserialize(serializer));
		}
	}

	@Test
	public void largeItems() throws IOException {
		int nearMaxSize = (1 << 28) // StreamOutput.MAX_SIZE
				- 4 // encoded size of an array
				- 1;
		for (byte[] array : asList(new byte[1024], new byte[32 * 1024], new byte[10 * 1024 * 1024], new byte[nearMaxSize])) {
			BinarySerializer<byte[]> serializer = BinarySerializers.BYTES_SERIALIZER;
			ThreadLocalRandom.current().nextBytes(array);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (StreamOutput streamOutput = StreamOutput.create(baos, 1)) {
				streamOutput.serialize(serializer, array);
			}

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			try (StreamInput streamInput = StreamInput.create(bais)) {
				assertArrayEquals(array, streamInput.deserialize(serializer));
			}
		}
	}
}
