package io.activej.streamcodecs;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.BinarySerializers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public final class StreamInputOutputTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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
		StreamCodec<byte[]> codec = StreamCodec.ofBinarySerializer(serializer);
		int int1 = 123;
		int int2 = -567;
		byte[] array = new byte[10 * 1024];

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (StreamOutput streamOutput = StreamOutput.create(baos, 1)) {
			streamOutput.writeInt(int1);
			streamOutput.writeInt(int2);
			codec.encode(streamOutput, array);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (StreamInput streamInput = StreamInput.create(bais)) {
			assertEquals(int1, streamInput.readInt());
			assertEquals(int2, streamInput.readInt());
			assertArrayEquals(array, codec.decode(streamInput));
		}
	}

	@Test
	public void largeItems() throws IOException {
		int nearMaxSize = (1 << 28) // StreamOutput.MAX_SIZE
				- 4 // encoded size of an array
				- 1;
		for (byte[] array : List.of(new byte[1024], new byte[32 * 1024], new byte[10 * 1024 * 1024], new byte[nearMaxSize])) {
			BinarySerializer<byte[]> serializer = BinarySerializers.BYTES_SERIALIZER;
			StreamCodec<byte[]> codec = StreamCodec.ofBinarySerializer(serializer);
			ThreadLocalRandom.current().nextBytes(array);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (StreamOutput streamOutput = StreamOutput.create(baos, 1)) {
				codec.encode(streamOutput, array);
			}

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			try (StreamInput streamInput = StreamInput.create(bais)) {
				assertArrayEquals(array, codec.decode(streamInput));
			}
		}
	}

	@Test
	public void isEndOfStream() throws IOException {
		Path tempFile = temporaryFolder.newFile().toPath();

		try (StreamOutput output = StreamOutput.create(new BufferedOutputStream(Files.newOutputStream(tempFile)))) {
			output.writeInt(1);
		}

		try (StreamInput input = StreamInput.create(new BufferedInputStream(Files.newInputStream(tempFile)))) {
			assertFalse(input.isEndOfStream());
			input.readInt();
			assertTrue(input.isEndOfStream());
		}
	}

}
