package io.activej.serializer;

import io.activej.serializer.datastream.DataInputStreamEx;
import io.activej.serializer.datastream.DataOutputStreamEx;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public final class DataStreamExTest {
	@Test
	public void bufferSizeOne() throws IOException {
		int int1 = 123;
		int int2 = -567;

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(baos, 1)) {
			dataOutputStream.writeInt(int1);
			dataOutputStream.writeInt(int2);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(bais)) {
			assertEquals(int1, dataInputStream.readInt());
			assertEquals(int2, dataInputStream.readInt());
		}
	}

	@Test
	public void bufferSizeOneWithBinarySerializer() throws IOException {
		BinarySerializer<byte[]> serializer = BinarySerializers.BYTES_SERIALIZER;
		int int1 = 123;
		int int2 = -567;
		byte[] array = new byte[10 * 1024];

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(baos, 1)) {
			dataOutputStream.writeInt(int1);
			dataOutputStream.writeInt(int2);
			dataOutputStream.serialize(serializer, array);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(bais)) {
			assertEquals(int1, dataInputStream.readInt());
			assertEquals(int2, dataInputStream.readInt());
			assertArrayEquals(array, dataInputStream.deserialize(serializer));
		}
	}

	@Test
	public void initialHeaderSizeIsLessThanActual() throws IOException {
		for (byte[] array : asList(new byte[1024], new byte[32 * 1024])) {
			BinarySerializer<byte[]> serializer = BinarySerializers.BYTES_SERIALIZER;
			ThreadLocalRandom.current().nextBytes(array);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(baos, 1)) {
				dataOutputStream.serialize(serializer, array, 1);
			}

			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(bais)) {
				assertArrayEquals(array, dataInputStream.deserialize(serializer));
			}
		}
	}

	@Test
	public void actualSizeIsOutOfBounds() throws IOException {
		BinarySerializer<byte[]> serializer = BinarySerializers.BYTES_SERIALIZER;
		byte[] array = new byte[2 * 1024 * 1024 + 1];
		ThreadLocalRandom.current().nextBytes(array);

		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(new ByteArrayOutputStream(), 1)) {
			dataOutputStream.serialize(serializer, array, 1);
			fail();
		} catch (IllegalStateException e){
			assertSame(DataOutputStreamEx.SIZE_EXCEPTION, e);
		}
	}

}
