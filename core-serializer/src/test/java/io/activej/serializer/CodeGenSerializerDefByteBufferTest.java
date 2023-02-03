package io.activej.serializer;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.def.SerializerDefs;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.activej.serializer.Utils.DEFINING_CLASS_LOADER;
import static io.activej.serializer.Utils.doTest;
import static org.junit.Assert.*;

public class CodeGenSerializerDefByteBufferTest {

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Test
	public void test() {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array);

		BinarySerializer<ByteBuffer> serializerByteBuffer = SerializerFactory.builder()
				.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(false))
				.build()
				.create(DEFINING_CLASS_LOADER, ByteBuffer.class);
		ByteBuffer testBuffer2 = doTest(testBuffer1, serializerByteBuffer);

		assertNotNull(testBuffer2);
		assertEquals(testBuffer1, testBuffer2);
	}

	@Test
	public void testWrap() {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array);

		BinarySerializer<ByteBuffer> serializerByteBuffer = SerializerFactory.builder()
				.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(true))
				.build()
				.create(DEFINING_CLASS_LOADER, ByteBuffer.class);

		ByteBuffer testBuffer2 = doTest(testBuffer1, serializerByteBuffer);

		assertNotNull(testBuffer2);
		assertEquals(testBuffer1, testBuffer2);
	}

	@Test
	public void test2() {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array, 10, 100);

		BinarySerializer<ByteBuffer> serializer = SerializerFactory.builder()
				.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(false))
				.build()
				.create(DEFINING_CLASS_LOADER, ByteBuffer.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		ByteBuffer testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertEquals(testBuffer1, testBuffer3);

		int position = testBuffer3.position();
		assertEquals(10, testBuffer3.get(position));
		buffer[position] = 123;
		assertEquals(10, testBuffer3.get(position));
	}

	@Test
	public void testWrap2() {
		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array, 10, 100);

		BinarySerializer<ByteBuffer> serializer = SerializerFactory.builder()
				.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(true))
				.build()
				.create(DEFINING_CLASS_LOADER, ByteBuffer.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		ByteBuffer testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertEquals(testBuffer1, testBuffer3);

		int position = testBuffer3.position();
		assertEquals(10, testBuffer3.get(position));
		buffer[position] = 123;
		assertEquals(123, testBuffer3.get(position));
	}

	public static final class TestByteBufferData {
		private final ByteBuffer buffer;

		public TestByteBufferData(@Deserialize("buffer") ByteBuffer buffer) {
			this.buffer = buffer;
		}

		public TestByteBufferData() {
			this.buffer = null;
		}

		@Serialize
		public ByteBuffer getBuffer() {
			return buffer;
		}
	}

	@Test
	public void test3() {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufferData testBuffer1 = new TestByteBufferData(ByteBuffer.wrap(array, 10, 2));

		BinarySerializer<TestByteBufferData> serializer = SerializerFactory.builder()
				.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(false))
				.build()
				.create(DEFINING_CLASS_LOADER, TestByteBufferData.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		TestByteBufferData testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer3.getBuffer());
		assertEquals(testBuffer1.getBuffer(), testBuffer3.getBuffer());
	}

	@Test
	public void testWrap3() {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufferData testBuffer1 = new TestByteBufferData(ByteBuffer.wrap(array, 10, 100));

		BinarySerializer<TestByteBufferData> serializer = SerializerFactory.builder()
				.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(true))
				.build()
				.create(DEFINING_CLASS_LOADER, TestByteBufferData.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		TestByteBufferData testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer3.getBuffer());
		assertEquals(testBuffer1.getBuffer(), testBuffer3.getBuffer());
	}


	public static class TestByteBuffer {
		@Serialize
		@SerializeNullable
		public ByteBuffer buffer;
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testByteBuffer() {
		TestByteBuffer object = new TestByteBuffer();

		{
			BinarySerializer<TestByteBuffer> serializer = SerializerFactory.builder()
					.withCompatibilityLevel(CompatibilityLevel.LEVEL_2)
					.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(false))
					.build()
					.create(DEFINING_CLASS_LOADER, TestByteBuffer.class);
			TestByteBuffer deserialized = doTest(object, serializer);
			assertNull(deserialized.buffer);
		}

		{
			BinarySerializer<TestByteBuffer> serializer = SerializerFactory.builder()
					.with(ByteBuffer.class, ctx -> SerializerDefs.ofByteBuffer(false))
					.build()
					.create(DEFINING_CLASS_LOADER, TestByteBuffer.class);
			TestByteBuffer deserialized = doTest(object, serializer);
			assertNull(deserialized.buffer);
		}
	}

}
