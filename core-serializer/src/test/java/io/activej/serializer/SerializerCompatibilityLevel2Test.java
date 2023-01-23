package io.activej.serializer;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

@SuppressWarnings("unused")
public class SerializerCompatibilityLevel2Test {
	public static <T> T doTest(Class<T> type, T testData) {
		BinarySerializer<T> serializer = SerializerBuilder.builder(DefiningClassLoader.create())
				.withCompatibilityLevel(CompatibilityLevel.LEVEL_2)
				.build()
				.build(type);
		return doTest(testData, serializer, serializer);
	}

	public static <T, R> R doTest(T testData, BinarySerializer<T> serializer, BinarySerializer<R> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData);
		return deserializer.decode(array, 0);
	}

	public static class TestDataScalars {
		@Serialize
		public byte @SerializeNullable [] bytes;
	}

	@Test
	public void testScalars() {
		TestDataScalars testData1 = new TestDataScalars();

		testData1.bytes = new byte[]{1, 2, 3};

		TestDataScalars testData2 = doTest(TestDataScalars.class, testData1);

		assertArrayEquals(testData1.bytes, testData2.bytes);
	}
}
