package io.activej.serializer;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer2.SerializerBuilder2;
import org.junit.Assert;

import java.util.Arrays;

public final class Utils {
	public static final DefiningClassLoader DEFINING_CLASS_LOADER = DefiningClassLoader.create();

	public static <T> T doTest(Class<T> type, T testData) {
		BinarySerializer<T> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.build(type);
		BinarySerializer<T> serializer2 = SerializerBuilder2.create(DEFINING_CLASS_LOADER)
				.build(type);
		return doTest(testData, serializer, serializer2, serializer2);
	}

	public static <T> T doTest(T testData, BinarySerializer<T> serializer) {
		return doTest(testData, serializer, serializer, serializer);
	}

	public static <T> T doTest(T testData, BinarySerializer<T> serializer, BinarySerializer<T> deserializer) {
		return doTest(testData, serializer, serializer, deserializer);
	}

	public static <T> T doTest(T testData, BinarySerializer<T> serializer, BinarySerializer<T> serializer2, BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		int pos = serializer.encode(array, 0, testData);
		byte[] array2 = new byte[1000];
		int pos2 = serializer2.encode(array2, 0, testData);
		Assert.assertArrayEquals(Arrays.copyOf(array, pos), Arrays.copyOf(array2, pos2));
		return deserializer.decode(array, 0);
	}
}
