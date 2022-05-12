package io.activej.serializer;

import io.activej.codegen.DefiningClassLoader;

public final class Utils {
	public static final DefiningClassLoader DEFINING_CLASS_LOADER = DefiningClassLoader.create();

	public static <T> T doTest(Class<T> type, T testData) {
		BinarySerializer<T> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.build(type);
		return doTest(testData, serializer);
	}

	public static <T> T doTest(T testData, BinarySerializer<T> serializer) {
		return doTest(testData, serializer, serializer);
	}

	public static <T, R> R doTest(T testData, BinarySerializer<T> serializer, BinarySerializer<R> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData);
		return deserializer.decode(array, 0);
	}

}
