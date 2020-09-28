package io.activej.serializer;

import io.activej.codegen.DefiningClassLoader;

public final class Utils {
	public static final DefiningClassLoader DEFINING_CLASS_LOADER = DefiningClassLoader.create();

	public static <T> T doTest(Class<T> type, T testData1) {
		BinarySerializer<T> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.build(type);
		return doTest(testData1, serializer, serializer);
	}

	public static <T> T doTest(T testData1, BinarySerializer<T> serializer, BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData1);
		return deserializer.decode(array, 0);
	}
}
