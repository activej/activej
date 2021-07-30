package io.activej.serializer;

import io.activej.codegen.DefiningClassLoader;

public final class Utils {
	public static final DefiningClassLoader DEFINING_CLASS_LOADER = DefiningClassLoader.create();

	static final boolean AT_LEAST_JAVA_9;
	static final boolean AT_LEAST_JAVA_12;

	static {
		String property = System.getProperty("java.specification.version");
		if (property.contains(".")) {
			AT_LEAST_JAVA_9 = false;
			AT_LEAST_JAVA_12 = false;
		} else {
			AT_LEAST_JAVA_9 = true;
			AT_LEAST_JAVA_12 = Integer.parseInt(property) >= 12;
		}
	}

	public static <T> T doTest(Class<T> type, T testData) {
		BinarySerializer<T> serializer = SerializerBuilder.create(DEFINING_CLASS_LOADER)
				.build(type);
		return doTest(testData, serializer);
	}

	public static <T> T doTest(T testData, BinarySerializer<T> serializer) {
		return doTest(testData, serializer, serializer);
	}

	public static <T> T doTest(T testData, BinarySerializer<T> serializer, BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData);
		return deserializer.decode(array, 0);
	}

}
