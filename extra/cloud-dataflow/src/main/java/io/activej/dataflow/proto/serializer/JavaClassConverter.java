package io.activej.dataflow.proto.serializer;

import io.activej.codegen.util.Primitives;
import io.activej.dataflow.proto.JavaClassProto.JavaClass;
import io.activej.serializer.CorruptedDataException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class JavaClassConverter {
	private static final Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

	static {
		for (Class<?> primitiveType : Primitives.allPrimitiveTypes()) {
			precacheClass(primitiveType);
		}
	}

	private static void precacheClass(Class<?> cls) {
		CLASS_CACHE.put(cls.getName(), cls);
	}

	private JavaClassConverter() {
	}

	public static JavaClass convert(Class<?> cls) {
		return JavaClass.newBuilder()
				.setClassName(cls.getName())
				.build();
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> convert(JavaClass javaClass) {
		return (Class<T>) CLASS_CACHE.computeIfAbsent(javaClass.getClassName(), className -> {
			try {
				return Class.forName(className);
			} catch (ClassNotFoundException e) {
				throw new CorruptedDataException("Cannot find class: " + className);
			}
		});
	}
}
