package io.activej.dataflow.proto.serializer;

import io.activej.codegen.util.Primitives;
import io.activej.dataflow.proto.JavaClassProto.JavaClass;
import io.activej.serializer.CorruptedDataException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class JavaClassConverter {
	private static final Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
	private static final Class<?> UNKNOWN_CLASS = JavaClassConverter.class;

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

	public static <T> Class<T> convert(JavaClass javaClass) {
		String className = javaClass.getClassName();
		Class<?> cls = CLASS_CACHE.computeIfAbsent(className, $ -> {
			try {
				return Class.forName(className);
			} catch (ClassNotFoundException e) {
				return UNKNOWN_CLASS;
			}
		});

		if (cls == UNKNOWN_CLASS) {
			throw new CorruptedDataException("Cannot find class: " + className);
		}

		//noinspection unchecked
		return (Class<T>) cls;
	}
}
