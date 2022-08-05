package io.activej.dataflow.proto.calcite.serializer;

import io.activej.codegen.util.Primitives;
import io.activej.dataflow.proto.calcite.JavaTypeProto;
import io.activej.dataflow.proto.calcite.JavaTypeProto.JavaType;
import io.activej.serializer.CorruptedDataException;
import io.activej.types.Types;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class JavaTypeConverter {
	private static final Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
	private static final Class<?> UNKNOWN_CLASS = JavaTypeConverter.class;

	static {
		for (Class<?> primitiveType : Primitives.allPrimitiveTypes()) {
			precacheClass(primitiveType);
		}
	}

	private static void precacheClass(Class<?> cls){
		CLASS_CACHE.put(cls.getName(), cls);
	}

	private JavaTypeConverter() {
	}

	static JavaType convert(Type type) {
		JavaType.Builder builder = JavaType.newBuilder();
		if (type instanceof Class<?> cls) {
			builder.setJavaClass(
					JavaType.JavaClass.newBuilder()
							.setClassName(cls.getName()));
		} else if (type instanceof ParameterizedType parameterizedType) {
			JavaTypeProto.JavaTypeNullable.Builder ownerTypeBuilder = JavaTypeProto.JavaTypeNullable.newBuilder();
			Type ownerType = parameterizedType.getOwnerType();
			if (ownerType == null) {
				ownerTypeBuilder.setIsNull(true);
			} else {
				ownerTypeBuilder.setType(convert(ownerType));
			}

			builder.setJavaParameterizedType(
					JavaType.JavaParameterizedType.newBuilder()
							.setOwnerType(ownerTypeBuilder)
							.setRawType(convert(parameterizedType.getRawType()))
							.addAllParameters(Arrays.stream(parameterizedType.getActualTypeArguments())
									.map(JavaTypeConverter::convert)
									.toList()));
		} else if (type instanceof GenericArrayType genericArrayType) {
			builder.setJavaGenericArrayType(
					JavaType.JavaGenericArrayType.newBuilder()
							.setComponentType(convert(genericArrayType.getGenericComponentType())));
		} else {
			throw new IllegalArgumentException("Unsupported type: " + type);
		}
		return builder.build();
	}

	static Type convert(JavaType javaType) {
		return switch (javaType.getJavaTypeCase()) {
			case JAVA_CLASS -> {
				String className = javaType.getJavaClass().getClassName();
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

				yield cls;
			}
			case JAVA_PARAMETERIZED_TYPE -> {
				JavaType.JavaParameterizedType javaParameterizedType = javaType.getJavaParameterizedType();

				JavaTypeProto.JavaTypeNullable javaOwnerType = javaParameterizedType.getOwnerType();
				Type ownerType = javaOwnerType.hasIsNull() ? null : convert(javaOwnerType.getType());
				Type rawType = convert(javaParameterizedType.getRawType());
				Type[] parameters = javaParameterizedType.getParametersList().stream()
						.map(JavaTypeConverter::convert)
						.toArray(Type[]::new);

				yield Types.parameterizedType(ownerType, rawType, parameters);
			}
			case JAVA_GENERIC_ARRAY_TYPE -> {
				JavaType.JavaGenericArrayType javaGenericArrayType = javaType.getJavaGenericArrayType();

				Type componentType = convert(javaGenericArrayType.getComponentType());

				yield Types.genericArrayType(componentType);
			}
			case JAVATYPE_NOT_SET -> throw new CorruptedDataException("Java type not set");
		};
	}
}
