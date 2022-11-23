package io.activej.dataflow.proto.calcite.serializer;

import io.activej.dataflow.proto.JavaClassProto;
import io.activej.dataflow.proto.calcite.JavaTypeProto;
import io.activej.dataflow.proto.calcite.JavaTypeProto.JavaType;
import io.activej.dataflow.proto.serializer.JavaClassConverter;
import io.activej.serializer.CorruptedDataException;
import io.activej.types.Types;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

final class JavaTypeConverter {
	private JavaTypeConverter() {
	}

	static JavaType convert(Type type) {
		JavaType.Builder builder = JavaType.newBuilder();
		if (type instanceof Class<?> cls) {
			builder.setJavaClass(
					JavaClassProto.JavaClass.newBuilder()
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
			case JAVA_CLASS -> JavaClassConverter.convert(javaType.getJavaClass());
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
