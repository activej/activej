/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.codegen.util;

import io.activej.codegen.Context;
import io.activej.types.Primitives;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.objectweb.asm.Type.*;
import static org.objectweb.asm.commons.Method.getMethod;

@SuppressWarnings("WeakerAccess")
public final class Utils {
	private static final Type OBJECT_TYPE = Type.getType(Object.class);
	private static final Map<String, Type> WRAPPER_TO_PRIMITIVE = new HashMap<>();

	static {
		for (Class<?> primitiveType : Primitives.allPrimitiveTypes()) {
			Class<?> wrappedType = Primitives.wrap(primitiveType);
			WRAPPER_TO_PRIMITIVE.put(wrappedType.getName(), getType(primitiveType));
		}
	}

	private static final Type WRAPPED_BOOLEAN_TYPE = getType(Boolean.class);
	private static final Type WRAPPED_CHAR_TYPE = getType(Character.class);
	private static final Type WRAPPED_BYTE_TYPE = getType(Byte.class);
	private static final Type WRAPPED_SHORT_TYPE = getType(Short.class);
	private static final Type WRAPPED_INT_TYPE = getType(Integer.class);
	private static final Type WRAPPED_FLOAT_TYPE = getType(Float.class);
	private static final Type WRAPPED_LONG_TYPE = getType(Long.class);
	private static final Type WRAPPED_DOUBLE_TYPE = getType(Double.class);
	private static final Type WRAPPED_VOID_TYPE = getType(Void.class);

	public static boolean isEqualType(@Nullable Type type1, @Nullable Type type2) {
		if (type1 == null && type2 == null) return true;
		if (type1 == null || type2 == null) return false;
		int sort = type1.getSort();
		if (sort != type2.getSort()) return false;
		if (sort <= DOUBLE) return true;
		return type1.equals(type2);
	}

	public static boolean isPrimitiveType(Type type) {
		return type.getSort() > VOID && type.getSort() <= DOUBLE;
	}

	public static boolean isWrapperType(Type type) {
		return type.getSort() == OBJECT &&
				WRAPPER_TO_PRIMITIVE.containsKey(type.getClassName());
	}

	public static Method unwrapToPrimitive(Type primitiveType) {
		return switch (primitiveType.getSort()) {
			case BOOLEAN -> getMethod("boolean booleanValue()");
			case CHAR -> getMethod("char charValue()");
			case BYTE -> getMethod("byte byteValue()");
			case SHORT -> getMethod("short shortValue()");
			case INT -> getMethod("int intValue()");
			case FLOAT -> getMethod("float floatValue()");
			case LONG -> getMethod("long longValue()");
			case DOUBLE -> getMethod("double doubleValue()");
			default -> throw new IllegalArgumentException(format("No primitive value method for %s ", primitiveType.getClassName()));
		};
	}

	public static Type wrap(Type type) {
		return switch (type.getSort()) {
			case BOOLEAN -> WRAPPED_BOOLEAN_TYPE;
			case CHAR -> WRAPPED_CHAR_TYPE;
			case BYTE -> WRAPPED_BYTE_TYPE;
			case SHORT -> WRAPPED_SHORT_TYPE;
			case INT -> WRAPPED_INT_TYPE;
			case FLOAT -> WRAPPED_FLOAT_TYPE;
			case LONG -> WRAPPED_LONG_TYPE;
			case DOUBLE -> WRAPPED_DOUBLE_TYPE;
			case VOID -> WRAPPED_VOID_TYPE;
			default -> throw new IllegalArgumentException(format("%s is not primitive", type.getClassName()));
		};
	}

	public static Type unwrap(Type type) {
		if (type.getSort() != OBJECT)
			throw new IllegalArgumentException("Cannot unwrap type that is not an object reference");
		@Nullable Type reference = WRAPPER_TO_PRIMITIVE.get(type.getClassName());
		if (reference == null)
			throw new IllegalArgumentException("Not a wrapper type: " + type.getClassName());
		return reference;
	}

	public static void invokeVirtualOrInterface(Context context, Type owner, Method method) {
		GeneratorAdapter g = context.getGeneratorAdapter();
		Class<?> ownerClass = context.toJavaType(owner);
		if (!ownerClass.isInterface()) {
			g.invokeVirtual(owner, method);
			return;
		}

		if (!hasMethod(context, ownerClass, method) && hasMethod(context, Object.class, method)) {
			g.invokeVirtual(OBJECT_TYPE, method);
		} else {
			g.invokeInterface(owner, method);
		}
	}

	private static boolean hasMethod(Context context, Class<?> owner, Method method) {
		Type[] argumentTypes = method.getArgumentTypes();
		Class<?>[] parameterTypes = new Class[argumentTypes.length];
		for (int i = 0; i < argumentTypes.length; i++) {
			parameterTypes[i] = context.toJavaType(argumentTypes[i]);
		}

		try {
			owner.getMethod(method.getName(), parameterTypes);
		} catch (NoSuchMethodException e) {
			return false;
		}

		return true;
	}

	public static String exceptionInGeneratedClass(Context ctx) {
		return format("Thrown in generated class %s in method %s",
				ctx.getSelfType().getClassName(),
				ctx.getMethod()
		);
	}

	public static boolean isValidCast(Type from, Type to) {
		return from.getSort() != to.getSort() &&
				!(from.getSort() < BOOLEAN || from.getSort() > DOUBLE || to.getSort() < BOOLEAN || to.getSort() > DOUBLE);
	}

	public static String getStringSetting(Class<?> cls, String key, String defaultValue) {
		return System.getProperty(cls.getSimpleName() + '.' + key, defaultValue);
	}

	public static Path getPathSetting(Class<?> cls, String key, Path defaultValue) {
		String setting = getStringSetting(cls, key, null);
		return setting == null ? defaultValue : Paths.get(setting);
	}
}
