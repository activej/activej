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
import org.jetbrains.annotations.NotNull;
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

	public static boolean isPrimitiveType(Type type) {
		int sort = type.getSort();
		return sort == BOOLEAN ||
				sort == CHAR ||
				sort == BYTE ||
				sort == SHORT ||
				sort == INT ||
				sort == FLOAT ||
				sort == LONG ||
				sort == DOUBLE ||
				sort == VOID;
	}

	public static boolean isWrapperType(Type type) {
		return type.getSort() == OBJECT &&
				WRAPPER_TO_PRIMITIVE.containsKey(type.getClassName());
	}

	public static Method toPrimitive(Type type) {
		if (type.getSort() == BOOLEAN || type.equals(WRAPPED_BOOLEAN_TYPE))
			return getMethod("boolean booleanValue()");
		if (type.getSort() == CHAR || type.equals(WRAPPED_CHAR_TYPE))
			return getMethod("char charValue()");
		if (type.getSort() == BYTE || type.equals(WRAPPED_BYTE_TYPE))
			return getMethod("byte byteValue()");
		if (type.getSort() == SHORT || type.equals(WRAPPED_SHORT_TYPE))
			return getMethod("short shortValue()");
		if (type.getSort() == INT || type.equals(WRAPPED_INT_TYPE))
			return getMethod("int intValue()");
		if (type.getSort() == FLOAT || type.equals(WRAPPED_FLOAT_TYPE))
			return getMethod("float floatValue()");
		if (type.getSort() == LONG || type.equals(WRAPPED_LONG_TYPE))
			return getMethod("long longValue()");
		if (type.getSort() == DOUBLE || type.equals(WRAPPED_DOUBLE_TYPE))
			return getMethod("double doubleValue()");

		throw new IllegalArgumentException(format("No primitive value method for %s ", type.getClassName()));
	}

	public static Type wrap(Type type) {
		int sort = type.getSort();
		if (sort == BOOLEAN)
			return WRAPPED_BOOLEAN_TYPE;
		if (sort == CHAR)
			return WRAPPED_CHAR_TYPE;
		if (sort == BYTE)
			return WRAPPED_BYTE_TYPE;
		if (sort == SHORT)
			return WRAPPED_SHORT_TYPE;
		if (sort == INT)
			return WRAPPED_INT_TYPE;
		if (sort == FLOAT)
			return WRAPPED_FLOAT_TYPE;
		if (sort == LONG)
			return WRAPPED_LONG_TYPE;
		if (sort == DOUBLE)
			return WRAPPED_DOUBLE_TYPE;
		if (sort == VOID)
			return WRAPPED_VOID_TYPE;

		throw new IllegalArgumentException(format("%s is not primitive", type.getClassName()));
	}

	public static Type unwrap(@NotNull Type type) {
		if (type.getSort() != OBJECT)
			throw new IllegalArgumentException("Cannot unwrap type that is not an object reference");
		@Nullable Type reference = WRAPPER_TO_PRIMITIVE.get(type.getClassName());
		if (reference == null)
			throw new NullPointerException();
		return reference;
	}

	public static void invokeVirtualOrInterface(GeneratorAdapter g, Class<?> owner, Method method) {
		if (owner.isInterface())
			g.invokeInterface(getType(owner), method);
		else
			g.invokeVirtual(getType(owner), method);
	}

	public static String exceptionInGeneratedClass(Context ctx) {
		return format("Thrown in generated class %s in method %s",
				ctx.getSelfType().getClassName(),
				ctx.getMethod()
		);
	}

	public static boolean isValidCast(Type from, Type to) {
		return from.getSort() != to.getSort()
				&&
				!(from.getSort() < BOOLEAN
						|| from.getSort() > DOUBLE
						|| to.getSort() < BOOLEAN
						|| to.getSort() > DOUBLE);
	}

	public static String getStringSetting(Class<?> cls, String key, String defaultValue) {
		return System.getProperty(cls.getSimpleName() + '.' + key, defaultValue);
	}

	public static Path getPathSetting(Class<?> cls, String key, Path defaultValue) {
		String setting = getStringSetting(cls, key, null);
		return setting == null ? defaultValue : Paths.get(setting);
	}
}
