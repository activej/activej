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

package io.activej.serializer.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.util.BinaryOutputUtils;
import org.jetbrains.annotations.NotNull;

import static io.activej.codegen.expression.Expressions.*;

/**
 * Provides methods for writing primitives
 * and Strings to byte arrays
 */
public final class SerializerExpressions {
	private static final Class<?> JDK_UNSAFE;
	private static final int BYTE_ARRAY_BASE_OFFSET;
	private static final boolean BIG_ENDIAN;

	static {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		Class<?> jdkUnsafe;
		int byteArrayBaseOffset = 0;
		boolean bigEndian = false;
		try {
			jdkUnsafe = classLoader.loadClass("jdk.internal.misc.Unsafe");
			Object unsafe = jdkUnsafe.getMethod("getUnsafe").invoke(null);
			byteArrayBaseOffset = (int) jdkUnsafe.getMethod("arrayBaseOffset", Class.class).invoke(unsafe, byte[].class);
			int byteArrayIndexScale = (int) jdkUnsafe.getMethod("arrayIndexScale", Class.class).invoke(unsafe, byte[].class);
			bigEndian = (boolean) jdkUnsafe.getMethod("isBigEndian").invoke(unsafe);
			if (byteArrayIndexScale != 1) jdkUnsafe = null;
		} catch (Exception e) {
			jdkUnsafe = null;
		}
		JDK_UNSAFE = jdkUnsafe;
		BYTE_ARRAY_BASE_OFFSET = byteArrayBaseOffset;
		BIG_ENDIAN = bigEndian;
	}

	private static @NotNull Expression getUnsafe() {
		return staticCall(JDK_UNSAFE, "getUnsafe");
	}

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes) {
		return writeBytes(buf, pos, bytes, value(0), length(bytes));
	}

	public static Expression writeBytes(Expression buf, Variable pos, Expression bytes, Expression bytesOff, Expression bytesLen) {
		return sequence(
				staticCall(System.class, "arraycopy", bytes, bytesOff, buf, pos, bytesLen),
				set(pos, add(pos, bytesLen)));
	}

	public static Expression writeByte(Expression buf, Variable pos, Expression value) {
		return sequence(
				arraySet(buf, pos, value),
				set(pos, add(pos, value(1))));
	}

	public static Expression writeBoolean(Expression buf, Variable pos, Expression value) {
		return writeByte(buf, pos, value);
	}

	public static Expression writeShort(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				putUnaligned(buf, pos, value, "putShortUnaligned", Short.class, 2, bigEndian) :
				set(pos, staticCall(BinaryOutputUtils.class, "writeShort" + (bigEndian ? "" : "LE"), buf, pos, cast(value, short.class)));
	}

	public static Expression writeChar(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				putUnaligned(buf, pos, value, "putCharUnaligned", Character.class, 2, bigEndian) :
				set(pos, staticCall(BinaryOutputUtils.class, "writeChar" + (bigEndian ? "" : "LE"), buf, pos, cast(value, char.class)));
	}

	public static Expression writeInt(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				putUnaligned(buf, pos, value, "putIntUnaligned", Integer.class, 4, bigEndian) :
				set(pos, staticCall(BinaryOutputUtils.class, "writeInt" + (bigEndian ? "" : "LE"), buf, pos, cast(value, int.class)));
	}

	public static Expression writeLong(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				putUnaligned(buf, pos, value, "putLongUnaligned", Long.class, 8, bigEndian) :
				set(pos, staticCall(BinaryOutputUtils.class, "writeLong" + (bigEndian ? "" : "LE"), buf, pos, cast(value, long.class)));
	}

	private static Expression putUnaligned(Expression buf, Variable pos, Expression value, String name, Class<?> numericType, int size, boolean bigEndian) {
		return ensureRemaining(buf, pos, size, sequence(
				call(getUnsafe(), name,
						buf, cast(add(value(BYTE_ARRAY_BASE_OFFSET), pos), long.class), convEndian(value, numericType, bigEndian)),
				set(pos, add(pos, value(size)))
		));
	}

	public static Expression writeVarInt(Expression buf, Variable pos, Expression value) {
		return set(pos,
				staticCall(BinaryOutputUtils.class, "writeVarInt", buf, pos, cast(value, int.class)));
	}

	public static Expression writeVarLong(Expression buf, Variable pos, Expression value) {
		return set(pos,
				staticCall(BinaryOutputUtils.class, "writeVarLong", buf, pos, cast(value, long.class)));
	}

	public static Expression writeFloat(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeInt(buf, pos, staticCall(Float.class, "floatToIntBits", cast(value, float.class)), bigEndian);
	}

	public static Expression writeDouble(Expression buf, Variable pos, Expression value, boolean bigEndian) {
		return writeLong(buf, pos, staticCall(Double.class, "doubleToLongBits", cast(value, double.class)), bigEndian);
	}

	public static Expression ensureRemaining(Expression buf, Variable pos, int size, Expression next) {
		return ensureRemaining(buf, pos, value(size), next);
	}

	public static Expression ensureRemaining(Expression buf, Variable pos, Expression size, Expression next) {
		return ifThenElse(cmpGt(add(pos, size), length(buf)),
				throwException(ArrayIndexOutOfBoundsException.class),
				next);
	}

	public static Expression array(Expression in) {
		return call(in, "array");
	}

	public static Expression pos(Expression in) {
		return call(in, "pos");
	}

	public static Expression pos(Expression in, Expression newPos) {
		return call(in, "pos", newPos);
	}

	public static Expression move(Expression in, Expression bytes) {
		return call(in, "move", bytes);
	}

	private static Expression move(Expression in, int bytes) {
		return call(in, "move", value(bytes));
	}

	public static Expression readBytes(Expression in, Expression buf) {
		return call(in, "read", buf);
	}

	public static Expression readBytes(Expression in, Expression buf, Expression off, Expression len) {
		return call(in, "read", buf, off, len);
	}

	public static Expression readByte(Expression in) {
		return call(in, "readByte");
	}

	public static Expression readBoolean(Expression in) {
		return call(in, "readBoolean");
	}

	public static Expression readShort(Expression in, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				getUnaligned(in, "getShortUnaligned", Short.class, 2, bigEndian) :
				call(in, "readShort" + (bigEndian ? "" : "LE"));
	}

	public static Expression readChar(Expression in, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				getUnaligned(in, "getCharUnaligned", Character.class, 2, bigEndian) :
				call(in, "readChar" + (bigEndian ? "" : "LE"));
	}

	public static Expression readInt(Expression in, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				getUnaligned(in, "getIntUnaligned", Integer.class, 4, bigEndian) :
				call(in, "readInt" + (bigEndian ? "" : "LE"));
	}

	public static Expression readLong(Expression in, boolean bigEndian) {
		return JDK_UNSAFE != null ?
				getUnaligned(in, "getLongUnaligned", Long.class, 8, bigEndian) :
				call(in, "readLong" + (bigEndian ? "" : "LE"));
	}

	private static Expression getUnaligned(Expression in, String name, Class<?> numericType, int size, boolean bigEndian) {
		return let(
				convEndian(
						call(getUnsafe(), name,
								array(in), cast(add(value(BYTE_ARRAY_BASE_OFFSET), pos(in)), long.class)),
						numericType,
						bigEndian),
				result -> sequence(
						move(in, size),
						result)
		);
	}

	private static Expression convEndian(Expression numericValue, Class<?> numericType, boolean bigEndian) {
		return BIG_ENDIAN == bigEndian ?
				numericValue :
				staticCall(numericType, "reverseBytes", numericValue);
	}

	public static Expression readVarInt(Expression in) {
		return call(in, "readVarInt");
	}

	public static Expression readVarLong(Expression in) {
		return call(in, "readVarLong");
	}

	public static Expression readFloat(Expression in, boolean bigEndian) {
		return staticCall(Float.class, "intBitsToFloat", readInt(in, bigEndian));
	}

	public static Expression readDouble(Expression in, boolean bigEndian) {
		return staticCall(Double.class, "longBitsToDouble", readLong(in, bigEndian));
	}

}
