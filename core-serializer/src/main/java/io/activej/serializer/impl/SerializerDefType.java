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
import io.activej.serializer.AbstractSerializerDef;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.util.BinaryOutputUtils;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.impl.SerializerExpressions.readByte;
import static io.activej.serializer.impl.SerializerExpressions.writeByte;

public final class SerializerDefType extends AbstractSerializerDef {
	public static final Map<Byte, Class<?>> INDEX_TO_TYPE;
	public static final Map<Class<?>, Byte> TYPE_TO_INDEX;

	static {
		Map<Byte, Class<?>> indexToClass = new HashMap<>();
		Map<Class<?>, Byte> classToIndex = new HashMap<>();

		register(indexToClass, classToIndex, (byte) 1, void.class);
		register(indexToClass, classToIndex, (byte) 2, boolean.class);
		register(indexToClass, classToIndex, (byte) 3, byte.class);
		register(indexToClass, classToIndex, (byte) 4, short.class);
		register(indexToClass, classToIndex, (byte) 5, int.class);
		register(indexToClass, classToIndex, (byte) 6, long.class);
		register(indexToClass, classToIndex, (byte) 7, float.class);
		register(indexToClass, classToIndex, (byte) 8, double.class);
		register(indexToClass, classToIndex, (byte) 9, char.class);
		register(indexToClass, classToIndex, (byte) 10, Void.class);
		register(indexToClass, classToIndex, (byte) 11, Boolean.class);
		register(indexToClass, classToIndex, (byte) 12, Byte.class);
		register(indexToClass, classToIndex, (byte) 13, Short.class);
		register(indexToClass, classToIndex, (byte) 14, Integer.class);
		register(indexToClass, classToIndex, (byte) 15, Long.class);
		register(indexToClass, classToIndex, (byte) 16, Float.class);
		register(indexToClass, classToIndex, (byte) 17, Double.class);
		register(indexToClass, classToIndex, (byte) 18, Character.class);
		register(indexToClass, classToIndex, (byte) 19, String.class);
		register(indexToClass, classToIndex, (byte) 20, Object.class);

		INDEX_TO_TYPE = Collections.unmodifiableMap(indexToClass);
		TYPE_TO_INDEX = Collections.unmodifiableMap(classToIndex);
	}

	private static void register(Map<Byte, Class<?>> idxToClass, Map<Class<?>, Byte> classToIdx, byte idx, Class<?> cls) {
		idxToClass.put(idx, cls);
		classToIdx.put(cls, idx);
	}

	@Override
	public Class<?> getEncodeType() {
		return Type.class;
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return let(call(staticField(SerializerDefType.class, "TYPE_TO_INDEX"), "get", value),
				idx -> ifNull(idx,
						sequence(writeByte(buf, pos, value(0)), set(pos, staticCall(BinaryOutputUtils.class, "writeUTF8", buf, pos, call(cast(value, Class.class), "getName")))),
						writeByte(buf, pos, cast(idx, byte.class))));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(cast(readByte(in), int.class),
				idx -> ifEq(idx, value(0),
						staticCall(Class.class, "forName", call(in, "readUTF8")),
						call(staticField(SerializerDefType.class, "INDEX_TO_TYPE"), "get", cast(idx, Byte.class))));
	}
}
