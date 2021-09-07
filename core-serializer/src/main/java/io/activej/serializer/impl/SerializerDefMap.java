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
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static io.activej.codegen.expression.Expressions.*;
import static org.objectweb.asm.Type.getType;

public final class SerializerDefMap extends AbstractSerializerDefMap {
	public SerializerDefMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		this(keySerializer, valueSerializer, Map.class,
				keySerializer.getDecodeType().isEnum() ?
						EnumMap.class :
						LinkedHashMap.class,
				false);
	}

	public SerializerDefMap(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType) {
		super(keySerializer, valueSerializer, encodeType, decodeType, Object.class, Object.class, false);
	}

	private SerializerDefMap(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType, boolean nullable) {
		super(keySerializer, valueSerializer, encodeType, decodeType, Object.class, Object.class, nullable);
	}

	@Override
	protected Expression createConstructor(Expression length) {
		Class<?> rawType = keySerializer.getDecodeType();
		if (rawType.isEnum()) {
			return constructor(EnumMap.class, cast(value(getType(rawType)), Class.class));
		}
		return super.createConstructor(length);
	}

	@Override
	public Expression mapForEach(Expression collection, Function<Expression, Expression> forEachKey, Function<Expression, Expression> forEachValue) {
		return forEach(collection, forEachKey, forEachValue);
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.compareTo(CompatibilityLevel.LEVEL_3) < 0) {
			return new SerializerDefNullable(this);
		}
		return new SerializerDefMap(keySerializer, valueSerializer, encodeType, decodeType, true);
	}
}
