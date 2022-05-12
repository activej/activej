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
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.util.Utils.hashInitialSize;

public final class SerializerDefMap extends SerializerDefRegularMap {
	public SerializerDefMap(SerializerDef keySerializer, SerializerDef valueSerializer) {
		this(keySerializer, valueSerializer, false);
	}

	private SerializerDefMap(SerializerDef keySerializer, SerializerDef valueSerializer, boolean nullable) {
		super(keySerializer, valueSerializer, Map.class, Map.class, Object.class, Object.class, nullable);
	}

	@Override
	protected @NotNull SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefMap(keySerializer, valueSerializer, true);
	}

	@Override
	protected @NotNull Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression length) {
		return ifEq(length, value(0),
				staticCall(Collections.class, "emptyMap"),
				ifEq(length, value(1),
						staticCall(Collections.class, "singletonMap",
								keySerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel),
								valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel)),
						super.doDecode(staticDecoders, in, version, compatibilityLevel, length)));
	}

	@Override
	protected @NotNull Expression createBuilder(Expression length) {
		Class<?> rawType = keySerializer.getDecodeType();
		if (rawType.isEnum()) {
			return constructor(EnumMap.class, value(rawType));
		}
		return constructor(HashMap.class, hashInitialSize(length));
	}
}
