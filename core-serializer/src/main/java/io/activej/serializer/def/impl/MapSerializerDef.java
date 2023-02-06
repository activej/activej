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

package io.activej.serializer.def.impl;

import io.activej.codegen.expression.Expression;
import io.activej.common.annotation.ExposedInternals;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.def.SerializerDef;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.util.Utils.hashInitialSize;

@ExposedInternals
public final class MapSerializerDef extends RegularMapSerializerDef {
	public MapSerializerDef(SerializerDef keySerializer, SerializerDef valueSerializer, boolean nullable) {
		super(keySerializer, valueSerializer, Map.class, Map.class, Object.class, Object.class, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new MapSerializerDef(keySerializer, valueSerializer, true);
	}

	@Override
	protected Expression doDecode(SerializerDef.StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression length) {
		SerializerDef.Decoder keyDecoder = keySerializer.defineDecoder(staticDecoders, version, compatibilityLevel);
		SerializerDef.Decoder valueDecoder = valueSerializer.defineDecoder(staticDecoders, version, compatibilityLevel);
		return ifEq(length, value(0),
				staticCall(Collections.class, "emptyMap"),
				ifEq(length, value(1),
						staticCall(Collections.class, "singletonMap",
								keyDecoder.decode(in),
								valueDecoder.decode(in)),
						super.doDecode(staticDecoders, in, version, compatibilityLevel, length)));
	}

	@Override
	protected Expression createBuilder(Expression length) {
		Class<?> rawType = keySerializer.getDecodeType();
		if (rawType.isEnum()) {
			return constructor(EnumMap.class, value(rawType));
		}
		return constructor(HashMap.class, hashInitialSize(length));
	}
}
