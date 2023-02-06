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

import java.util.EnumMap;

import static io.activej.codegen.expression.Expressions.constructor;
import static io.activej.codegen.expression.Expressions.value;

@ExposedInternals
public final class EnumMapSerializerDef extends RegularMapSerializerDef {
	public EnumMapSerializerDef(SerializerDef keySerializer, SerializerDef valueSerializer, boolean nullable) {
		super(keySerializer, valueSerializer, EnumMap.class, EnumMap.class, Enum.class, Object.class, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new EnumMapSerializerDef(keySerializer, valueSerializer, true);
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(EnumMap.class, value(keySerializer.getDecodeType()));
	}

}
