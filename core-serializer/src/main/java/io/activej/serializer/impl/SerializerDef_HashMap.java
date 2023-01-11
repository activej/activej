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

import static io.activej.codegen.expression.Expressions.constructor;
import static io.activej.serializer.util.Utils.hashInitialSize;

public final class SerializerDef_HashMap extends SerializerDef_RegularMap {
	public SerializerDef_HashMap(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType) {
		super(keySerializer, valueSerializer, encodeType, decodeType, Object.class, Object.class, false);
	}

	private SerializerDef_HashMap(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType, boolean nullable) {
		super(keySerializer, valueSerializer, encodeType, decodeType, Object.class, Object.class, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDef_HashMap(keySerializer, valueSerializer, encodeType, decodeType, true);
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(decodeType, hashInitialSize(length));
	}
}
