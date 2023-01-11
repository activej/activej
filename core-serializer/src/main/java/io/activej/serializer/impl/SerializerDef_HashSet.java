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

public final class SerializerDef_HashSet extends SerializerDef_RegularCollection {
	public SerializerDef_HashSet(SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType) {
		this(valueSerializer, encodeType, decodeType, false);
	}

	private SerializerDef_HashSet(SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType, boolean nullable) {
		super(valueSerializer, encodeType, decodeType, Object.class, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDef_HashSet(valueSerializer, encodeType, decodeType, true);
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(decodeType, hashInitialSize(length));
	}
}
