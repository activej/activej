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
import io.activej.serializer.def.AbstractCollectionSerializerDef;
import io.activej.serializer.def.SerializerDef;

import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;

@ExposedInternals
public class RegularCollectionSerializerDef extends AbstractCollectionSerializerDef {
	public RegularCollectionSerializerDef(SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType, Class<?> elementType, boolean nullable) {
		super(valueSerializer, encodeType, decodeType, elementType, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new RegularCollectionSerializerDef(valueSerializer, encodeType, decodeType, elementType, true);
	}

	@Override
	protected Expression doIterate(Expression collection, UnaryOperator<Expression> action) {
		return iterateIterable(collection, action);
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(decodeType, length);
	}

	@Override
	protected Expression addToBuilder(Expression builder, Expression index, Expression element) {
		return call(builder, "add", element);
	}

	@Override
	protected Expression build(Expression builder) {
		return builder;
	}
}
