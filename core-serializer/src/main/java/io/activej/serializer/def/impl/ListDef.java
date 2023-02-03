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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;

@ExposedInternals
public final class ListDef extends RegularCollectionDef {
	public ListDef(SerializerDef valueSerializer, boolean nullable) {
		super(valueSerializer, List.class, List.class, Object.class, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new ListDef(valueSerializer, true);
	}

	@Override
	protected Expression doIterate(Expression collection, UnaryOperator<Expression> action) {
		return let(collection, v -> iterateList(v, action));
	}

	@Override
	protected Expression doDecode(SerializerDef.StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression length) {
		SerializerDef.Decoder decoder = valueSerializer.defineDecoder(staticDecoders, version, compatibilityLevel);
		return ifEq(length, value(0),
				staticCall(Collections.class, "emptyList"),
				ifEq(length, value(1),
						staticCall(Collections.class, "singletonList", decoder.decode(in)),
						super.doDecode(staticDecoders, in, version, compatibilityLevel, length)));
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return arrayNew(Object[].class, length);
	}

	@Override
	protected Expression addToBuilder(Expression array, Expression i, Expression element) {
		return arraySet(array, i, element);
	}

	@Override
	protected Expression build(Expression array) {
		return staticCall(Arrays.class, "asList", array);
	}
}
