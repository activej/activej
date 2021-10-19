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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.util.Utils.hashInitialSize;

public final class SerializerDefSet extends SerializerDefRegularCollection {
	public SerializerDefSet(SerializerDef valueSerializer) {
		this(valueSerializer, false);
	}

	private SerializerDefSet(SerializerDef valueSerializer, boolean nullable) {
		super(valueSerializer, Set.class, Set.class, Object.class, nullable);
	}

	@Override
	protected @NotNull SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefSet(valueSerializer, true);
	}

	@Override
	protected @NotNull Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression length) {
		return ifThenElse(cmpEq(length, value(0)),
				staticCall(Collections.class, "emptySet"),
				ifThenElse(cmpEq(length, value(1)),
						staticCall(Collections.class, "singleton", valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel)),
						super.doDecode(staticDecoders, in, version, compatibilityLevel, length)));
	}

	@Override
	protected @NotNull Expression createBuilder(Expression length) {
		if (valueSerializer.getDecodeType().isEnum()) {
			return staticCall(EnumSet.class, "noneOf", value(valueSerializer.getEncodeType()));
		}
		return constructor(HashSet.class, hashInitialSize(length));
	}
}
