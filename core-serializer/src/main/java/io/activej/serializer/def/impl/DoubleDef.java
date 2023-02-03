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
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.def.PrimitiveSerializerDef;
import io.activej.serializer.def.SerializerDef;

import static io.activej.serializer.def.SerializerExpressions.readDouble;
import static io.activej.serializer.def.SerializerExpressions.writeDouble;

@ExposedInternals
public final class DoubleDef extends PrimitiveSerializerDef {

	@SuppressWarnings("unused") // used via reflection
	public DoubleDef() {
		this(true);
	}

	public DoubleDef(boolean wrapped) {
		super(double.class, wrapped);
	}

	@Override
	public SerializerDef ensureWrapped() {
		return new DoubleDef(true);
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel) {
		return writeDouble(byteArray, off, value, !compatibilityLevel.isLittleEndian());
	}

	@Override
	protected Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel) {
		return readDouble(in, !compatibilityLevel.isLittleEndian());
	}
}

