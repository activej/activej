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

import static io.activej.serializer.def.SerializerExpressions.readChar;
import static io.activej.serializer.def.SerializerExpressions.writeChar;

@ExposedInternals
public final class CharSerializer extends PrimitiveSerializerDef {

	@SuppressWarnings("unused") // used via reflection
	public CharSerializer() {
		this(true);
	}

	public CharSerializer(boolean wrapped) {
		super(char.class, wrapped);
	}

	@Override
	public SerializerDef ensureWrapped() {
		return new CharSerializer(true);
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel) {
		return writeChar(byteArray, off, value, !compatibilityLevel.isLittleEndian());
	}

	@Override
	protected Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel) {
		return readChar(in, !compatibilityLevel.isLittleEndian());
	}
}
