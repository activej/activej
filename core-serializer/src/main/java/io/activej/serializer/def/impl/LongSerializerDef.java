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
import io.activej.serializer.def.SerializerDefWithVarLength;

import static io.activej.serializer.def.SerializerExpressions.*;

@ExposedInternals
public final class LongSerializerDef extends PrimitiveSerializerDef implements SerializerDefWithVarLength {
	public final boolean varLength;

	@SuppressWarnings("unused") // used via reflection
	public LongSerializerDef() {
		this(true, false);
	}

	public LongSerializerDef(boolean wrapped, boolean varLength) {
		super(long.class, wrapped);
		this.varLength = varLength;
	}

	@Override
	public SerializerDef ensureWrapped() {
		return new LongSerializerDef(true, varLength);
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel) {
		return varLength ?
			writeVarLong(byteArray, off, value) :
			writeLong(byteArray, off, value, !compatibilityLevel.isLittleEndian());
	}

	@Override
	protected Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel) {
		return varLength ?
			readVarLong(in) :
			readLong(in, !compatibilityLevel.isLittleEndian());
	}

	@Override
	public SerializerDef ensureVarLength() {
		return new LongSerializerDef(wrapped, true);
	}

}
