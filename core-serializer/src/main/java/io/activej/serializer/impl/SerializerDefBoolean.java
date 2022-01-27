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
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.CompatibilityLevel.LEVEL_4;
import static io.activej.serializer.impl.SerializerExpressions.readByte;
import static io.activej.serializer.impl.SerializerExpressions.writeByte;

public final class SerializerDefBoolean extends SerializerDefPrimitive implements SerializerDefWithNullable {
	public static final byte NULLABLE_NULL = 0b00;
	public static final byte NULLABLE_FALSE = 0b10;
	public static final byte NULLABLE_TRUE = 0b11;

	private final boolean nullable;

	public SerializerDefBoolean() {
		this(true);
	}

	public SerializerDefBoolean(boolean wrapped) {
		this(wrapped, false);
	}

	public SerializerDefBoolean(boolean wrapped, boolean nullable) {
		super(boolean.class, wrapped);
		if (nullable && !wrapped) throw new IllegalArgumentException("Primitive cannot be nullable");
		this.nullable = nullable;
	}

	@Override
	public SerializerDef ensureWrapped() {
		return new SerializerDefBoolean(true, nullable);
	}

	@Override
	protected boolean castToPrimitive() {
		return !nullable;
	}

	@Override
	protected Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel) {
		return !nullable ?
				writeByte(byteArray, off, value) :
				ifNull(value,
						writeByte(byteArray, off, value(NULLABLE_NULL)),
						writeByte(byteArray, off, bitOr(cast(call(value, "booleanValue"), byte.class), value(0b10))));
	}

	@Override
	protected Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel) {
		return let(readByte(in), aByte ->
				!nullable ?
						cast(aByte, boolean.class) :
						ifEq(aByte, value(NULLABLE_NULL),
								nullRef(Boolean.class),
								cast(bitAnd(aByte, value(1)), Boolean.class)
						));
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_4.getLevel()) {
			return new SerializerDefNullable(this);
		}
		return new SerializerDefBoolean(wrapped, true);
	}
}
