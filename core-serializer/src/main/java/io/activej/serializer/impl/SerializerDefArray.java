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
import io.activej.serializer.AbstractSerializerDef;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import org.jetbrains.annotations.NotNull;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.CompatibilityLevel.LEVEL_3;
import static io.activej.serializer.impl.SerializerExpressions.*;

public final class SerializerDefArray extends AbstractSerializerDef implements SerializerDefWithNullable, SerializerDefWithFixedSize {
	private final SerializerDef valueSerializer;
	private final int fixedSize;
	private final Class<?> type;
	private final boolean nullable;

	public SerializerDefArray(SerializerDef serializer, Class<?> type) {
		this.valueSerializer = serializer;
		this.fixedSize = -1;
		this.type = type;
		this.nullable = false;
	}

	private SerializerDefArray(@NotNull SerializerDef serializer, int fixedSize, Class<?> type, boolean nullable) {
		this.valueSerializer = serializer;
		this.fixedSize = fixedSize;
		this.type = type;
		this.nullable = nullable;
	}

	@Override
	public SerializerDefArray ensureFixedSize(int fixedSize) {
		return new SerializerDefArray(valueSerializer, fixedSize, type, nullable);
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_3.getLevel()) {
			return new SerializerDefNullable(this);
		}
		return new SerializerDefArray(valueSerializer, fixedSize, type, true);
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(valueSerializer);
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return type.getComponentType() == Byte.TYPE;
	}

	@Override
	public Class<?> getEncodeType() {
		return Object.class;
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		if (type.getComponentType() == Byte.TYPE) {
			Expression castedValue = cast(value, type);
			Expression length = fixedSize != -1 ? value(fixedSize) : length(castedValue);

			if (!nullable) {
				return sequence(
						writeVarInt(buf, pos, length),
						writeBytes(buf, pos, castedValue));
			} else {
				return ifThenElse(isNull(value),
						writeByte(buf, pos, value((byte) 0)),
						sequence(
								writeVarInt(buf, pos, inc(length)),
								writeBytes(buf, pos, castedValue))
				);
			}
		} else {
			Expression methodLength = fixedSize != -1 ? value(fixedSize) : length(cast(value, type));

			Expression writeCollection = loop(value(0), methodLength,
					i -> valueSerializer.defineEncoder(staticEncoders, buf, pos, arrayGet(cast(value, type), i), version, compatibilityLevel));

			if (!nullable) {
				return sequence(
						writeVarInt(buf, pos, methodLength),
						writeCollection);
			} else {
				return ifThenElse(isNull(value),
						writeByte(buf, pos, value((byte) 0)),
						sequence(
								writeVarInt(buf, pos, inc(methodLength)),
								writeCollection));
			}
		}
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		if (type.getComponentType() == Byte.TYPE) {
			return let(readVarInt(in),
					len -> !nullable ?
							let(arrayNew(type, len),
									array -> sequence(
											readBytes(in, array),
											array)) :
							ifThenElse(cmpEq(len, value(0)),
									nullRef(type),
									let(arrayNew(type, dec(len)),
											array -> sequence(
													readBytes(in, array, value(0), dec(len)),
													array))));
		}

		return let(readVarInt(in),
				len -> !nullable ?
						doDecode(staticDecoders, in, version, compatibilityLevel, len) :
						ifThenElse(cmpEq(len, value(0)),
								nullRef(type),
								let(dec(len),
										len0 -> doDecode(staticDecoders, in, version, compatibilityLevel, len0))));
	}

	private Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression size) {
		return let(arrayNew(type, size),
				array -> sequence(
						loop(value(0), size,
								i -> arraySet(array, i,
										cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), type.getComponentType()))),
						array));
	}

}
