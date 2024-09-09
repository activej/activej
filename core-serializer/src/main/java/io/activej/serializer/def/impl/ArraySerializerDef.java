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
import io.activej.serializer.def.*;
import io.activej.serializer.util.ZeroArrayUtils;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.serializer.CompatibilityLevel.LEVEL_3;
import static io.activej.serializer.def.SerializerExpressions.*;

@ExposedInternals
public final class ArraySerializerDef extends AbstractSerializerDef implements SerializerDefWithNullable, SerializerDefWithFixedSize {
	public final SerializerDef valueSerializer;
	public final int fixedSize;
	public final boolean nullable;

	private final Class<?> encodeType;
	private final Class<?> decodeType;

	public ArraySerializerDef(SerializerDef serializer, int fixedSize, boolean nullable, Class<?> encodeType, Class<?> decodeType) {
		checkArgument(encodeType.isArray(), "Trying to serialize a non-array type %s as array", encodeType);
		checkArgument(decodeType.isArray(), "Trying to deserialize a non-array type %s as array", decodeType);

		this.valueSerializer = serializer;
		this.fixedSize = fixedSize;
		this.nullable = nullable;
		this.encodeType = encodeType;
		this.decodeType = decodeType;
	}

	@Override
	public ArraySerializerDef ensureFixedSize(int fixedSize) {
		return new ArraySerializerDef(valueSerializer, fixedSize, nullable, encodeType, decodeType);
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_3.getLevel()) {
			return SerializerDefs.ofNullable(this);
		}
		return new ArraySerializerDef(valueSerializer, fixedSize, true, encodeType, decodeType);
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(valueSerializer);
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return encodeType.getComponentType() == Byte.TYPE;
	}

	@Override
	public Class<?> getEncodeType() {
		return encodeType;
	}

	@Override
	public Class<?> getDecodeType() {
		return decodeType;
	}

	@Override
	public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		if (encodeType.getComponentType() == Byte.TYPE) {
			Expression castedValue = cast(value, encodeType);
			Expression length = fixedSize != -1 ? value(fixedSize) : length(castedValue);

			if (!nullable) {
				return sequence(
					writeVarInt(buf, pos, length),
					writeBytes(buf, pos, castedValue));
			} else {
				return ifNull(value,
					writeByte(buf, pos, value((byte) 0)),
					sequence(
						writeVarInt(buf, pos, inc(length)),
						writeBytes(buf, pos, castedValue))
				);
			}
		} else {
			Expression size = fixedSize != -1 ? value(fixedSize) : length(cast(value, encodeType));

			Encoder encoder = valueSerializer.defineEncoder(staticEncoders, version, compatibilityLevel);
			Expression writeCollection = iterate(value(0), size,
				i -> encoder.encode(buf, pos, arrayGet(cast(value, encodeType), i)));

			if (!nullable) {
				return sequence(
					writeVarInt(buf, pos, size),
					writeCollection);
			} else {
				return ifNull(value,
					writeByte(buf, pos, value((byte) 0)),
					sequence(
						writeVarInt(buf, pos, inc(size)),
						writeCollection));
			}
		}
	}

	@Override
	public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		if (decodeType.getComponentType() == Byte.TYPE) {
			return let(readVarInt(in),
				len -> !nullable ?
					let(arrayNew0(len),
						array -> sequence(
							readBytes(in, array),
							array)) :
					ifEq(len, value(0),
						nullRef(decodeType),
						let(arrayNew0(dec(len)),
							array -> sequence(
								readBytes(in, array, value(0), dec(len)),
								array))));
		}

		return let(readVarInt(in),
			len -> !nullable ?
				doDecode(staticDecoders, in, version, compatibilityLevel, len) :
				ifEq(len, value(0),
					nullRef(decodeType),
					let(dec(len),
						len0 -> doDecode(staticDecoders, in, version, compatibilityLevel, len0))));
	}

	private Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression size) {
		Decoder decoder = valueSerializer.defineDecoder(staticDecoders, version, compatibilityLevel);
		return let(arrayNew0(size),
			array -> sequence(
				iterate(value(0), size,
					i -> arraySet(array, i,
						cast(decoder.decode(in), decodeType.getComponentType()))),
				array));
	}

	private Expression arrayNew0(Expression len) {
		Class<?> componentType = decodeType.getComponentType();
		if (!componentType.isPrimitive()) return arrayNew(decodeType, len);
		return ifEq(len, value(0),
			staticField(ZeroArrayUtils.class, "ZERO_ARRAY_" + componentType.getSimpleName().toUpperCase() + "S"),
			arrayNew(decodeType, len));

	}
}
