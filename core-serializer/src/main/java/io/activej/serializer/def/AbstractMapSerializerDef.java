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

package io.activej.serializer.def;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;

import java.util.function.BinaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.CompatibilityLevel.LEVEL_3;
import static io.activej.serializer.def.SerializerExpressions.*;

public abstract class AbstractMapSerializerDef extends AbstractSerializerDef implements SerializerDefWithNullable {
	protected final SerializerDef keySerializer;
	protected final SerializerDef valueSerializer;
	protected final Class<?> encodeType;
	protected final Class<?> decodeType;
	protected final Class<?> keyType;
	protected final Class<?> valueType;
	protected final boolean nullable;

	protected AbstractMapSerializerDef(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> encodeType, Class<?> decodeType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.encodeType = encodeType;
		this.decodeType = decodeType;
		this.keyType = keyType;
		this.valueType = valueType;
		this.nullable = nullable;
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit("key", keySerializer);
		visitor.visit("value", valueSerializer);
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return false;
	}

	@Override
	public Class<?> getEncodeType() {
		return encodeType;
	}

	@Override
	public Class<?> getDecodeType() {
		return decodeType;
	}

	public SerializerDef getKeySerializer() {
		return keySerializer;
	}

	public SerializerDef getValueSerializer() {
		return valueSerializer;
	}

	public Class<?> getKeyType() {
		return keyType;
	}

	public Class<?> getValueType() {
		return valueType;
	}

	public boolean isNullable() {
		return nullable;
	}

	@Override
	public final SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_3.getLevel()) {
			return SerializerDefs.ofNullable(this);
		}
		return doEnsureNullable(compatibilityLevel);
	}

	@Override
	public final Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		if (!nullable) {
			return sequence(
				writeVarInt(buf, pos, length(value)),
				doEncode(staticEncoders, buf, pos, value, version, compatibilityLevel));
		} else {
			return ifNull(value,
				writeByte(buf, pos, value((byte) 0)),
				sequence(
					writeVarInt(buf, pos, inc(length(value))),
					doEncode(staticEncoders, buf, pos, value, version, compatibilityLevel)));
		}
	}

	protected Expression doEncode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Encoder keyEncoder = keySerializer.defineEncoder(staticEncoders, version, compatibilityLevel);
		Encoder valueEncoder = valueSerializer.defineEncoder(staticEncoders, version, compatibilityLevel);
		return doIterateMap(value,
			(k, v) -> sequence(
				keyEncoder.encode(buf, pos, cast(k, keySerializer.getEncodeType())),
				valueEncoder.encode(buf, pos, cast(v, valueSerializer.getEncodeType()))));
	}

	@Override
	public final Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readVarInt(in), length ->
			!nullable ?
				doDecode(staticDecoders, in, version, compatibilityLevel, length) :
				ifEq(length, value(0),
					nullRef(decodeType),
					let(dec(length), len -> doDecode(staticDecoders, in, version, compatibilityLevel, len))));
	}

	protected Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression length) {
		Decoder keyDecoder = keySerializer.defineDecoder(staticDecoders, version, compatibilityLevel);
		Decoder valueDecoder = valueSerializer.defineDecoder(staticDecoders, version, compatibilityLevel);
		return let(createBuilder(length), builder -> sequence(
			iterate(value(0), length,
				i -> putToBuilder(builder, i,
					cast(keyDecoder.decode(in), keyType),
					cast(valueDecoder.decode(in), valueType))),
			build(builder)));
	}

	protected abstract SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel);

	protected abstract Expression doIterateMap(Expression collection, BinaryOperator<Expression> keyValueAction);

	protected abstract Expression createBuilder(Expression length);

	protected abstract Expression putToBuilder(Expression builder, Expression index, Expression key, Expression value);

	protected abstract Expression build(Expression builder);
}
