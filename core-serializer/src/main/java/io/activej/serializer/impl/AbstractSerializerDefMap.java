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

import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.CompatibilityLevel.LEVEL_3;
import static io.activej.serializer.impl.SerializerExpressions.*;

public abstract class AbstractSerializerDefMap extends AbstractSerializerDef implements SerializerDefWithNullable {
	protected final SerializerDef keySerializer;
	protected final SerializerDef valueSerializer;
	protected final Class<?> encodeType;
	protected final Class<?> decodeType;
	protected final Class<?> keyType;
	protected final Class<?> valueType;
	protected final boolean nullable;

	protected AbstractSerializerDefMap(@NotNull SerializerDef keySerializer, @NotNull SerializerDef valueSerializer, @NotNull Class<?> encodeType, @NotNull Class<?> decodeType, @NotNull Class<?> keyType, @NotNull Class<?> valueType, boolean nullable) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.encodeType = encodeType;
		this.decodeType = decodeType;
		this.keyType = keyType;
		this.valueType = valueType;
		this.nullable = nullable;
	}

	protected Expression mapForEach(Expression collection, UnaryOperator<Expression> forEachKey, UnaryOperator<Expression> forEachValue){
		return forEach(collection, forEachKey, forEachValue);
	}

	protected Expression createConstructor(Expression length) {
		return constructor(decodeType, initialSize(length));
	}

	protected Expression initialSize(Expression length) {
		return div(mul(length, value(4)), value(3));
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

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression length = length(value);
		Expression writeLength = writeVarInt(buf, pos, !nullable ? length : inc(length));
		Expression forEach = mapForEach(value,
				k -> keySerializer.defineEncoder(staticEncoders, buf, pos, cast(k, keySerializer.getEncodeType()), version, compatibilityLevel),
				v -> valueSerializer.defineEncoder(staticEncoders, buf, pos, cast(v, valueSerializer.getEncodeType()), version, compatibilityLevel));

		return !nullable ?
				sequence(writeLength, forEach) :
				ifThenElse(isNull(value),
						writeByte(buf, pos, value((byte) 0)),
						sequence(writeLength, forEach));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readVarInt(in), length ->
				!nullable ?
						doDecode(staticDecoders, in, version, compatibilityLevel, length) :
						ifThenElse(
								cmpEq(length, value(0)),
								nullRef(decodeType),
								let(dec(length), len -> doDecode(staticDecoders, in, version, compatibilityLevel, len))));
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_3.getLevel()) {
			return new SerializerDefNullable(this);
		}
		return doEnsureNullable(compatibilityLevel);
	}

	protected abstract SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel);

	protected @NotNull Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Variable length) {
		return let(createConstructor(length), instance -> sequence(
				loop(value(0), length,
						i -> call(instance, "put",
								cast(keySerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), keyType),
								cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), valueType))),
				instance));
	}

}
