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

public abstract class AbstractSerializerDefCollection extends AbstractSerializerDef implements SerializerDefWithNullable {
	protected final SerializerDef valueSerializer;
	protected final Class<?> encodeType;
	protected final Class<?> decodeType;
	protected final Class<?> elementType;
	protected final boolean nullable;

	protected AbstractSerializerDefCollection(@NotNull SerializerDef valueSerializer, @NotNull Class<?> encodeType, @NotNull Class<?> decodeType, @NotNull Class<?> elementType, boolean nullable) {
		this.valueSerializer = valueSerializer;
		this.encodeType = encodeType;
		this.decodeType = decodeType;
		this.elementType = elementType;
		this.nullable = nullable;
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(valueSerializer);
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
	public final SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_3.getLevel()) {
			return new SerializerDefNullable(this);
		}
		return doEnsureNullable(compatibilityLevel);
	}

	@Override
	public final Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
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

	protected @NotNull Expression doEncode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel){
		return doIterate(value,
				it -> valueSerializer.defineEncoder(staticEncoders, buf, pos, cast(it, valueSerializer.getEncodeType()), version, compatibilityLevel));
	}

	@Override
	public final Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readVarInt(in), length ->
				!nullable ?
						doDecode(staticDecoders, in, version, compatibilityLevel, length) :
						ifEq(length, value(0),
								nullRef(decodeType),
								let(dec(length), len -> doDecode(staticDecoders, in, version, compatibilityLevel, len))));
	}

	protected @NotNull Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression length){
		return let(createBuilder(length), builder -> sequence(
				iterate(value(0), length,
						i -> addToBuilder(builder, i, cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), elementType))),
				build(builder)));
	}

	protected abstract @NotNull SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel);

	protected abstract @NotNull Expression doIterate(Expression collection, UnaryOperator<Expression> action);

	protected abstract @NotNull Expression createBuilder(Expression length);

	protected abstract @NotNull Expression addToBuilder(Expression builder, Expression index, Expression element);

	protected abstract @NotNull Expression build(Expression builder);
}
