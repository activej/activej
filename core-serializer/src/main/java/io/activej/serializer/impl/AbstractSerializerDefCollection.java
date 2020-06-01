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

import io.activej.codegen.Expression;
import io.activej.codegen.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.function.Function;

import static io.activej.codegen.Expressions.*;
import static io.activej.serializer.SerializerDef.StaticDecoders.IN;
import static io.activej.serializer.SerializerDef.StaticEncoders.*;
import static io.activej.serializer.impl.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public abstract class AbstractSerializerDefCollection implements SerializerDefWithNullable {
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

	protected Expression collectionForEach(Expression collection, Class<?> valueType, Function<Expression, Expression> value) {
		return forEach(collection, valueType, value);
	}

	protected Expression createConstructor(Expression length) {
		return constructor(decodeType, !nullable ? length : dec(length));
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(valueSerializer);
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
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
	public final Expression defineEncoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return staticEncoders.define(encodeType, buf, pos, value,
				encoder(staticEncoders, BUF, POS, VALUE, version, compatibilityLevel));
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression forEach = collectionForEach(value, valueSerializer.getEncodeType(),
				it -> valueSerializer.defineEncoder(staticEncoders, buf, pos, cast(it, valueSerializer.getEncodeType()), version, compatibilityLevel));

		if (!nullable) {
			return sequence(
					writeVarInt(buf, pos, call(value, "size")),
					forEach);
		} else {
			return ifThenElse(isNull(value),
					writeByte(buf, pos, value((byte) 0)),
					sequence(
							writeVarInt(buf, pos, inc(call(value, "size"))),
							forEach));
		}
	}

	@Override
	public final Expression defineDecoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return staticDecoders.define(getDecodeType(), in,
				decoder(staticDecoders, IN, version, compatibilityLevel));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readVarInt(in), length ->
				!nullable ?
						let(createConstructor(length), instance -> sequence(
								loop(value(0), length,
										it -> sequence(
												call(instance, "add",
														cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), elementType)),
												voidExp())),
								instance)) :
						ifThenElse(cmpEq(length, value(0)),
								nullRef(decodeType),
								let(createConstructor(length), instance -> sequence(
										loop(value(0), dec(length),
												it -> sequence(
														call(instance, "add",
																cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), elementType)),
														voidExp())),
										instance))));
	}
}
