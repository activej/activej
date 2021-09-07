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

import java.util.Arrays;
import java.util.List;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.impl.SerializerExpressions.*;

public final class SerializerDefList extends AbstractSerializerDef implements SerializerDefWithNullable {

	private final SerializerDef valueSerializer;
	private final Class<?> encodeType;
	private final Class<?> decodeType;
	private final Class<?> elementType;
	private final boolean nullable;

	public SerializerDefList(SerializerDef valueSerializer) {
		this(valueSerializer, false);
	}

	private SerializerDefList(SerializerDef valueSerializer, boolean nullable) {
		this.valueSerializer = valueSerializer;
		this.encodeType = List.class;
		this.decodeType = List.class;
		this.elementType = Object.class;
		this.nullable = nullable;
	}

	@Override
	public SerializerDef ensureNullable() {
		return new SerializerDefList(valueSerializer, true);
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
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression list, int version, CompatibilityLevel compatibilityLevel) {
		if (nullable && compatibilityLevel.compareTo(CompatibilityLevel.LEVEL_3) < 0) {
			SerializerDefList serializer = new SerializerDefList(valueSerializer, false);
			return SerializerDefNullable.encode(serializer, staticEncoders, buf, pos, list, version, compatibilityLevel);
		}
		if (!nullable) {
			return let(call(list, "size"),
					len -> sequence(
							writeVarInt(buf, pos, len),
							doEncode(staticEncoders, buf, pos, list, version, compatibilityLevel, len)));
		} else {
			return ifThenElse(isNull(list),
					writeByte(buf, pos, value((byte) 0)),
					let(call(list, "size"),
							len -> sequence(
									writeVarInt(buf, pos, inc(len)),
									doEncode(staticEncoders, buf, pos, list, version, compatibilityLevel, len))));
		}
	}

	@NotNull
	private Expression doEncode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel, Expression len) {
		return loop(value(0), len,
				i -> let(call(value, "get", i),
						item -> valueSerializer.defineEncoder(staticEncoders, buf, pos, cast(item, valueSerializer.getEncodeType()), version, compatibilityLevel)));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		if (nullable && compatibilityLevel.compareTo(CompatibilityLevel.LEVEL_3) < 0) {
			SerializerDefList serializer = new SerializerDefList(valueSerializer, false);
			return SerializerDefNullable.decode(serializer, staticDecoders, in, version, compatibilityLevel);
		}
		return let(readVarInt(in),
				len -> !nullable ?
						doDecode(staticDecoders, in, version, compatibilityLevel, len) :
						ifThenElse(cmpEq(len, value(0)),
								nullRef(decodeType),
								let(dec(len),
										len0 -> doDecode(staticDecoders, in, version, compatibilityLevel, len0))));
	}

	private Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression len) {
		return let(arrayNew(Object[].class, len),
				array -> sequence(
						loop(value(0), len,
								i -> arraySet(array, i,
										cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), elementType))),
						staticCall(Arrays.class, "asList", array)));
	}

}
