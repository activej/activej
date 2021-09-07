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
import static io.activej.serializer.impl.SerializerExpressions.readByte;
import static io.activej.serializer.impl.SerializerExpressions.writeByte;

public final class SerializerDefNullable extends AbstractSerializerDef implements SerializerDef {
	private final SerializerDef serializer;

	public SerializerDefNullable(@NotNull SerializerDef serializer) {
		this.serializer = serializer;
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(serializer);
	}

	@Override
	public Class<?> getEncodeType() {
		return serializer.getEncodeType();
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return ifThenElse(isNotNull(value),
				sequence(
						writeByte(buf, pos, value((byte) 1)),
						serializer.defineEncoder(staticEncoders, buf, pos, value, version, compatibilityLevel)),
				writeByte(buf, pos, value((byte) 0))
		);
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readByte(in),
				isNotNull -> ifThenElse(cmpNe(isNotNull, value((byte) 0)),
						serializer.defineDecoder(staticDecoders, in, version, compatibilityLevel),
						nullRef(serializer.getDecodeType())));
	}
}
