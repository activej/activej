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
import io.activej.serializer.def.AbstractSerializerDef;
import io.activej.serializer.def.SerializerDef;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.def.SerializerExpressions.readByte;
import static io.activej.serializer.def.SerializerExpressions.writeByte;

@ExposedInternals
public final class NullableSerializerDef extends AbstractSerializerDef implements SerializerDef {
	public final SerializerDef serializer;

	public NullableSerializerDef(SerializerDef serializer) {
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
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return serializer.isInline(version, compatibilityLevel);
	}

	@Override
	public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Encoder encoder = serializer.defineEncoder(staticEncoders, version, compatibilityLevel);
		return ifNonNull(value,
				sequence(
						writeByte(buf, pos, value((byte) 1)),
						encoder.encode(buf, pos, value)),
				writeByte(buf, pos, value((byte) 0))
		);
	}

	@Override
	public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		Decoder decoder = serializer.defineDecoder(staticDecoders, version, compatibilityLevel);
		return let(readByte(in),
				b -> ifNe(b, value((byte) 0),
						decoder.decode(in),
						nullRef(serializer.getDecodeType())));
	}
}
