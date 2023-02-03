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
import io.activej.serializer.def.SerializerDefWithNullable;
import io.activej.serializer.def.SerializerDefs;

import java.nio.ByteBuffer;
import java.util.Set;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.CompatibilityLevel.LEVEL_3;
import static io.activej.serializer.def.SerializerExpressions.*;

@ExposedInternals
public final class ByteBufferDef extends AbstractSerializerDef implements SerializerDefWithNullable {
	public final boolean wrapped;
	public final boolean nullable;

	@SuppressWarnings("unused") // used via reflection
	public ByteBufferDef() {
		this(false, false);
	}

	public ByteBufferDef(boolean wrapped, boolean nullable) {
		this.wrapped = wrapped;
		this.nullable = nullable;
	}

	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return Set.of();
	}

	@Override
	public Class<?> getEncodeType() {
		return ByteBuffer.class;
	}

	@Override
	public Expression encode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return let(
				cast(value, ByteBuffer.class),
				buffer -> {
					if (!nullable) {
						return let(call(buffer, "remaining"), remaining ->
								sequence(
										writeVarInt(buf, pos, remaining),
										writeBytes(buf, pos, call(buffer, "array"), call(buffer, "position"), remaining)));
					} else {
						return ifNull(buffer,
								writeByte(buf, pos, value((byte) 0)),
								let(call(buffer, "remaining"), remaining ->
										sequence(
												writeVarInt(buf, pos, inc(remaining)),
												writeBytes(buf, pos, call(buffer, "array"), call(buffer, "position"), remaining))));
					}
				});
	}

	@Override
	public Expression decode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return !wrapped ?
				let(readVarInt(in),
						length -> {
							if (!nullable) {
								return let(
										arrayNew(byte[].class, length),
										array ->
												sequence(length,
														readBytes(in, array),
														staticCall(ByteBuffer.class, "wrap", array)));
							} else {
								return ifEq(length, value(0),
										nullRef(ByteBuffer.class),
										let(
												arrayNew(byte[].class, dec(length)),
												array -> sequence(
														readBytes(in, array),
														staticCall(ByteBuffer.class, "wrap", array))));
							}
						}) :
				let(readVarInt(in),
						length -> {
							if (!nullable) {
								return let(staticCall(ByteBuffer.class, "wrap", array(in), pos(in), length),
										buf -> sequence(
												move(in, length),
												buf));
							} else {
								return ifEq(length, value(0),
										nullRef(ByteBuffer.class),
										let(staticCall(ByteBuffer.class, "wrap", array(in), pos(in), dec(length)),
												result -> sequence(
														move(in, length),
														result)));
							}
						});
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.getLevel() < LEVEL_3.getLevel()) {
			return SerializerDefs.ofNullable(this);
		}
		return new ByteBufferDef(wrapped, true);
	}
}
