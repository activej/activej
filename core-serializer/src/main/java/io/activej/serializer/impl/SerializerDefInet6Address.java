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

import java.net.Inet6Address;
import java.net.NetworkInterface;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.impl.SerializerExpressions.readBytes;
import static io.activej.serializer.impl.SerializerExpressions.writeBytes;

public final class SerializerDefInet6Address extends AbstractSerializerDef {
	@Override
	public Class<?> getEncodeType() {
		return Inet6Address.class;
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return writeBytes(buf, pos, call(value, "getAddress"));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(arrayNew(byte[].class, value(16)), array ->
				sequence(
						readBytes(in, array),
						staticCall(getDecodeType(), "getByAddress", nullRef(String.class), array, nullRef(NetworkInterface.class))));
	}
}
