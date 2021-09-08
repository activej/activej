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

import static io.activej.codegen.expression.Expressions.cast;
import static io.activej.codegen.util.Primitives.wrap;

public abstract class SerializerDefPrimitive extends AbstractSerializerDef implements SerializerDef {
	protected final Class<?> primitiveType;
	protected final Class<?> wrappedType;
	protected final boolean wrapped;

	protected SerializerDefPrimitive(Class<?> primitiveType, boolean wrapped) {
		if (!primitiveType.isPrimitive())
			throw new IllegalArgumentException("Not a primitive type");
		this.primitiveType = primitiveType;
		this.wrappedType = wrap(primitiveType);
		this.wrapped = wrapped;
	}

	@Override
	public Class<?> getEncodeType() {
		return wrapped ? wrappedType : primitiveType;
	}

	public boolean isWrapped() {
		return wrapped;
	}

	public abstract SerializerDef ensureWrapped();

	protected abstract Expression doSerialize(Expression byteArray, Variable off, Expression value, CompatibilityLevel compatibilityLevel);

	protected abstract Expression doDeserialize(Expression in, CompatibilityLevel compatibilityLevel);

	protected boolean castToPrimitive() {
		return true;
	}

	@Override
	public final Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return doSerialize(buf, pos, castToPrimitive() ? cast(value, primitiveType) : value, compatibilityLevel);
	}

	@Override
	public final Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		Expression expression = doDeserialize(in, compatibilityLevel);
		return wrapped ? cast(expression, wrappedType) : expression;
	}
}
