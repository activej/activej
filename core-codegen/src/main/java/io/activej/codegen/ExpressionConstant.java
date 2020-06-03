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

package io.activej.codegen;

import io.activej.codegen.utils.Primitives;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.common.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

/**
 * Defines methods to create a constant value
 */
final class ExpressionConstant implements Expression {
	@NotNull
	private final Object value;
	@Nullable
	private final Type type;

	private String staticConstantField;

	ExpressionConstant(Object value) {
		this.value = checkNotNull(value);
		this.type = null;
	}

	ExpressionConstant(Object value, @Nullable Type type) {
		this.value = checkNotNull(value);
		this.type = type;
	}

	ExpressionConstant(Object value, Class<?> type) {
		this.value = checkNotNull(value);
		this.type = getType(type);
	}

	public @NotNull Object getValue() {
		return value;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type type = this.type;
		if (type == null) {
			if (value instanceof String) {
				type = getType(String.class);
			} else if (value instanceof Type) {
				type = (Type) value;
			} else {
				type = getType(Primitives.unwrap(value.getClass()));
			}
		}
		if (value instanceof Byte) {
			g.push((Byte) value);
		} else if (value instanceof Short) {
			g.push((Short) value);
		} else if (value instanceof Integer) {
			g.push((Integer) value);
		} else if (value instanceof Long) {
			g.push((Long) value);
		} else if (value instanceof Float) {
			g.push((Float) value);
		} else if (value instanceof Double) {
			g.push((Double) value);
		} else if (value instanceof Boolean) {
			g.push((Boolean) value);
		} else if (value instanceof Character) {
			g.push((Character) value);
		} else if (value instanceof String) {
			g.push((String) value);
		} else if (value instanceof Type) {
			g.push((Type) value);
		} else if (value instanceof Enum) {
			g.getStatic(type, ((Enum<?>) value).name(), type);
		} else {
			if (staticConstantField == null) {
				staticConstantField = "$STATIC_CONSTANT_" + (ctx.getStaticConstants().size() + 1);
				ctx.addStaticConstant(staticConstantField, value);
			}
			g.getStatic(ctx.getSelfType(), staticConstantField, getType(value.getClass()));
		}
		return type;
	}
}
