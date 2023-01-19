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

package io.activej.codegen.expression;

import io.activej.codegen.Context;
import io.activej.types.Primitives;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.concurrent.atomic.AtomicInteger;

import static org.objectweb.asm.Type.getType;

/**
 * Defines methods to create a constant value
 */
public final class Expression_Constant implements Expression {
	private static final AtomicInteger COUNTER = new AtomicInteger();

	private final Object value;
	private final @Nullable Class<?> cls;

	private final int id = COUNTER.incrementAndGet();

	Expression_Constant(Object value) {
		this.value = value;
		this.cls = null;
	}

	Expression_Constant(Object value, Class<?> cls) {
		if (!cls.isInstance(value)) {
			throw new IllegalArgumentException(value + " is not an instance of " + cls);
		}

		this.value = value;
		this.cls = cls;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Type type;
		if (this.cls == null) {
			if (value instanceof String) {
				type = getType(String.class);
			} else if (value instanceof Type) {
				type = (Type) value;
			} else {
				type = getType(Primitives.unwrap(value.getClass()));
			}
		} else {
			type = getType(this.cls);
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
			String field = "$STATIC_CONSTANT_" + getId();
			Class<?> aClass = cls == null ? value.getClass() : cls;
			ctx.getClassBuilder().withStaticFinalField(field, aClass, this);
			g.getStatic(ctx.getSelfType(), field, getType(aClass));
		}
		return type;
	}

	public int getId() {
		return id;
	}

	public boolean isJvmPrimitive() {
		return value.getClass() == String.class || Primitives.isWrapperType(value.getClass());
	}
}
