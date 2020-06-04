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
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static io.activej.codegen.util.Utils.exceptionInGeneratedClass;
import static java.lang.String.format;
import static org.objectweb.asm.Type.getType;

final class ExpressionStaticField implements Variable {
	private final Class<?> owner;
	private final String name;

	ExpressionStaticField(Class<?> owner, String name) {
		this.owner = owner;
		this.name = name;
	}

	@Override
	public Type load(Context ctx) {
		Type fieldType;
		Field field;
		try {
			Class<?> ownerJavaType = ctx.toJavaType(Type.getType(owner));
			field = ownerJavaType.getField(name);
			Class<?> type = field.getType();
			fieldType = getType(type);
		} catch (NoSuchFieldException ignored) {
			throw new RuntimeException(format("No static field %s.%s %s",
					owner.getName(),
					name,
					exceptionInGeneratedClass(ctx)));
		}
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		g.getStatic(Type.getType(owner), name, fieldType);
		return fieldType;
	}

	@Override
	public Object beginStore(Context ctx) {
		return getType(owner);
	}

	@Override
	public void store(Context ctx, Object storeContext, Type type) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		try {
			Field javaField = owner.getField(name);
			if (Modifier.isPublic(javaField.getModifiers()) && Modifier.isStatic(javaField.getModifiers())) {
				Type fieldType = getType(javaField.getType());
				g.putStatic((Type) storeContext, name, fieldType);
				return;
			}
		} catch (NoSuchFieldException ignored) {
		}

		throw new RuntimeException(format("No static field or setter for class %s for field \"%s\". %s ",
				((Type) storeContext).getClassName(),
				name,
				exceptionInGeneratedClass(ctx))
		);
	}
}
