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
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.BiConsumer;

import static io.activej.codegen.util.Utils.exceptionInGeneratedClass;
import static java.lang.String.format;
import static org.objectweb.asm.Type.getType;

public final class Expression_StaticField implements Variable {
	private final @Nullable Class<?> owner;
	private final String name;

	Expression_StaticField(@Nullable Class<?> owner, String name) {
		this.owner = owner;
		this.name = name;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		return findField(ctx, (ownerType, fieldType) -> g.getStatic(ownerType, name, fieldType));
	}

	@Override
	public Object beginStore(Context ctx) {
		return null;
	}

	@Override
	public void store(Context ctx, Object storeContext, Type type) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		findField(ctx, (ownerType, fieldType) -> g.putStatic(ownerType, name, fieldType));
	}

	private Type findField(Context ctx, BiConsumer<Type, Type> action) {
		Type ownerType;
		Type fieldType;
		if (owner != null) {

			Field javaField;
			try {
				Class<?> ownerJavaType = ctx.toJavaType(Type.getType(owner));
				javaField = ownerJavaType.getField(name);
			} catch (NoSuchFieldException ignored) {
				throw new RuntimeException(format("No static field %s.%s %s",
						owner.getName(),
						name,
						exceptionInGeneratedClass(ctx)));
			}

			if (!(Modifier.isPublic(javaField.getModifiers()) && Modifier.isStatic(javaField.getModifiers()))) {
				throw new RuntimeException(format("No static field or setter for class %s for field \"%s\". %s ",
						owner.getName(),
						name,
						exceptionInGeneratedClass(ctx))
				);
			}

			ownerType = getType(owner);
			fieldType = getType(javaField.getType());
		} else {
			ownerType = ctx.getSelfType();
			fieldType = getType(ctx.getFields().get(name));
		}
		action.accept(ownerType, fieldType);
		return fieldType;
	}

}
