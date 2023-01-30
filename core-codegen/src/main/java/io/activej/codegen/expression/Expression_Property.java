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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isObject;
import static io.activej.codegen.util.Utils.exceptionInGeneratedClass;
import static io.activej.codegen.util.Utils.invokeVirtualOrInterface;
import static java.lang.Character.toUpperCase;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Defines methods which allow taking property according to the name
 */
public final class Expression_Property implements Variable {
	private final Expression owner;
	private final String property;

	Expression_Property(Expression owner, String property) {
		this.owner = owner;
		this.property = property;
	}

	@Override
	public Type load(Context ctx) {
		Type ownerType = owner.load(ctx);
		checkType(ownerType, isObject());

		return loadPropertyOrGetter(ctx, ownerType, property);
	}

	@Override
	public Object beginStore(Context ctx) {
		Type ownerType = owner.load(ctx);
		checkType(ownerType, isObject());

		return ownerType;
	}

	@Override
	public void store(Context ctx, Object storeContext, Type type) {
		setProperty(ctx, (Type) storeContext, property, type);
	}

	public static void setProperty(Context ctx, Type ownerType, String property, Type valueType) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		Class<?> valueClass = ctx.toJavaType(valueType);

		if (ctx.getSelfType().equals(ownerType)) {
			Class<?> propertyClass = ctx.getFields().get(property);
			if (propertyClass == null) {
				throw new RuntimeException(format("No property \"%s\" in generated class %s. %s",
						property,
						ctx.getSelfType().getClassName(),
						exceptionInGeneratedClass(ctx)));
			}
			Type propertyType = getType(propertyClass);
			ctx.cast(valueType, propertyType);
			g.putField(ownerType, property, propertyType);
			return;
		}

		Class<?> argumentClass = ctx.toJavaType(ownerType);

		try {
			Field javaProperty = argumentClass.getField(property);
			if (Modifier.isPublic(javaProperty.getModifiers())) {
				Type propertyType = getType(javaProperty.getType());
				ctx.cast(valueType, propertyType);
				g.putField(ownerType, property, propertyType);
				return;
			}
		} catch (NoSuchFieldException ignored) {
		}

		Method javaSetter = tryFindSetter(argumentClass, property, valueClass);

		if (javaSetter == null && Primitives.isWrapperType(valueClass)) {
			javaSetter = tryFindSetter(argumentClass, property, Primitives.unwrap(valueClass));
		}

		if (javaSetter == null && valueClass.isPrimitive()) {
			javaSetter = tryFindSetter(argumentClass, property, Primitives.wrap(valueClass));
		}

		if (javaSetter == null) {
			javaSetter = tryFindSetter(argumentClass, property);
		}

		if (javaSetter != null) {
			Type fieldType = getType(javaSetter.getParameterTypes()[0]);
			ctx.cast(valueType, fieldType);
			invokeVirtualOrInterface(ctx, ownerType, getMethod(javaSetter));
			Type returnType = getType(javaSetter.getReturnType());
			if (returnType.getSize() == 1) {
				g.pop();
			} else if (returnType.getSize() == 2) {
				g.pop2();
			}
			return;
		}

		throw new RuntimeException(format("No public property or setter for class %s for property \"%s\". %s ",
				ownerType.getClassName(),
				property,
				exceptionInGeneratedClass(ctx))
		);
	}

	private static @Nullable Method tryFindSetter(Class<?> argumentClass, String property, Class<?> valueClass) {
		Method m = null;
		try {
			m = argumentClass.getDeclaredMethod(property, valueClass);
		} catch (NoSuchMethodException ignored) {
		}

		if (m == null && property.length() >= 1) {
			try {
				m = argumentClass.getDeclaredMethod("set" + toUpperCase(property.charAt(0)) + property.substring(1), valueClass);
			} catch (NoSuchMethodException ignored) {
			}
		}
		return m;
	}

	private static @Nullable Method tryFindSetter(Class<?> argumentClass, String property) {
		String setterName = "set" + toUpperCase(property.charAt(0)) + property.substring(1);

		for (Method method : argumentClass.getDeclaredMethods()) {
			if (method.getParameterTypes().length != 1)
				continue;
			if (method.getName().equals(property) || method.getName().equals(setterName))
				return method;
		}
		return null;
	}

	public static Type loadPropertyOrGetter(Context ctx, Type ownerType, String property) {
		return loadPropertyOrGetter(ctx, ownerType, property, true);
	}

	public static Type typeOfPropertyOrGetter(Context ctx, Type ownerType, String property) {
		return loadPropertyOrGetter(ctx, ownerType, property, false);
	}

	private static Type loadPropertyOrGetter(Context ctx, Type ownerType, String property, boolean load) {
		GeneratorAdapter g = load ? ctx.getGeneratorAdapter() : null;

		if (ownerType.equals(ctx.getSelfType())) {
			Class<?> thisPropertyClass = ctx.getFields().get(property);
			if (thisPropertyClass != null) {
				Type resultType = Type.getType(thisPropertyClass);
				if (g != null) {
					g.getField(ownerType, property, resultType);
				}
				return resultType;
			} else {
				throw new RuntimeException(format("No public property or getter for class %s for property \"%s\". %s",
						ownerType.getClassName(),
						property,
						exceptionInGeneratedClass(ctx)));
			}
		}

		Class<?> argumentClass = ctx.toJavaType(ownerType);

		try {
			Field javaProperty = argumentClass.getField(property);
			if (isPublic(javaProperty.getModifiers()) && !isStatic(javaProperty.getModifiers())) {
				Type resultType = Type.getType(javaProperty.getType());
				if (g != null) {
					g.getField(ownerType, property, resultType);
				}
				return resultType;
			}
		} catch (NoSuchFieldException ignored) {
		}

		Method m = null;
		try {
			m = argumentClass.getDeclaredMethod(property);
		} catch (NoSuchMethodException ignored) {
		}

		if (m == null && property.length() >= 1) {
			try {
				m = argumentClass.getDeclaredMethod("get" + toUpperCase(property.charAt(0)) + property.substring(1));
			} catch (NoSuchMethodException ignored) {
			}
		}

		if (m != null) {
			Type resultType = getType(m.getReturnType());
			if (g != null) {
				invokeVirtualOrInterface(ctx, ownerType, getMethod(m));
			}
			return resultType;
		}

		throw new RuntimeException(format("No public property or getter for class %s for property \"%s\". %s",
				ownerType.getClassName(),
				property,
				exceptionInGeneratedClass(ctx)));
	}
}
