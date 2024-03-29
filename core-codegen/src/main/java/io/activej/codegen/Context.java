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

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.LocalVariable;
import io.activej.codegen.expression.impl.Constant;
import io.activej.codegen.util.TypeChecks;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isNotThrow;
import static io.activej.codegen.util.Utils.*;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.objectweb.asm.Type.*;

/**
 * Contains information about a dynamic class
 */
public final class Context {
	private static final Set<Method> OBJECT_INSTANCE_METHODS = Arrays.stream(Object.class.getMethods())
		.filter(m -> !isStatic(m.getModifiers()))
		.map(Method::getMethod)
		.collect(toSet());

	private final ClassLoader classLoader;
	private final ClassGenerator<?> classGenerator;
	private final GeneratorAdapter g;
	private final Type selfType;
	private final Method method;

	private Set<Method> accessibleMethods;
	private final Map<Object, LocalVariable> varLocals = new HashMap<>();
	private final Map<String, Constant> constantMap;

	public Context(
		ClassLoader classLoader, ClassGenerator<?> classGenerator, GeneratorAdapter g, Type selfType, Method method,
		Map<String, Constant> constantMap
	) {
		this.classLoader = classLoader;
		this.classGenerator = classGenerator;
		this.g = g;
		this.selfType = selfType;
		this.method = method;
		this.constantMap = constantMap;
	}

	public ClassLoader getClassLoader() {
		return classLoader;
	}

	public void setConstant(String field, Constant value) {
		constantMap.put(field, value);
	}

	public GeneratorAdapter getGeneratorAdapter() {
		return g;
	}

	public Type getSelfType() {
		return selfType;
	}

	public Class<?> getSuperclass() {
		return classGenerator.superclass;
	}

	public List<Class<?>> getInterfaces() {
		return classGenerator.interfaces;
	}

	public Map<String, Class<?>> getFields() {
		return classGenerator.fields;
	}

	public Map<Method, Expression> getMethods() {
		return classGenerator.methods;
	}

	public Set<Method> getAccessibleMethods() {
		if (accessibleMethods != null) {
			return accessibleMethods;
		}
		accessibleMethods = new HashSet<>(classGenerator.methods.keySet());
		Class<?> superclass = classGenerator.superclass;
		while (superclass != null) {
			for (java.lang.reflect.Method method : superclass.getDeclaredMethods()) {
				int modifiers = method.getModifiers();
				if (!Modifier.isStatic(modifiers) &&
					(Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers))
				) {
					accessibleMethods.add(Method.getMethod(method));
				}
			}
			superclass = superclass.getSuperclass();
		}
		return accessibleMethods;
	}

	public Map<Method, Expression> getStaticMethods() {
		return classGenerator.staticMethods;
	}

	public Method getMethod() {
		return method;
	}

	public LocalVariable newLocal(Type type) {
		if (type == Type.VOID_TYPE) return localVoid();
		int local = getGeneratorAdapter().newLocal(type);
		return local(local);
	}

	public LocalVariable ensureLocal(Object key, Expression expression) {
		LocalVariable varLocal = varLocals.get(key);
		if (varLocal == null) {
			Type type = expression.load(this);
			checkType(type, isNotThrow());

			if (type == Type.VOID_TYPE) {
				varLocal = localVoid();
			} else {
				int local = getGeneratorAdapter().newLocal(type);
				varLocal = local(local);
				g.storeLocal(local);
			}
			varLocals.put(key, varLocal);
		}
		return varLocal;
	}

	public @Nullable Type unifyTypes(@Nullable Type type1, @Nullable Type type2) {
		if (type1 == null) return type2;
		if (type2 == null) return type1;
		int sort1 = type1.getSort();
		int sort2 = type2.getSort();
		if (sort1 == ARRAY && sort2 == ARRAY) {
			if (type1.equals(type2))
				return type1;
		}
		if (sort1 == OBJECT && sort2 == OBJECT) {
			if (type1.equals(type2))
				return type1;
			Class<?> class1 = toJavaType(type1);
			Class<?> class2 = toJavaType(type2);
			if (class1.isAssignableFrom(class2)) {
				return type1;
			}
			if (class2.isAssignableFrom(class1)) {
				return type2;
			}
		}
		if (sort1 == sort2) {
			return type1;
		}
		throw new IllegalArgumentException();
	}

	private SelfOrClass toSelfOrClass(Type type) {
		return type.equals(getSelfType()) ?
			new SelfOrClass(classGenerator.superclass, classGenerator.interfaces) :
			new SelfOrClass(toJavaType(type), Collections.emptyList());
	}

	public Class<?> toJavaType(Type type) {
		if (type.equals(getSelfType()))
			throw new IllegalArgumentException();
		int sort = type.getSort();
		return switch (sort) {
			case BOOLEAN -> boolean.class;
			case CHAR -> char.class;
			case BYTE -> byte.class;
			case SHORT -> short.class;
			case INT -> int.class;
			case FLOAT -> float.class;
			case LONG -> long.class;
			case DOUBLE -> double.class;
			case VOID -> void.class;
			case OBJECT -> {
				try {
					yield classLoader.loadClass(type.getClassName());
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException(format("No class %s in class loader", type.getClassName()), e);
				}
			}
			case ARRAY -> {
				Class<?> result;
				if (type.equals(getType(Object[].class))) {
					result = Object[].class;
				} else {
					String className = type.getDescriptor().replace('/', '.');
					try {
						result = Class.forName(className);
					} catch (ClassNotFoundException e) {
						throw new IllegalArgumentException(format("No class %s in Class.forName", className), e);
					}
				}
				yield result;
			}
			default -> throw new IllegalArgumentException(format("No Java type for %s", type.getClassName()));
		};
	}

	public void cast(Type typeFrom, Type typeTo) {
		GeneratorAdapter g = getGeneratorAdapter();

		if (typeFrom.equals(typeTo)) {
			return;
		}

		if (typeTo == VOID_TYPE) {
			if (typeFrom.getSize() == 1)
				g.pop();
			if (typeFrom.getSize() == 2)
				g.pop2();
			return;
		}

		if (typeFrom == VOID_TYPE) {
			throw new RuntimeException(format("Can't cast VOID_TYPE type to %s. %s",
				typeTo.getClassName(),
				exceptionInGeneratedClass(this)));
		}

		if (typeFrom.equals(getSelfType())) {
			SelfOrClass fromSelfOrClass = toSelfOrClass(typeFrom);
			SelfOrClass toSelfOrClass = toSelfOrClass(typeTo);
			if (toSelfOrClass.isAssignableFrom(fromSelfOrClass)) {
				return;
			}
			throw new RuntimeException(format("Can't cast self %s type to %s, %s",
				typeFrom.getClassName(),
				typeTo.getClassName(),
				exceptionInGeneratedClass(this)));
		}

		if (!typeFrom.equals(getSelfType()) && !typeTo.equals(getSelfType()) &&
			toJavaType(typeTo).isAssignableFrom(toJavaType(typeFrom))
		) {
			return;
		}

		if (typeTo.equals(getType(Object.class)) && isPrimitiveType(typeFrom)) {
			g.box(typeFrom);
//			g.cast(wrap(typeFrom), getType(Object.class));
			return;
		}

		if (!isPrimitiveType(typeFrom) && !isWrapperType(typeFrom) && isPrimitiveType(typeTo)) {
			Type typeToWrapped = wrap(typeTo);
			g.checkCast(typeToWrapped);
			typeFrom = typeToWrapped;
		}

		if ((isPrimitiveType(typeFrom) || isWrapperType(typeFrom)) &&
			(isPrimitiveType(typeTo) || isWrapperType(typeTo))
		) {
			Type targetTypePrimitive = isPrimitiveType(typeTo) ? typeTo : unwrap(typeTo);

			if (isWrapperType(typeFrom)) {
				g.invokeVirtual(typeFrom, unwrapToPrimitive(targetTypePrimitive));
				return;
			}

			assert isPrimitiveType(typeFrom);

			if (isValidCast(typeFrom, targetTypePrimitive)) {
				g.cast(typeFrom, targetTypePrimitive);
			}

			if (isWrapperType(typeTo)) {
				g.valueOf(targetTypePrimitive);
			}

			return;
		}

		g.checkCast(typeTo);
	}

	public Type invoke(Expression owner, String methodName, Expression... arguments) {
		return invoke(owner, methodName, List.of(arguments));
	}

	public Type invoke(Expression owner, String methodName, List<Expression> arguments) {
		Type ownerType = owner.load(this);
		checkType(ownerType, TypeChecks.isAssignable());

		Type[] argumentTypes = getArgumentTypes(arguments);
		return invoke(ownerType, methodName, argumentTypes);
	}

	public Type invoke(Type ownerType, String methodName, Type... argumentTypes) {
		SelfOrClass[] arguments = Stream.of(argumentTypes).map(this::toSelfOrClass).toArray(SelfOrClass[]::new);
		Method foundMethod;
		if (ownerType.equals(getSelfType())) {
			foundMethod = findMethod(
				getAccessibleMethods().stream(),
				methodName,
				arguments);
			if (foundMethod == null) {
				throw new IllegalArgumentException(
					"Method not found: " + ownerType.getClassName() + '#' + methodName +
					Arrays.stream(arguments)
						.map(SelfOrClass::toString)
						.collect(joining(",", "(", ")")));
			}
			g.invokeVirtual(ownerType, foundMethod);
		} else {
			Class<?> javaOwnerType = toJavaType(ownerType);
			foundMethod = findMethod(
				Arrays.stream(javaOwnerType.getMethods())
					.filter(m -> !isStatic(m.getModifiers()))
					.map(Method::getMethod),
				methodName,
				arguments);
			if (foundMethod == null) {
				throw new IllegalArgumentException(
					"Method not found: " + ownerType.getClassName() + '#' + methodName +
					Arrays.stream(arguments)
						.map(SelfOrClass::toString)
						.collect(joining(",", "(", ")")));
			}
			invokeVirtualOrInterface(this, ownerType, foundMethod);
		}
		return foundMethod.getReturnType();
	}

	public Type invokeStatic(Type ownerType, String methodName, Expression... arguments) {
		return invokeStatic(ownerType, methodName, List.of(arguments));
	}

	public Type invokeStatic(Type ownerType, String methodName, List<Expression> arguments) {
		Type[] argumentTypes = getArgumentTypes(arguments);
		return invokeStatic(ownerType, methodName, argumentTypes);
	}

	public Type invokeStatic(Type ownerType, String methodName, Type... argumentTypes) {
		SelfOrClass[] arguments = Stream.of(argumentTypes).map(this::toSelfOrClass).toArray(SelfOrClass[]::new);
		Method foundMethod;
		if (ownerType.equals(getSelfType())) {
			foundMethod = findMethod(
				getStaticMethods().keySet().stream(),
				methodName,
				arguments);
		} else {
			foundMethod = findMethod(
				Arrays.stream(toJavaType(ownerType).getMethods())
					.filter(m -> isStatic(m.getModifiers()))
					.map(Method::getMethod),
				methodName,
				arguments);
		}
		if (foundMethod == null) {
			throw new IllegalArgumentException(
				"Static method not found: " + ownerType.getClassName() + '.' + methodName +
				Arrays.stream(arguments)
					.map(SelfOrClass::toString)
					.collect(joining(",", "(", ")")));
		}
		g.invokeStatic(ownerType, foundMethod);
		return foundMethod.getReturnType();
	}

	public Type invokeConstructor(Type ownerType, Expression... arguments) {
		return invokeConstructor(ownerType, List.of(arguments));
	}

	public Type invokeConstructor(Type ownerType, List<Expression> arguments) {
		g.newInstance(ownerType);
		g.dup();

		Type[] argumentTypes = getArgumentTypes(arguments);
		return invokeConstructor(ownerType, argumentTypes);
	}

	public Type invokeConstructor(Type ownerType, Type... argumentTypes) {
		if (ownerType.equals(getSelfType()))
			throw new IllegalArgumentException();
		SelfOrClass[] arguments = Stream.of(argumentTypes).map(this::toSelfOrClass).toArray(SelfOrClass[]::new);
		Method foundMethod = findMethod(
			Arrays.stream(toJavaType(ownerType).getConstructors()).map(Method::getMethod),
			"<init>",
			arguments);
		if (foundMethod == null) {
			throw new IllegalArgumentException(
				"Constructor not found:" + ownerType.getClassName() +
				Arrays.stream(arguments)
					.map(SelfOrClass::toString)
					.collect(joining(",", "(", ")")));
		}
		g.invokeConstructor(ownerType, foundMethod);
		return ownerType;
	}

	public Type invokeSuperConstructor(List<Expression> arguments) {
		g.loadThis();
		Type[] argumentTypes = getArgumentTypes(arguments);
		SelfOrClass[] argumentClasses = Stream.of(argumentTypes).map(this::toSelfOrClass).toArray(SelfOrClass[]::new);
		Method foundMethod = findMethod(
			Arrays.stream(classGenerator.superclass.getDeclaredConstructors()).map(Method::getMethod),
			"<init>",
			argumentClasses);
		if (foundMethod == null) {
			throw new IllegalArgumentException(
				"Parent constructor not found: " + classGenerator.superclass.getSimpleName() +
				" with arguments " +
				Arrays
					.stream(argumentClasses)
					.map(SelfOrClass::toString)
					.collect(joining(",", "(", ")")));
		}
		g.invokeConstructor(getType(classGenerator.superclass), foundMethod);

		for (String field : classGenerator.fieldExpressions.keySet()) {
			if (classGenerator.fieldsStatic.contains(field)) continue;
			Expression expression = classGenerator.fieldExpressions.get(field);
			set(property(self(), field), expression).load(this);
		}

		return VOID_TYPE;
	}

	public Type invokeSuperMethod(String methodName, Expression[] arguments) {
		return invokeSuperMethod(methodName, List.of(arguments));
	}

	public Type invokeSuperMethod(String methodName, List<Expression> arguments) {
		g.loadThis();
		Type[] argumentTypes = getArgumentTypes(arguments);
		SelfOrClass[] argumentClasses = Stream.of(argumentTypes).map(this::toSelfOrClass).toArray(SelfOrClass[]::new);
		Method foundMethod = findMethod(
			getAccessibleMethods().stream(),
			methodName,
			argumentClasses);
		if (foundMethod == null) {
			throw new IllegalArgumentException(
				"Parent method of " + classGenerator.superclass.getSimpleName() +
				" with name'" + methodName +
				"' and arguments " +
				Arrays.stream(argumentClasses)
					.map(SelfOrClass::toString)
					.collect(joining(",", "(", ")")) +
				" not found");
		}
		String typeName = getType(classGenerator.superclass).getInternalName();
		g.visitMethodInsn(Opcodes.INVOKESPECIAL, typeName, methodName, method.getDescriptor(), false);
		return foundMethod.getReturnType();
	}

	@SuppressWarnings("StatementWithEmptyBody")
	private @Nullable Method findMethod(Stream<Method> methods, String name, SelfOrClass[] arguments) {
		Set<Method> methodSet = methods.collect(toSet());

		methodSet.addAll(OBJECT_INSTANCE_METHODS);

		Method foundMethod = null;
		SelfOrClass[] foundMethodArguments = null;

		for (Method method : methodSet) {
			if (!name.equals(method.getName())) continue;
			SelfOrClass[] methodArguments = Stream.of(method.getArgumentTypes()).map(this::toSelfOrClass).toArray(SelfOrClass[]::new);
			if (!isAssignable(methodArguments, arguments)) {
				continue;
			}
			if (foundMethod == null) {
				foundMethod = method;
				foundMethodArguments = methodArguments;
			} else {
				if (isAssignable(foundMethodArguments, methodArguments)) {
					foundMethod = method;
					foundMethodArguments = methodArguments;
				} else if (isAssignable(methodArguments, foundMethodArguments)) {
					// do nothing
				} else {
					throw new IllegalArgumentException("Ambiguous method: " + method + " " + Arrays.toString(arguments));
				}
			}
		}

		return foundMethod;
	}

	private static boolean isAssignable(SelfOrClass[] to, SelfOrClass[] from) {
		if (to.length != from.length) return false;
		return IntStream.range(0, from.length)
			.allMatch(i -> to[i].isAssignableFrom(from[i]));
	}

	public static final class SelfOrClass {
		final Class<?> implementation;
		final List<Class<?>> interfaces;

		private SelfOrClass(Class<?> implementation, List<Class<?>> interfaces) {
			this.implementation = implementation;
			this.interfaces = interfaces;
		}

		boolean isAssignableFrom(Class<?> cls) {
			if (implementation.isAssignableFrom(cls)) return true;
			for (Class<?> anInterface : interfaces) {
				if (anInterface.isAssignableFrom(cls)) return true;
			}
			return false;
		}

		boolean isAssignableFrom(SelfOrClass selfOrClass) {
			if (!selfOrClass.isSelf()) {
				return isAssignableFrom(selfOrClass.implementation);
			}

			if (isSelf()) {
				// same classes
				assert implementation == selfOrClass.implementation;
				assert interfaces.equals(selfOrClass.interfaces);

				return true;
			}

			if (implementation.isAssignableFrom(selfOrClass.implementation)) return true;
			for (Class<?> anInterface : selfOrClass.interfaces) {
				if (implementation.isAssignableFrom(anInterface)) return true;
			}
			return false;
		}

		boolean isSelf() {
			return !interfaces.isEmpty();
		}

		@Override
		public String toString() {
			if (interfaces.isEmpty()) return implementation.getName();

			StringBuilder sb = new StringBuilder(implementation.getName());
			for (Class<?> anInterface : interfaces) {
				sb.append('|').append(anInterface.getName());
			}
			return sb.toString();
		}
	}

	private Type[] getArgumentTypes(List<Expression> arguments) {
		Type[] argumentTypes = new Type[arguments.size()];
		for (int i = 0; i < arguments.size(); i++) {
			Expression argument = arguments.get(i);
			Type argumentType = argument.load(this);
			checkType(argumentType, TypeChecks.isAssignable());
			argumentTypes[i] = argumentType;
		}
		return argumentTypes;
	}
}
