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
import io.activej.codegen.expression.Expression_Constant;
import io.activej.codegen.util.DefiningClassWriter;
import io.activej.common.initializer.WithInitializer;
import org.jetbrains.annotations.ApiStatus.Internal;
import org.jetbrains.annotations.VisibleForTesting;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.activej.codegen.DefiningClassLoader.createInstance;
import static io.activej.codegen.expression.Expressions.*;
import static io.activej.codegen.util.Utils.getStringSetting;
import static java.util.stream.Collectors.toList;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Intends for dynamic description of the  object behaviour in runtime
 *
 * @param <T> type of class to be generated
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class ClassBuilder<T> implements WithInitializer<ClassBuilder<T>> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static final String CLASS_BUILDER_MARKER = "$GENERATED";
	public static final String PACKAGE_PREFIX = getStringSetting(ClassBuilder.class, "packagePrefix", "io.activej.codegen.");

	private static final AtomicInteger COUNTER = new AtomicInteger();
	private static final ConcurrentHashMap<Integer, Object> STATIC_CONSTANTS = new ConcurrentHashMap<>();

	final Class<?> superclass;
	final List<Class<?>> interfaces;

	private String className;
	private final String autoClassName;

	final Map<String, Class<?>> fields = new LinkedHashMap<>();
	final Set<String> fieldsFinal = new HashSet<>();
	final Set<String> fieldsStatic = new HashSet<>();
	final Map<String, Expression> fieldExpressions = new HashMap<>();

	final Map<Method, Expression> methods = new LinkedHashMap<>();
	final Map<Method, Expression> staticMethods = new LinkedHashMap<>();

	private final Map<Method, Expression> constructors = new LinkedHashMap<>();
	private final List<Expression> staticInitializers = new ArrayList<>();

	// region builders

	private ClassBuilder(Class<?> superclass, List<Class<?>> interfaces, String className) {
		this.superclass = superclass;
		this.interfaces = interfaces;
		this.autoClassName = PACKAGE_PREFIX + className;
		withStaticField(CLASS_BUILDER_MARKER, Void.class);
	}

	/**
	 * Creates a new instance of ClassBuilder
	 *
	 * @param implementation type of dynamic class
	 * @param interfaces     additional interfaces for the class to implement
	 */
	public static <T> ClassBuilder<T> create(Class<?> implementation, List<Class<?>> interfaces) {
		if (!interfaces.stream().allMatch(Class::isInterface))
			throw new IllegalArgumentException();
		if (implementation.isInterface()) {
			return new ClassBuilder<>(
					Object.class,
					Stream.concat(Stream.of(implementation), interfaces.stream()).collect(toList()),
					implementation.getName());
		} else {
			return new ClassBuilder<>(
					implementation,
					interfaces,
					implementation.getName());
		}
	}

	/**
	 * Creates a new instance of ClassBuilder
	 *
	 * @param implementation type of dynamic class
	 * @param interfaces     additional interfaces for the class to implement
	 */
	public static <T> ClassBuilder<T> create(Class<T> implementation, Class<?>... interfaces) {
		return create(implementation, List.of(interfaces));
	}

	/**
	 * Sets a class name for the generated class
	 *
	 * @param name a class name of the generated class
	 */
	public ClassBuilder<T> withClassName(String name) {
		this.className = name;
		return this;
	}

	/**
	 * Adds static initializer for the generated class
	 * (code that would be executed inside a static initialization block of the generated class)
	 *
	 * @param expression an expression that represents static initializer
	 */
	public ClassBuilder<T> withStaticInitializer(Expression expression) {
		staticInitializers.add(expression);
		return this;
	}

	/**
	 * Adds a constructor for the given class with specified argument types and an {@link Exception}
	 * that would be executed inside the constructor
	 *
	 * @param argumentTypes types of arguments to the constructor
	 * @param expression    an expression that would be executed inside the constructor
	 */
	public ClassBuilder<T> withConstructor(List<? extends Class<?>> argumentTypes, Expression expression) {
		constructors.put(new Method("<init>", VOID_TYPE, argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
		return this;
	}

	/**
	 * Adds a constructor for the given class with an {@link Exception}
	 * that would be executed inside the constructor
	 *
	 * @param expression an expression that would be executed inside the constructor
	 * @see #withConstructor(List, Expression)
	 */
	public ClassBuilder<T> withConstructor(Expression expression) {
		return withConstructor(List.of(), expression);
	}

	/**
	 * Adds a new uninitialized field for a class
	 *
	 * @param field name of the field
	 * @param type  type of the field
	 */
	public ClassBuilder<T> withField(String field, Class<?> type) {
		fields.put(field, type);
		return this;
	}

	/**
	 * Adds a new initialized field for a class
	 *
	 * @param field name of the field
	 * @param type  type of  the field
	 * @param value an expression that represents how the new field will be initialized
	 */
	public ClassBuilder<T> withField(String field, Class<?> type, Expression value) {
		fields.put(field, type);
		fieldExpressions.put(field, value);
		return this;
	}

	/**
	 * Adds a new final initialized field for a class
	 *
	 * @param field name of the field
	 * @param type  type of the field
	 * @param value an expression that represents how the new final field will be initialized
	 */
	public ClassBuilder<T> withFinalField(String field, Class<?> type, Expression value) {
		fields.put(field, type);
		fieldsFinal.add(field);
		fieldExpressions.put(field, value);
		return this;
	}

	/**
	 * Adds a new method to a class
	 *
	 * @param methodName    name of the method
	 * @param returnType    a return type of the method
	 * @param argumentTypes list of the method's arguments
	 * @param expression    an expression that represents the method's body
	 */
	public ClassBuilder<T> withMethod(String methodName, Class<?> returnType, List<? extends Class<?>> argumentTypes, Expression expression) {
		methods.put(new Method(methodName, getType(returnType), argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
		return this;
	}

	/**
	 * Adds a new method to a class
	 *
	 * @param methodName name of the method
	 * @param expression an expression that represents the method's body
	 */
	public ClassBuilder<T> withMethod(String methodName, Expression expression) {
		if (methodName.contains("(")) {
			Method method = Method.getMethod(methodName);
			methods.put(method, expression);
			return this;
		}

		Method foundMethod = null;
		List<List<java.lang.reflect.Method>> listOfMethods = new ArrayList<>();
		listOfMethods.add(List.of(Object.class.getMethods()));
		listOfMethods.add(List.of(superclass.getMethods()));
		listOfMethods.add(List.of(superclass.getDeclaredMethods()));
		for (Class<?> type : interfaces) {
			listOfMethods.add(List.of(type.getMethods()));
			listOfMethods.add(List.of(type.getDeclaredMethods()));
		}
		for (List<java.lang.reflect.Method> list : listOfMethods) {
			for (java.lang.reflect.Method m : list) {
				if (m.getName().equals(methodName)) {
					Method method = getMethod(m);
					if (foundMethod != null && !method.equals(foundMethod))
						throw new IllegalArgumentException("Method " + method + " collides with " + foundMethod);
					foundMethod = method;
				}
			}
		}
		if (foundMethod == null)
			throw new IllegalArgumentException(String.format("Could not find method '%s'", methodName));
		methods.put(foundMethod, expression);
		return this;
	}

	/**
	 * @see #setStaticMethod(String, Class, List, Expression)
	 */
	public ClassBuilder<T> withStaticMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		setStaticMethod(methodName, returnClass, argumentTypes, expression);
		return this;
	}

	/**
	 * Adds a static method to a class
	 *
	 * @param methodName    a name of the method
	 * @param returnClass   the method's return type
	 * @param argumentTypes types of the method's arguments
	 * @param expression    an expression that represents the method's body
	 */
	public void setStaticMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		staticMethods.put(new Method(methodName, getType(returnClass), argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
	}

	/**
	 * Adds a new uninitialized static field for a class
	 *
	 * @param field name of the field
	 * @param type  type of the field
	 */
	public ClassBuilder<T> withStaticField(String field, Class<?> type) {
		this.fields.put(field, type);
		this.fieldsStatic.add(field);
		return this;
	}

	/**
	 * Adds a new initialized static field for a class
	 *
	 * @param field name of the field
	 * @param type  type of  the field
	 * @param value an expression that represents how the new static field will be initialized
	 */
	public ClassBuilder<T> withStaticField(String field, Class<?> type, Expression value) {
		this.fields.put(field, type);
		this.fieldsStatic.add(field);
		this.fieldExpressions.put(field, value);
		return this;
	}

	/**
	 * Adds a new static final initialized field for a class
	 *
	 * @param field name of the field
	 * @param type  type of the field
	 * @param value an expression that represents how the new static final field will be initialized
	 */
	public ClassBuilder<T> withStaticFinalField(String field, Class<?> type, Expression value) {
		this.fields.put(field, type);
		this.fieldsStatic.add(field);
		this.fieldsFinal.add(field);
		if (value instanceof Expression_Constant && !((Expression_Constant) value).isJvmPrimitive()) {
			STATIC_CONSTANTS.put(((Expression_Constant) value).getId(), ((Expression_Constant) value).getValue());
		}
		this.fieldExpressions.put(field, value);
		return this;
	}

	/**
	 * Returns a static constant by an integer id
	 * <p>
	 * This method is used internally by generated classes for constant initialization
	 *
	 * @param id id of a static constant
	 * @return static constant
	 */
	@Internal
	public static Object getStaticConstant(int id) {
		return STATIC_CONSTANTS.get(id);
	}

	/**
	 * Returns a size of static constants
	 */
	@VisibleForTesting
	public static int getStaticConstantsSize() {
		return STATIC_CONSTANTS.size();
	}

	/**
	 * Clears all static constants
	 */
	@VisibleForTesting
	public static void clearStaticConstants() {
		STATIC_CONSTANTS.clear();
	}
	// endregion

	/**
	 * Defines a class from {@code this} {@link ClassBuilder} using given {@link DefiningClassLoader}
	 *
	 * @param classLoader a class loader that would be used to define a class
	 * @return a defined class
	 */
	public Class<T> defineClass(DefiningClassLoader classLoader) {
		GeneratedBytecode generatedBytecode = toBytecode(classLoader);
		//noinspection unchecked
		return (Class<T>) generatedBytecode.defineClass(classLoader);
	}

	/**
	 * Defines a class from {@code this} {@link ClassBuilder} using given {@link DefiningClassLoader}
	 * and creates an instance of defined class.
	 *
	 * @param classLoader a class loader that would be used to define a class
	 * @param arguments   an array of parameters that would be passed to the constructor of a defined class
	 * @return an instance of a defined class
	 */
	public T defineClassAndCreateInstance(DefiningClassLoader classLoader, Object... arguments) {
		Class<T> aClass = defineClass(classLoader);
		return createInstance(aClass, arguments);
	}

	/**
	 * Uses a given class loader to generate a bytecode out of this class builder.
	 *
	 * @param classLoader a class loader for generating a bytecode
	 * @return a generated bytecode which consists of actual bytecode as well as a class name
	 * @see GeneratedBytecode
	 */
	public GeneratedBytecode toBytecode(ClassLoader classLoader) {
		return toBytecode(classLoader, className != null ? className : autoClassName + '_' + COUNTER.incrementAndGet());
	}

	/**
	 * Uses a given class loader to generate a bytecode out of this class builder.
	 *
	 * @param classLoader a class loader for generating a bytecode
	 * @param className   a name of a class
	 * @return a generated bytecode which consists of actual bytecode as well as a class name
	 * @see GeneratedBytecode
	 */
	public GeneratedBytecode toBytecode(ClassLoader classLoader, String className) {
		byte[] bytecode = toBytecode(className, classLoader);
		return new GeneratedBytecode(className, bytecode) {
			@Override
			protected void onDefinedClass(Class<?> clazz) {
				try {
					Field field = clazz.getField(CLASS_BUILDER_MARKER);
					//noinspection ResultOfMethodCallIgnored
					field.get(null);
				} catch (IllegalAccessException | NoSuchFieldException e) {
					throw new AssertionError(e);
				} finally {
					cleanup();
				}
			}

			@Override
			protected void onError(Exception e) {
				cleanup();
			}
		};
	}

	private byte[] toBytecode(String className, ClassLoader classLoader) {
		DefiningClassWriter cw = DefiningClassWriter.create(classLoader);

		Type classType = getType('L' + className.replace('.', '/') + ';');

		cw.visit(V1_6, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
				classType.getInternalName(),
				null,
				getInternalName(superclass),
				interfaces.stream().map(Type::getInternalName).toArray(String[]::new));

		Map<Method, Expression> constructors = new LinkedHashMap<>(this.constructors);
		if (constructors.isEmpty()) {
			constructors.put(new Method("<init>", VOID_TYPE, new Type[]{}), superConstructor());
		}

		for (Map.Entry<Method, Expression> entry : constructors.entrySet()) {
			Method method = entry.getKey();

			GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, method, null, null, cw);
			Context ctx = new Context(classLoader, this, g, classType, method);
			Type type = entry.getValue().load(ctx);
			if (type != null) {
				ctx.cast(type, method.getReturnType());
				g.returnValue();
			}

			g.endMethod();
		}

		Set<Method> methods = new HashSet<>();
		Set<Method> staticMethods = new HashSet<>();
		Set<String> fields = new HashSet<>();

		while (true) {
			Set<String> newFields = new LinkedHashSet<>(this.fields.keySet());
			newFields.removeAll(fields);
			Set<Method> newMethods = new LinkedHashSet<>(this.methods.keySet());
			newMethods.removeAll(methods);
			Set<Method> newStaticMethods = new LinkedHashSet<>(this.staticMethods.keySet());
			newStaticMethods.removeAll(staticMethods);

			if (newFields.isEmpty() && newMethods.isEmpty() && newStaticMethods.isEmpty()) {
				break;
			}

			for (String field : newFields) {
				cw.visitField(ACC_PUBLIC + (fieldsStatic.contains(field) ? ACC_STATIC : 0) + (fieldsFinal.contains(field) ? ACC_FINAL : 0),
						field, getType(this.fields.get(field)).getDescriptor(), null, null);
			}

			for (Method m : newMethods) {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC + ACC_FINAL, m, null, null, cw);

				Context ctx = new Context(classLoader, this, g, classType, m);

				Expression expression = this.methods.get(m);
				Type type = expression.load(ctx);
				if (type != null) {
					ctx.cast(type, m.getReturnType());
					g.returnValue();
				}

				g.endMethod();
			}

			for (Method m : newStaticMethods) {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC + ACC_STATIC + ACC_FINAL, m, null, null, cw);

				Context ctx = new Context(classLoader, this, g, classType, m);

				Expression expression = this.staticMethods.get(m);
				Type type = expression.load(ctx);
				if (type != null) {
					ctx.cast(type, m.getReturnType());
					g.returnValue();
				}

				g.endMethod();
			}

			fields.addAll(newFields);
			methods.addAll(newMethods);
			staticMethods.addAll(newStaticMethods);
		}

		{
			Method m = getMethod("void <clinit> ()");
			GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC + ACC_STATIC, m, null, null, cw);

			Context ctx = new Context(classLoader, this, g, classType, m);

			for (Map.Entry<String, Expression> entry : this.fieldExpressions.entrySet()) {
				String field = entry.getKey();
				if (!this.fieldsStatic.contains(field)) continue;
				Expression expression = entry.getValue();

				if (expression instanceof Expression_Constant && !((Expression_Constant) expression).isJvmPrimitive()) {
					set(staticField(field), cast(
							staticCall(ClassBuilder.class, "getStaticConstant", value(((Expression_Constant) expression).getId())),
							this.fields.get(field)))
							.load(ctx);
				} else {
					set(staticField(field), expression).load(ctx);
				}
			}

			for (Expression initializer : staticInitializers) {
				initializer.load(ctx);
			}

			g.returnValue();
			g.endMethod();
		}

		cw.visitEnd();

		return cw.toByteArray();
	}

	private void cleanup() {
		for (Expression expression : this.fieldExpressions.values()) {
			if (expression instanceof Expression_Constant) {
				STATIC_CONSTANTS.remove(((Expression_Constant) expression).getId());
			}
		}
	}

}
