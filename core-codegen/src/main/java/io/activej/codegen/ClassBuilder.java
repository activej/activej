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

import io.activej.codegen.DefiningClassLoader.ClassKey;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.ExpressionConstant;
import io.activej.codegen.util.DefiningClassWriter;
import io.activej.codegen.util.WithInitializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.codegen.util.Utils.getPathSetting;
import static io.activej.codegen.util.Utils.getStringSetting;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 * Intends for dynamic description of the behaviour of the object in runtime
 *
 * @param <T> type of item
 */
@SuppressWarnings({"unchecked", "WeakerAccess", "unused"})
public final class ClassBuilder<T> implements WithInitializer<ClassBuilder<T>> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static final String CLASS_BUILDER_MARKER = "$GENERATED";
	public static final String PACKAGE_PREFIX = getStringSetting(ClassBuilder.class, "packagePrefix", "io.activej.codegen.");
	public static final Path DEFAULT_SAVE_DIR = getPathSetting(ClassBuilder.class, "saveDir", null);

	private static final AtomicInteger COUNTER = new AtomicInteger();
	private static final ConcurrentHashMap<Integer, Object> STATIC_CONSTANTS = new ConcurrentHashMap<>();

	private final DefiningClassLoader classLoader;
	private final String className;

	protected final Class<?> superclass;
	protected final List<Class<?>> interfaces;
	@Nullable
	private ClassKey classKey;
	private Path bytecodeSaveDir = DEFAULT_SAVE_DIR;
	@Nullable
	private String customClassName;

	protected final Map<String, Class<?>> fields = new LinkedHashMap<>();
	protected final Set<String> fieldsFinal = new HashSet<>();
	protected final Set<String> fieldsStatic = new HashSet<>();
	protected final Map<String, Expression> fieldExpressions = new HashMap<>();

	protected final Map<Method, Expression> methods = new LinkedHashMap<>();
	protected final Map<Method, Expression> staticMethods = new LinkedHashMap<>();

	protected final Map<Method, Expression> constructors = new LinkedHashMap<>();
	protected final List<Expression> staticInitializers = new ArrayList<>();

	// region builders

	/**
	 * Creates a new instance of ClassBuilder
	 *
	 * @param classLoader class loader
	 * @param superclass  type of dynamic class
	 */
	private ClassBuilder(DefiningClassLoader classLoader, Class<?> superclass, List<Class<?>> interfaces, @NotNull String className) {
		this.classLoader = classLoader;
		this.superclass = superclass;
		this.interfaces = interfaces;
		this.className = PACKAGE_PREFIX + className;
		this.classKey = null;
		withStaticField(CLASS_BUILDER_MARKER, Void.class);
	}

	public static <T> ClassBuilder<T> create(DefiningClassLoader classLoader, Class<? super T> implementation, Class<?>... interfaces) {
		return create(classLoader, implementation, asList(interfaces));
	}

	public static <T> ClassBuilder<T> create(DefiningClassLoader classLoader, Class<? super T> implementation, List<Class<?>> interfaces) {
		if (!interfaces.stream().allMatch(Class::isInterface))
			throw new IllegalArgumentException();
		if (implementation.isInterface()) {
			return new ClassBuilder<>(classLoader,
					Object.class,
					Stream.concat(Stream.of(implementation), interfaces.stream()).collect(toList()),
					implementation.getName());
		} else {
			return new ClassBuilder<>(classLoader,
					implementation,
					interfaces,
					implementation.getName());
		}
	}

	public ClassBuilder<T> withBytecodeSaveDir(Path bytecodeSaveDir) {
		this.bytecodeSaveDir = bytecodeSaveDir;
		return this;
	}

	public ClassBuilder<T> withClassKey(Object... keyParameters) {
		this.classKey = keyParameters != null ? new ClassKey(superclass, new HashSet<>(interfaces), asList(keyParameters)) : null;
		return this;
	}

	public ClassBuilder<T> withClassName(String name) {
		this.customClassName = name;
		return this;
	}

	public ClassBuilder<T> withStaticInitializer(Expression expression) {
		staticInitializers.add(expression);
		return this;
	}

	public ClassBuilder<T> withConstructor(Expression expression) {
		return withConstructor(emptyList(), expression);
	}

	public ClassBuilder<T> withConstructor(List<? extends Class<?>> argumentTypes, Expression expression) {
		constructors.put(new Method("<init>", VOID_TYPE, argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
		return this;
	}

	/**
	 * Creates a new field for a dynamic class
	 *
	 * @param field name of field
	 * @param type  type of field
	 * @return changed AsmFunctionFactory
	 */
	public ClassBuilder<T> withField(String field, Class<?> type) {
		fields.put(field, type);
		return this;
	}

	public ClassBuilder<T> withField(String field, Class<?> type, Expression value) {
		fields.put(field, type);
		fieldExpressions.put(field, value);
		return this;
	}

	public ClassBuilder<T> withFinalField(String field, Class<?> type, Expression value) {
		fields.put(field, type);
		fieldsFinal.add(field);
		fieldExpressions.put(field, value);
		return this;
	}

	/**
	 * Creates a new method for a dynamic class
	 *
	 * @param methodName    name of method
	 * @param returnType    type which returns this method
	 * @param argumentTypes list of types of arguments
	 * @param expression    function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public ClassBuilder<T> withMethod(String methodName, Class<?> returnType, List<? extends Class<?>> argumentTypes, Expression expression) {
		methods.put(new Method(methodName, getType(returnType), argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
		return this;
	}

	/**
	 * CCreates a new method for a dynamic class
	 *
	 * @param methodName name of method
	 * @param expression function which will be processed
	 * @return changed AsmFunctionFactory
	 */
	public ClassBuilder<T> withMethod(String methodName, Expression expression) {
		if (methodName.contains("(")) {
			Method method = Method.getMethod(methodName);
			methods.put(method, expression);
			return this;
		}

		Method foundMethod = null;
		List<List<java.lang.reflect.Method>> listOfMethods = new ArrayList<>();
		listOfMethods.add(asList(Object.class.getMethods()));
		listOfMethods.add(asList(superclass.getMethods()));
		listOfMethods.add(asList(superclass.getDeclaredMethods()));
		for (Class<?> type : interfaces) {
			listOfMethods.add(asList(type.getMethods()));
			listOfMethods.add(asList(type.getDeclaredMethods()));
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

	public ClassBuilder<T> withStaticMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		setStaticMethod(methodName, returnClass, argumentTypes, expression);
		return this;
	}

	public void setStaticMethod(String methodName, Class<?> returnClass, List<? extends Class<?>> argumentTypes, Expression expression) {
		staticMethods.put(new Method(methodName, getType(returnClass), argumentTypes.stream().map(Type::getType).toArray(Type[]::new)), expression);
	}

	public ClassBuilder<T> withStaticField(String field, Class<?> type) {
		this.fields.put(field, type);
		this.fieldsStatic.add(field);
		return this;
	}

	public ClassBuilder<T> withStaticField(String field, Class<?> type, Expression value) {
		this.fields.put(field, type);
		this.fieldsStatic.add(field);
		this.fieldExpressions.put(field, value);
		return this;
	}

	public ClassBuilder<T> withStaticFinalField(String field, Class<?> type, Expression value) {
		this.fields.put(field, type);
		this.fieldsStatic.add(field);
		this.fieldsFinal.add(field);
		if (value instanceof ExpressionConstant && !((ExpressionConstant) value).isJvmPrimitive()) {
			STATIC_CONSTANTS.put(((ExpressionConstant) value).getId(), ((ExpressionConstant) value).getValue());
		}
		this.fieldExpressions.put(field, value);
		return this;
	}

	public static Object getStaticConstant(int id) {
		return STATIC_CONSTANTS.get(id);
	}

	public static int getStaticConstantsSize() {
		return STATIC_CONSTANTS.size();
	}

	// For testing purposes
	public static void clearStaticConstants() {
		STATIC_CONSTANTS.clear();
	}
	// endregion

	public Class<T> build() {
		if (classKey != null) {
			Class<?> cachedClass = classLoader.getCachedClass(classKey);

			if (cachedClass != null) {
				cleanup();
				return (Class<T>) cachedClass;
			}
		}

		try {
			String actualClassName = customClassName != null ? customClassName : className + '_' + COUNTER.incrementAndGet();
			byte[] bytecode = defineNewClass(actualClassName);

			synchronized (classLoader) {
				if (classKey != null) {
					Class<?> cachedClass = classLoader.getCachedClass(classKey);

					if (cachedClass != null) {
						return (Class<T>) cachedClass;
					}
				}
				Class<T> aClass = (Class<T>) classLoader.defineAndCacheClass(classKey, actualClassName, bytecode);
				try {
					Field field = aClass.getField(CLASS_BUILDER_MARKER);
					//noinspection ResultOfMethodCallIgnored
					field.get(null);
				} catch (IllegalAccessException | NoSuchFieldException e) {
					throw new AssertionError(e);
				}
				return aClass;
			}
		} finally {
			cleanup();
		}
	}

	public void cleanup() {
		for (Expression expression : this.fieldExpressions.values()) {
			if (expression instanceof ExpressionConstant) {
				STATIC_CONSTANTS.remove(((ExpressionConstant) expression).getId());
			}
		}
	}

	private byte[] defineNewClass(String actualClassName) {
		DefiningClassWriter cw = DefiningClassWriter.create(classLoader);

		Type classType = getType('L' + actualClassName.replace('.', '/') + ';');

		cw.visit(V1_6, ACC_PUBLIC + ACC_FINAL + ACC_SUPER,
				classType.getInternalName(),
				null,
				getInternalName(superclass),
				interfaces.stream().map(Type::getInternalName).toArray(String[]::new));

		Map<Method, Expression> constructors = new LinkedHashMap<>(this.constructors);
		if (constructors.isEmpty()) {
			constructors.put(new Method("<init>", VOID_TYPE, new Type[]{}), superConstructor());
		}

		for (Method m : constructors.keySet()) {
			Expression expression = constructors.get(m);

			GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC, m, null, null, cw);
			Context ctx = new Context(classLoader, this, g, classType, m);
			ctx.cast(expression.load(ctx), m.getReturnType());
			g.returnValue();

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
				ctx.cast(expression.load(ctx), m.getReturnType());
				g.returnValue();

				g.endMethod();
			}

			for (Method m : newStaticMethods) {
				GeneratorAdapter g = new GeneratorAdapter(ACC_PUBLIC + ACC_STATIC + ACC_FINAL, m, null, null, cw);

				Context ctx = new Context(classLoader, this, g, classType, m);

				Expression expression = this.staticMethods.get(m);
				ctx.cast(expression.load(ctx), m.getReturnType());
				g.returnValue();

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

			for (String field : this.fieldExpressions.keySet()) {
				if (!this.fieldsStatic.contains(field)) continue;
				Expression expression = this.fieldExpressions.get(field);

				if (expression instanceof ExpressionConstant && !((ExpressionConstant) expression).isJvmPrimitive()) {
					set(staticField(field), cast(
							staticCall(ClassBuilder.class, "getStaticConstant", value(((ExpressionConstant) expression).getId())),
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

		if (bytecodeSaveDir != null) {
			String classFileName = customClassName != null ? actualClassName : actualClassName.substring(PACKAGE_PREFIX.length());
			try (FileOutputStream fos = new FileOutputStream(bytecodeSaveDir.resolve(classFileName + ".class").toFile())) {
				fos.write(cw.toByteArray());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		cw.visitEnd();

		return cw.toByteArray();
	}

	public T buildClassAndCreateNewInstance() {
		try {
			return build().getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	public T buildClassAndCreateNewInstance(Object... constructorParameters) {
		Class<?>[] constructorParameterTypes = new Class<?>[constructorParameters.length];
		for (int i = 0; i < constructorParameters.length; i++) {
			constructorParameterTypes[i] = constructorParameters[i].getClass();
		}
		return buildClassAndCreateNewInstance(constructorParameterTypes, constructorParameters);
	}

	public T buildClassAndCreateNewInstance(Class<?>[] constructorParameterTypes, Object[] constructorParameters) {
		try {
			return build().getConstructor(constructorParameterTypes).newInstance(constructorParameters);
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}
}
