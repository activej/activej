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

package io.activej.inject.util;

import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.Scope;
import io.activej.inject.annotation.*;
import io.activej.inject.binding.*;
import io.activej.inject.impl.*;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.ModuleBuilder1;
import io.activej.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.activej.inject.Qualifiers.uniqueQualifier;
import static io.activej.inject.binding.BindingType.*;
import static io.activej.inject.util.Utils.isMarker;
import static io.activej.types.Types.parameterizedType;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * These are various reflection utilities that are used by the DSL.
 * While you should not use them normally, they are pretty well organized and thus are left public.
 */
public final class ReflectionUtils {
	private static final String IDENT = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";

	private static final Pattern PACKAGE = Pattern.compile("(?:" + IDENT + "\\.)*");
	private static final Pattern PACKAGE_AND_PARENT = Pattern.compile(PACKAGE.pattern() + "(?:" + IDENT + "\\$\\d*)?");
	private static final Pattern ARRAY_SIGNATURE = Pattern.compile("\\[L(.*?);");
	private static final Pattern RAW_PART = Pattern.compile("^" + IDENT);

	public static String getDisplayName(Type type) {
		Class<?> raw = Types.getRawType(type);
		String typeName;
		if (raw.isAnonymousClass()) {
			Type superclass = raw.getGenericSuperclass();
			typeName = "? extends " + superclass.getTypeName();
		} else {
			typeName = type.getTypeName();
		}

		String defaultName = PACKAGE_AND_PARENT.matcher(ARRAY_SIGNATURE.matcher(typeName).replaceAll("$1[]")).replaceAll("");

		ShortTypeName override = raw.getDeclaredAnnotation(ShortTypeName.class);
		return override != null ?
				RAW_PART.matcher(defaultName).replaceFirst(override.value()) :
				defaultName;
	}

	public static String getShortName(Type type) {
		return PACKAGE.matcher(ARRAY_SIGNATURE.matcher(type.getTypeName()).replaceAll("$1[]")).replaceAll("");
	}

	public static @Nullable Object getOuterClassInstance(Object innerClassInstance) {
		if (innerClassInstance == null) {
			return null;
		}
		Class<?> cls = innerClassInstance.getClass();
		Class<?> enclosingClass = cls.getEnclosingClass();
		if (enclosingClass == null) {
			return null;
		}
		for (Field field : cls.getDeclaredFields()) {
			if (!field.isSynthetic() || !field.getName().startsWith("this$") || field.getType() != enclosingClass)
				continue;
			field.setAccessible(true);
			try {
				return field.get(innerClassInstance);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	public static @Nullable Object qualifierOf(AnnotatedElement annotatedElement) {
		List<Annotation> names = new ArrayList<>();
		for (Annotation annotation : annotatedElement.getDeclaredAnnotations()) {
			if (annotation.annotationType().isAnnotationPresent(QualifierAnnotation.class)) {
				names.add(annotation);
			}
		}
		switch (names.size()) {
			case 0:
				return null;
			case 1:
				Annotation annotation = names.iterator().next();
				if (annotation instanceof Named) {
					return ((Named) annotation).value();
				}
				Class<? extends Annotation> annotationType = annotation.annotationType();
				if (isMarker(annotationType)) {
					return annotationType;
				}
				return annotation;
			default:
				throw new DIException("More than one name annotation on " + annotatedElement);
		}
	}

	public static <T> Key<T> keyOf(@Nullable Type container, Type type, AnnotatedElement annotatedElement) {
		return Key.ofType(container != null ? Types.bind(type, Types.getAllTypeBindings(container)) : type, qualifierOf(annotatedElement));
	}

	public static Scope[] getScope(AnnotatedElement annotatedElement) {
		Annotation[] annotations = annotatedElement.getDeclaredAnnotations();

		Set<Annotation> scopes = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(ScopeAnnotation.class))
				.collect(toSet());

		Scopes nested = (Scopes) Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType() == Scopes.class)
				.findAny()
				.orElse(null);

		if (nested != null) {
			if (scopes.isEmpty()) {
				return Arrays.stream(nested.value()).map(Scope::of).toArray(Scope[]::new);
			}
			throw new DIException("Cannot have both @Scoped and a scope annotation on " + annotatedElement);
		}
		return switch (scopes.size()) {
			case 0 -> Scope.UNSCOPED;
			case 1 -> new Scope[]{Scope.of(scopes.iterator().next())};
			default -> throw new DIException("More than one scope annotation on " + annotatedElement);
		};
	}

	public static <T extends AnnotatedElement & Member> List<T> getAnnotatedElements(Class<?> cls,
			Class<? extends Annotation> annotationType, Function<Class<?>, T[]> extractor, boolean allowStatic) {

		List<T> result = new ArrayList<>();
		while (cls != null) {
			for (T element : extractor.apply(cls)) {
				if (element.isAnnotationPresent(annotationType)) {
					if (!allowStatic && Modifier.isStatic(element.getModifiers())) {
						throw new DIException("@" + annotationType.getSimpleName() + " annotation is not allowed on " + element);
					}
					result.add(element);
				}
			}
			cls = cls.getSuperclass();
		}
		return result;
	}

	public static <T> Binding<T> generateImplicitBinding(Key<T> key) {
		Binding<T> binding = generateConstructorBinding(key);
		return binding != null ?
				binding.initializeWith(generateInjectingInitializer(key)).as(SYNTHETIC) :
				null;
	}

	@SuppressWarnings("unchecked")
	public static <T> @Nullable Binding<T> generateConstructorBinding(Key<T> key) {
		Class<?> cls = key.getRawType();

		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);
		Set<Constructor<?>> injectConstructors = new HashSet<>();
		Set<Constructor<?>> constructors = new HashSet<>();
		for (Constructor<?> c : cls.getDeclaredConstructors()) {
			if (c.isAnnotationPresent(Inject.class)) {
				injectConstructors.add(c);
			}
			constructors.add(c);
		}

		Set<Method> injectFactoryMethods = new HashSet<>();
		Set<Method> factoryMethods = new HashSet<>();

		for (Method method : cls.getDeclaredMethods()) {
			if (method.getReturnType() == cls
					&& Modifier.isStatic(method.getModifiers())) {
				if (method.isAnnotationPresent(Inject.class)) {
					injectFactoryMethods.add(method);
				}
				factoryMethods.add(method);
			}
		}

		if (classInjectAnnotation != null) {
			if (!injectConstructors.isEmpty()) {
				throw failedImplicitBinding(key, "inject annotation on class with inject constructor");
			}
			if (!factoryMethods.isEmpty()) {
				throw failedImplicitBinding(key, "inject annotation on class with factory method");
			}
			if (constructors.size() == 0) {
				throw failedImplicitBinding(key, "inject annotation on interface");
			}
			if (constructors.size() > 1) {
				throw failedImplicitBinding(key, "inject annotation on class with multiple constructors");
			}
			Constructor<T> declaredConstructor = (Constructor<T>) constructors.iterator().next();

			Class<?> enclosingClass = cls.getEnclosingClass();
			if (enclosingClass != null && !Modifier.isStatic(cls.getModifiers()) && declaredConstructor.getParameterCount() != 1) {
				throw failedImplicitBinding(key, "inject annotation on local class that closes over outside variables and/or has no default constructor");
			}
			return bindingFromConstructor(key, declaredConstructor);
		}
		if (!injectConstructors.isEmpty()) {
			if (injectConstructors.size() > 1) {
				throw failedImplicitBinding(key, "more than one inject constructor");
			}
			if (!injectFactoryMethods.isEmpty()) {
				throw failedImplicitBinding(key, "both inject constructor and inject factory method are present");
			}
			return bindingFromConstructor(key, (Constructor<T>) injectConstructors.iterator().next());
		}

		if (!injectFactoryMethods.isEmpty()) {
			if (injectFactoryMethods.size() > 1) {
				throw failedImplicitBinding(key, "more than one inject factory method");
			}
			return bindingFromMethod(null, injectFactoryMethods.iterator().next());
		}
		return null;
	}

	private static DIException failedImplicitBinding(Key<?> requestedKey, String message) {
		return new DIException("Failed to generate implicit binding for " + requestedKey.getDisplayString() + ", " + message);
	}

	public static <T> BindingInitializer<T> generateInjectingInitializer(Key<T> container) {
		Class<T> rawType = container.getRawType();
		List<BindingInitializer<T>> initializers = Stream.concat(
						getAnnotatedElements(rawType, Inject.class, Class::getDeclaredFields, false).stream()
								.map(field -> fieldInjector(container, field)),
						getAnnotatedElements(rawType, Inject.class, Class::getDeclaredMethods, true).stream()
								.filter(method -> !Modifier.isStatic(method.getModifiers())) // we allow them and just filter out to allow static factory methods
								.map(method -> methodInjector(container, method)))
				.collect(toList());
		return BindingInitializer.combine(initializers);
	}

	public static <T> BindingInitializer<T> fieldInjector(Key<T> container, Field field) {
		field.setAccessible(true);
		Key<Object> key = keyOf(container.getType(), field.getGenericType(), field);
		return new BindingInitializer<>(Set.of(key)) {
			@Override
			public CompiledBindingInitializer<T> compile(CompiledBindingLocator compiledBindings) {
				CompiledBinding<Object> binding = compiledBindings.get(key);
				//noinspection Convert2Lambda
				return new CompiledBindingInitializer<>() {
					@SuppressWarnings("rawtypes")
					@Override
					public void initInstance(T instance, AtomicReferenceArray[] instances, int synchronizedScope) {
						Object arg = binding.getInstance(instances, synchronizedScope);
						try {
							field.set(instance, arg);
						} catch (IllegalAccessException e) {
							throw new DIException("Not allowed to set injectable field " + field, e);
						}
					}
				};
			}
		};
	}

	@SuppressWarnings("rawtypes")
	public static <T> BindingInitializer<T> methodInjector(Key<T> container, Method method) {
		method.setAccessible(true);
		Key<?>[] dependencies = toDependencies(container.getType(), method);
		return new BindingInitializer<>(Set.of(dependencies)) {
			@Override
			public CompiledBindingInitializer<T> compile(CompiledBindingLocator compiledBindings) {
				CompiledBinding[] argBindings = Stream.of(dependencies)
						.map(compiledBindings::get)
						.toArray(CompiledBinding[]::new);
				//noinspection Convert2Lambda
				return new CompiledBindingInitializer<>() {
					@Override
					public void initInstance(T instance, AtomicReferenceArray[] instances, int synchronizedScope) {
						Object[] args = new Object[argBindings.length];
						for (int i = 0; i < argBindings.length; i++) {
							args[i] = argBindings[i].getInstance(instances, synchronizedScope);
						}
						try {
							method.invoke(instance, args);
						} catch (IllegalAccessException e) {
							throw new DIException("Not allowed to call injectable method " + method, e);
						} catch (InvocationTargetException e) {
							throw new DIException("Failed to call injectable method " + method, e.getCause());
						}
					}
				};
			}
		};
	}

	public static Key<?>[] toDependencies(@Nullable Type container, Executable executable) {
		Parameter[] parameters = executable.getParameters();
		Key<?>[] dependencies = new Key<?>[parameters.length];
		if (parameters.length == 0) {
			return dependencies;
		}

		Type type = parameters[0].getParameterizedType();
		Parameter parameter = parameters[0];
		dependencies[0] = keyOf(container, type, parameter);

		Type[] genericParameterTypes = executable.getGenericParameterTypes();
		boolean hasImplicitDependency = genericParameterTypes.length != parameters.length;
		for (int i = 1; i < dependencies.length; i++) {
			type = genericParameterTypes[hasImplicitDependency ? i - 1 : i];
			parameter = parameters[i];
			dependencies[i] = keyOf(container, type, parameter);
		}
		return dependencies;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingFromMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		Binding<T> binding = Binding.to(
				args -> {
					try {
						T result = (T) method.invoke(module, args);
						if (result == null)
							throw new NullPointerException("@Provides method must return non-null result, method " + method);
						return result;
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call method " + method, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call method " + method, e.getCause());
					}
				},
				toDependencies(module != null ? module.getClass() : method.getDeclaringClass(), method));

		return module != null ? binding.at(LocationInfo.from(module, method)) : binding;
	}

	public static <T> Binding<T> bindingFromConstructor(Key<T> key, Constructor<T> constructor) {
		constructor.setAccessible(true);

		Key<?>[] dependencies = toDependencies(key.getType(), constructor);

		return Binding.to(
				args -> {
					try {
						return constructor.newInstance(args);
					} catch (InstantiationException e) {
						throw new DIException("Cannot instantiate object from the constructor " + constructor + " to provide requested key " + key, e);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call constructor " + constructor + " to provide requested key " + key, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call constructor " + constructor + " to provide requested key " + key, e.getCause());
					}
				},
				dependencies);
	}

	public static Module scanClass(@NotNull Class<?> moduleClass, @Nullable Object module) {
		return scanClassInto(moduleClass, module, ModuleBuilder.create());
	}

	@SuppressWarnings("unchecked")
	public static Module scanClassInto(@NotNull Class<?> moduleClass, @Nullable Object module, ModuleBuilder builder) {
		for (Method method : moduleClass.getDeclaredMethods()) {
			if (method.isAnnotationPresent(Provides.class)) {
				if (module == null && !Modifier.isStatic(method.getModifiers())) {
					throw new DIException("Found non-static provider method while scanning for statics, method " + method);
				}

				Object qualifier = qualifierOf(method);
				Scope[] methodScope = getScope(method);

				boolean isEager = method.isAnnotationPresent(Eager.class);
				boolean isTransient = method.isAnnotationPresent(Transient.class);

				TypeVariable<Method>[] methodTypeParameters = method.getTypeParameters();
				Map<TypeVariable<?>, Type> mapping = new HashMap<>();
				for (TypeVariable<Method> methodTypeParameter : methodTypeParameters) {
					mapping.put(methodTypeParameter, methodTypeParameter);
				}
				mapping.putAll(Types.getAllTypeBindings(module != null ? module.getClass() : moduleClass));

				Type returnType = Types.bind(method.getGenericReturnType(), mapping);

				if (methodTypeParameters.length == 0) {
					Key<Object> key = Key.ofType(returnType, qualifier);

					ModuleBuilder1<Object> binder = builder.bind(key).to(bindingFromMethod(module, method)).in(methodScope);
					if (isEager) binder.asEager();
					if (isTransient) binder.asTransient();
				} else {
					Set<TypeVariable<?>> unused = Arrays.stream(methodTypeParameters)
							.filter(typeVar -> !TypeUtils.contains(returnType, typeVar))
							.collect(toSet());
					if (!unused.isEmpty()) {
						throw new DIException("Generic type variables " + unused + " are not used in return type of templated provider method " + method);
					}
					builder.generate(
							KeyPattern.of((Class<Object>) method.getReturnType()),
							new TemplatedProviderGenerator(methodScope, qualifier, method, module, returnType, isEager ? EAGER : isTransient ? TRANSIENT : REGULAR));
				}

			} else if (method.isAnnotationPresent(ProvidesIntoSet.class)) {
				if (module == null && !Modifier.isStatic(method.getModifiers())) {
					throw new DIException("Found non-static provider method while scanning for statics, method " + method);
				}
				if (method.getTypeParameters().length != 0) {
					throw new DIException("@ProvidesIntoSet does not support templated methods, method " + method);
				}

				Type container = module != null ? module.getClass() : moduleClass;
				Type type = Types.bind(method.getGenericReturnType(), Types.getAllTypeBindings(container));
				Scope[] methodScope = getScope(method);

				boolean isEager = method.isAnnotationPresent(Eager.class);
				boolean isTransient = method.isAnnotationPresent(Transient.class);

				Key<Object> key = Key.ofType(type, uniqueQualifier());

				builder.bind(key).to(bindingFromMethod(module, method)).in(methodScope);

				Key<Set<Object>> setKey = Key.ofType(parameterizedType(Set.class, type), qualifierOf(method));

				Binding<Set<Object>> binding = Binding.to(Set::of, key);

				if (module != null) {
					binding.at(LocationInfo.from(module, method));
				}

				ModuleBuilder1<Set<Object>> setBinder = builder.bind(setKey).to(binding).in(methodScope);
				if (isEager) {
					setBinder.asEager();
				}
				if (isTransient) {
					setBinder.asTransient();
				}
				builder.multibind(setKey, Multibinders.toSet());
			}
		}

		return builder.build();
	}

	public static Map<Class<?>, Module> scanClassHierarchy(@NotNull Class<?> moduleClass, @Nullable Object module) {
		Map<Class<?>, Module> result = new HashMap<>();
		Class<?> cls = moduleClass;
		while (cls != Object.class && cls != null) {
			result.put(cls, scanClass(cls, module));
			cls = cls.getSuperclass();
		}
		return result;
	}

	private static class TemplatedProviderGenerator implements BindingGenerator<Object> {
		private final Scope[] methodScope;
		private final @Nullable Object qualifier;
		private final Method method;

		private final Object module;
		private final Type returnType;
		private final BindingType bindingType;

		private TemplatedProviderGenerator(Scope[] methodScope, @Nullable Object qualifier, Method method, Object module, Type returnType, BindingType bindingType) {
			this.methodScope = methodScope;
			this.qualifier = qualifier;
			this.method = method;
			this.module = module;
			this.returnType = returnType;
			this.bindingType = bindingType;
		}

		@Override
		public @Nullable Binding<Object> generate(BindingLocator bindings, Scope[] scope, Key<Object> key) {
			if (scope.length < methodScope.length || (qualifier != null && !qualifier.equals(key.getQualifier())) || !TypeUtils.matches(key.getType(), returnType)) {
				return null;
			}
			for (int i = 0; i < methodScope.length; i++) {
				if (!scope[i].equals(methodScope[i])) {
					return null;
				}
			}
			method.setAccessible(true);

			Type genericReturnType = method.getGenericReturnType();
			Map<TypeVariable<?>, Type> mapping = TypeUtils.extractMatchingGenerics(genericReturnType, key.getType());

			Key<?>[] dependencies = Arrays.stream(method.getParameters())
					.map(parameter -> {
						Type type = Types.bind(parameter.getParameterizedType(), mapping);
						Object q = qualifierOf(parameter);
						return Key.ofType(type, q);
					})
					.toArray(Key<?>[]::new);

			Binding<Object> binding = Binding.to(
					args -> {
						try {
							Object result = method.invoke(module, args);
							if (result == null)
								throw new NullPointerException("@Provides method must return non-null result, method " + method);
							return result;
						} catch (IllegalAccessException e) {
							throw new DIException("Not allowed to call generic method " + method + " to provide requested key " + key, e);
						} catch (InvocationTargetException e) {
							throw new DIException("Failed to call generic method " + method + " to provide requested key " + key, e.getCause());
						}
					},
					dependencies);

			return (module != null ? binding.at(LocationInfo.from(module, method)) : binding).as(bindingType);
		}
	}
}
