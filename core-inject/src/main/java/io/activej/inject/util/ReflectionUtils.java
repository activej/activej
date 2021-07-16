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
import io.activej.inject.annotation.Optional;
import io.activej.inject.annotation.*;
import io.activej.inject.binding.*;
import io.activej.inject.impl.BindingInitializer;
import io.activej.inject.impl.BindingLocator;
import io.activej.inject.impl.CompiledBinding;
import io.activej.inject.impl.CompiledBindingInitializer;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.ModuleBuilder1;
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
import static java.util.Collections.singleton;
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

	@Nullable
	public static Object getOuterClassInstance(Object innerClassInstance) {
		if (innerClassInstance == null) {
			return null;
		}
		Class<?> cls = innerClassInstance.getClass();
		Class<?> enclosingClass = cls.getEnclosingClass();
		if (enclosingClass == null) {
			return null;
		}
		return Arrays.stream(cls.getDeclaredFields())
				.filter(f -> f.isSynthetic() && f.getName().startsWith("this$") && f.getType() == enclosingClass)
				.findAny()
				.map(f -> {
					f.setAccessible(true);
					try {
						return f.get(innerClassInstance);
					} catch (IllegalAccessException e) {
						return null;
					}
				})
				.orElse(null);
	}

	@Nullable
	public static Object qualifierOf(AnnotatedElement annotatedElement) {
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
		return keyOf(container, null, type, annotatedElement);
	}

	public static <T> Key<T> keyOf(@Nullable Type container, @Nullable Object containerInstance, Type type, AnnotatedElement annotatedElement) {
		Type resolved = container != null ? Types.resolveTypeVariables(type, container, containerInstance) : type;
		return Key.ofType(resolved, qualifierOf(annotatedElement));
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
		switch (scopes.size()) {
			case 0:
				return Scope.UNSCOPED;
			case 1:
				return new Scope[]{Scope.of(scopes.iterator().next())};
			default:
				throw new DIException("More than one scope annotation on " + annotatedElement);
		}
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
		return generateImplicitBinding(key, null);
	}

	public static <T> Binding<T> generateImplicitBinding(Key<T> key, @Nullable T containerInstance) {
		Binding<T> binding = generateConstructorBinding(key, containerInstance);
		return binding != null ?
				binding.initializeWith(generateInjectingInitializer(key, containerInstance)) :
				null;
	}

	@Nullable
	public static <T> Binding<T> generateConstructorBinding(Key<T> key) {
		return generateConstructorBinding(key, null);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> Binding<T> generateConstructorBinding(Key<T> key, @Nullable T containerInstance) {
		Class<?> cls = key.getRawType();

		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);
		Set<Constructor<?>> injectConstructors = Arrays.stream(cls.getDeclaredConstructors())
				.filter(c -> c.isAnnotationPresent(Inject.class))
				.collect(toSet());
		Set<Method> factoryMethods = Arrays.stream(cls.getDeclaredMethods())
				.filter(method -> method.isAnnotationPresent(Inject.class)
						&& method.getReturnType() == cls
						&& Modifier.isStatic(method.getModifiers()))
				.collect(toSet());

		if (classInjectAnnotation != null) {
			if (!injectConstructors.isEmpty()) {
				throw failedImplicitBinding(key, "inject annotation on class with inject constructor");
			}
			if (!factoryMethods.isEmpty()) {
				throw failedImplicitBinding(key, "inject annotation on class with inject factory method");
			}
			Class<?> enclosingClass = cls.getEnclosingClass();
			if (enclosingClass != null && !Modifier.isStatic(cls.getModifiers())) {
				try {
					return bindingFromConstructor(key, containerInstance, (Constructor<T>) cls.getDeclaredConstructor(enclosingClass));
				} catch (NoSuchMethodException e) {
					throw failedImplicitBinding(key, "inject annotation on local class that closes over outside variables and/or has no default constructor");
				}
			}
			try {
				return bindingFromConstructor(key, containerInstance, (Constructor<T>) cls.getDeclaredConstructor());
			} catch (NoSuchMethodException e) {
				throw failedImplicitBinding(key, "inject annotation on class with no default constructor");
			}
		} else {
			if (injectConstructors.size() > 1) {
				throw failedImplicitBinding(key, "more than one inject constructor");
			}
			if (!injectConstructors.isEmpty()) {
				if (!factoryMethods.isEmpty()) {
					throw failedImplicitBinding(key, "both inject constructor and inject factory method are present");
				}
				return bindingFromConstructor(key, containerInstance, (Constructor<T>) injectConstructors.iterator().next());
			}
		}

		if (factoryMethods.size() > 1) {
			throw failedImplicitBinding(key, "more than one inject factory method");
		}
		if (!factoryMethods.isEmpty()) {
			return bindingFromMethod(null, factoryMethods.iterator().next());
		}
		return null;
	}

	private static DIException failedImplicitBinding(Key<?> requestedKey, String message) {
		return new DIException("Failed to generate implicit binding for " + requestedKey.getDisplayString() + ", " + message);
	}

	public static <T> BindingInitializer<T> generateInjectingInitializer(Key<T> container) {
		return generateInjectingInitializer(container, null);
	}

	public static <T> BindingInitializer<T> generateInjectingInitializer(Key<T> container, @Nullable T containerInstance) {
		Class<T> rawType = container.getRawType();
		List<BindingInitializer<T>> initializers = Stream.concat(
				getAnnotatedElements(rawType, Inject.class, Class::getDeclaredFields, false).stream()
						.map(field -> fieldInjector(container, containerInstance, field, !field.isAnnotationPresent(Optional.class))),
				getAnnotatedElements(rawType, Inject.class, Class::getDeclaredMethods, true).stream()
						.filter(method -> !Modifier.isStatic(method.getModifiers())) // we allow them and just filter out to allow static factory methods
						.map(method -> methodInjector(container, containerInstance, method)))
				.collect(toList());
		return BindingInitializer.combine(initializers);
	}

	public static <T> BindingInitializer<T> fieldInjector(Key<T> container, Field field, boolean required) {
		return fieldInjector(container, null, field, required);
	}

	public static <T> BindingInitializer<T> fieldInjector(Key<T> container, @Nullable T containerInstance, Field field, boolean required) {
		field.setAccessible(true);
		Key<Object> key = keyOf(container.getType(), containerInstance, field.getGenericType(), field);
		return BindingInitializer.of(
				singleton(Dependency.toKey(key, required)),
				compiledBindings -> {
					CompiledBinding<Object> binding = compiledBindings.get(key);
					//noinspection Convert2Lambda
					return new CompiledBindingInitializer<T>() {
						@SuppressWarnings("rawtypes")
						@Override
						public void initInstance(T instance, AtomicReferenceArray[] instances, int synchronizedScope) {
							Object arg = binding.getInstance(instances, synchronizedScope);
							if (arg == null) {
								return;
							}
							try {
								field.set(instance, arg);
							} catch (IllegalAccessException e) {
								throw new DIException("Not allowed to set injectable field " + field, e);
							}
						}
					};
				});
	}

	public static <T> BindingInitializer<T> methodInjector(Key<T> container, Method method) {
		return methodInjector(container, null, method);
	}

	@SuppressWarnings("rawtypes")
	public static <T> BindingInitializer<T> methodInjector(Key<T> container, @Nullable T containerInstance, Method method) {
		method.setAccessible(true);
		Dependency[] dependencies = toDependencies(container.getType(), containerInstance, method.getParameters());
		return BindingInitializer.of(
				Stream.of(dependencies).collect(toSet()),
				compiledBindings -> {
					CompiledBinding[] argBindings = Stream.of(dependencies)
							.map(dependency -> compiledBindings.get(dependency.getKey()))
							.toArray(CompiledBinding[]::new);
					//noinspection Convert2Lambda
					return new CompiledBindingInitializer<T>() {
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
				});
	}

	@NotNull
	public static Dependency[] toDependencies(@Nullable Type container, Parameter[] parameters) {
		return toDependencies(container, null, parameters);
	}

	@NotNull
	public static Dependency[] toDependencies(@Nullable Type container, @Nullable Object containerInstance, Parameter[] parameters) {
		Dependency[] dependencies = new Dependency[parameters.length];
		if (parameters.length == 0) {
			return dependencies;
		}
		// an actual JDK bug (fixed in Java 9)
		boolean workaround = parameters[0].getDeclaringExecutable().getParameterAnnotations().length != parameters.length;
		for (int i = 0; i < dependencies.length; i++) {
			Type type = parameters[i].getParameterizedType();
			Parameter parameter = parameters[workaround && i != 0 ? i - 1 : i];
			dependencies[i] = Dependency.toKey(keyOf(container, containerInstance, type, parameter), !parameter.isAnnotationPresent(Optional.class));
		}
		return dependencies;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingFromMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		Binding<T> binding = Binding.to(
				args -> {
					try {
						return (T) method.invoke(module, args);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call method " + method, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call method " + method, e.getCause());
					}
				},
				toDependencies(module != null ? module.getClass() : method.getDeclaringClass(), module, method.getParameters()));

		return module != null ? binding.at(LocationInfo.from(module, method)) : binding;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingFromGenericMethod(@Nullable Object module, Key<?> requestedKey, Method method) {
		method.setAccessible(true);

		Type genericReturnType = method.getGenericReturnType();
		Map<TypeVariable<?>, Type> mapping = Types.extractMatchingGenerics(genericReturnType, requestedKey.getType());

		Dependency[] dependencies = Arrays.stream(method.getParameters())
				.map(parameter -> {
					Type type = Types.resolveTypeVariables(parameter.getParameterizedType(), mapping);
					Object qualifier = qualifierOf(parameter);
					return Dependency.toKey(Key.ofType(type, qualifier), !parameter.isAnnotationPresent(Optional.class));
				})
				.toArray(Dependency[]::new);

		Binding<T> binding = Binding.to(
				args -> {
					try {
						return (T) method.invoke(module, args);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call generic method " + method + " to provide requested key " + requestedKey, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call generic method " + method + " to provide requested key " + requestedKey, e.getCause());
					}
				},
				dependencies);
		return module != null ? binding.at(LocationInfo.from(module, method)) : binding;
	}

	public static <T> Binding<T> bindingFromConstructor(Key<T> key, Constructor<T> constructor) {
		return bindingFromConstructor(key, null, constructor);
	}

	public static <T> Binding<T> bindingFromConstructor(Key<T> key, @Nullable T containerInstance, Constructor<T> constructor) {
		constructor.setAccessible(true);

		Dependency[] dependencies = toDependencies(key.getType(), containerInstance, constructor.getParameters());

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

				Type returnType = Types.resolveTypeVariables(method.getGenericReturnType(), module != null ? module.getClass() : moduleClass, module);
				TypeVariable<Method>[] typeVars = method.getTypeParameters();

				if (typeVars.length == 0) {
					Key<Object> key = Key.ofType(returnType, qualifier);

					ModuleBuilder1<Object> binder = builder.bind(key).to(bindingFromMethod(module, method)).in(methodScope);
					if (isEager) {
						binder.asEager();
					}
					if (isTransient) {
						binder.asTransient();
					}
					continue;
				}
				Set<TypeVariable<?>> unused = Arrays.stream(typeVars)
						.filter(typeVar -> !Types.contains(returnType, typeVar))
						.collect(toSet());
				if (!unused.isEmpty()) {
					throw new DIException("Generic type variables " + unused + " are not used in return type of templated provider method " + method);
				}
				builder.generate(KeyPattern.of((Class<Object>) method.getReturnType()), new TemplatedProviderGenerator(methodScope, qualifier, method, module, returnType, isEager ? EAGER : isTransient ? TRANSIENT : REGULAR));

			} else if (method.isAnnotationPresent(ProvidesIntoSet.class)) {
				if (module == null && !Modifier.isStatic(method.getModifiers())) {
					throw new DIException("Found non-static provider method while scanning for statics, method " + method);
				}
				if (method.getTypeParameters().length != 0) {
					throw new DIException("@ProvidesIntoSet does not support templated methods, method " + method);
				}

				Type type = Types.resolveTypeVariables(method.getGenericReturnType(), module != null ? module.getClass() : moduleClass, module);
				Scope[] methodScope = getScope(method);

				boolean isEager = method.isAnnotationPresent(Eager.class);
				boolean isTransient = method.isAnnotationPresent(Transient.class);

				Key<Object> key = Key.ofType(type, uniqueQualifier());

				builder.bind(key).to(bindingFromMethod(module, method)).in(methodScope);

				Key<Set<Object>> setKey = Key.ofType(Types.parameterized(Set.class, type), qualifierOf(method));

				Binding<Set<Object>> binding = Binding.to(Collections::singleton, key);

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
		@Nullable
		private final Object qualifier;
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
			if (scope.length < methodScope.length || (qualifier != null && !qualifier.equals(key.getQualifier())) || !Types.matches(key.getType(), returnType)) {
				return null;
			}
			for (int i = 0; i < methodScope.length; i++) {
				if (!scope[i].equals(methodScope[i])) {
					return null;
				}
			}
			return bindingFromGenericMethod(module, key, method).as(bindingType);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TemplatedProviderGenerator generator = (TemplatedProviderGenerator) o;

			if (!Arrays.equals(methodScope, generator.methodScope)) return false;
			if (!Objects.equals(qualifier, generator.qualifier)) return false;
			return method.equals(generator.method);
		}

		@Override
		public int hashCode() {
			return 961 * Arrays.hashCode(methodScope) + 31 * (qualifier != null ? qualifier.hashCode() : 0) + method.hashCode();
		}
	}
}
