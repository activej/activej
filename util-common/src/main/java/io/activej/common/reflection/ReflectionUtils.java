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

package io.activej.common.reflection;

import io.activej.common.exception.UncheckedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkArgument;
import static java.util.stream.Collectors.toList;

public final class ReflectionUtils {

	public static boolean isPrimitiveType(Class<?> cls) {
		return cls == boolean.class
				|| cls == byte.class
				|| cls == char.class
				|| cls == short.class
				|| cls == int.class
				|| cls == long.class
				|| cls == float.class
				|| cls == double.class;
	}

	public static boolean isBoxedPrimitiveType(Class<?> cls) {
		return cls == Boolean.class
				|| cls == Byte.class
				|| cls == Character.class
				|| cls == Short.class
				|| cls == Integer.class
				|| cls == Long.class
				|| cls == Float.class
				|| cls == Double.class;
	}

	public static boolean isPrimitiveTypeOrBox(Class<?> cls) {
		return isPrimitiveType(cls) || isBoxedPrimitiveType(cls);
	}

	public static boolean isSimpleType(Class<?> cls) {
		return isPrimitiveTypeOrBox(cls) || cls == String.class;
	}

	public static boolean isThrowable(Class<?> cls) {
		return Throwable.class.isAssignableFrom(cls);
	}

	public static boolean isPublic(Class<?> cls) {
		return Modifier.isPublic(cls.getModifiers());
	}

	public static boolean isPublic(Method method) {
		return Modifier.isPublic(method.getModifiers());
	}

	public static boolean isGetter(Method method) {
		return method.getName().length() > 2
				&& method.getParameterCount() == 0
				&& (method.getName().startsWith("get") && method.getReturnType() != void.class
				|| method.getName().startsWith("is") && (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class));
	}

	public static boolean isSetter(Method method) {
		return method.getName().length() > 3
				&& method.getName().startsWith("set")
				&& method.getReturnType() == void.class
				&& method.getParameterCount() == 1;
	}

	public static String extractFieldNameFromGetter(Method getter) {
		return extractFieldNameFromGetterName(getter.getName());
	}

	public static String extractFieldNameFromGetterName(String getterName) {
		if (getterName.startsWith("get")) {
			if (getterName.length() == 3) {
				return "";
			}
			String firstLetter = getterName.substring(3, 4);
			String restOfName = getterName.substring(4);
			return firstLetter.toLowerCase() + restOfName;
		}
		if (getterName.startsWith("is") && getterName.length() > 2) {
			String firstLetter = getterName.substring(2, 3);
			String restOfName = getterName.substring(3);
			return firstLetter.toLowerCase() + restOfName;
		}
		throw new IllegalArgumentException("Given method is not a getter");
	}

	public static String extractFieldNameFromSetter(Method setter) {
		return extractFieldNameFromSetterName(setter.getName());
	}

	public static String extractFieldNameFromSetterName(String setterName) {
		checkArgument(setterName.startsWith("set") && setterName.length() > 3, "Given method is not a setter");

		String firstLetter = setterName.substring(3, 4);
		String restOfName = setterName.substring(4);
		return firstLetter.toLowerCase() + restOfName;
	}

	@SuppressWarnings("unchecked")
	private static <T> @Nullable Supplier<T> getConstructorOrFactory(Class<T> cls, String... factoryMethodNames) {
		for (String methodName : factoryMethodNames) {
			Method method;
			try {
				method = cls.getDeclaredMethod(methodName);
			} catch (NoSuchMethodException e) {
				continue;
			}
			if ((method.getModifiers() & (Modifier.PUBLIC | Modifier.STATIC)) == 0 || method.getReturnType() != cls) {
				continue;
			}
			return () -> {
				try {
					return (T) method.invoke(null);
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw UncheckedException.of(e);
				}
			};
		}
		return Arrays.stream(cls.getConstructors())
				.filter(c -> c.getParameterTypes().length == 0)
				.findAny()
				.<Supplier<T>>map(c -> () -> {
					try {
						return (T) c.newInstance();
					} catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
						throw UncheckedException.of(e);
					}
				})
				.orElse(null);
	}

	public static boolean canBeCreated(Class<?> cls, String... factoryMethodNames) {
		return getConstructorOrFactory(cls, factoryMethodNames) != null;
	}

	public static <T> @Nullable T tryToCreateInstanceWithFactoryMethods(Class<T> cls, String... factoryMethodNames) {
		try {
			Supplier<T> supplier = getConstructorOrFactory(cls, factoryMethodNames);
			return supplier != null ? supplier.get() : null;
		} catch (UncheckedException u) {
			return null;
		}
	}

	public static boolean isClassPresent(String fullClassName) {
		return isClassPresent(fullClassName, ReflectionUtils.class.getClassLoader());
	}

	public static boolean isClassPresent(String fullClassName, ClassLoader classLoader) {
		try {
			Class.forName(fullClassName, false, classLoader);
			return true;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	/**
	 * Returns a list containing {@code Method} objects reflecting all the
	 * methods of the class or interface represented by this {@code
	 * Class} object, including those declared by the class or interface and
	 * those from superclasses and superinterfaces.
	 */
	public static List<Method> getAllMethods(Class<?> cls) {
		Set<MethodSignature> methodSignatures = new LinkedHashSet<>();
		walkClassHierarchy(cls, aClass -> {
			Arrays.stream(aClass.getDeclaredMethods())
					.filter(method -> !method.isBridge() && !method.isSynthetic())
					.map(MethodSignature::new)
					.forEach(methodSignatures::add);
			return Optional.empty();
		});
		return methodSignatures.stream()
				.map(MethodSignature::getMethod)
				.collect(toList());
	}

	public static <A extends Annotation> Optional<A> deepFindAnnotation(Class<?> aClass, Class<A> annotation) {
		return walkClassHierarchy(aClass, clazz -> Optional.ofNullable(clazz.getAnnotation(annotation)));
	}

	public static <T> Optional<T> walkClassHierarchy(@NotNull Class<?> aClass, @NotNull Function<Class<?>, Optional<T>> finder) {
		return walkClassHierarchy(aClass, finder, new HashSet<>());
	}

	private static <T> Optional<T> walkClassHierarchy(@Nullable Class<?> aClass, @NotNull Function<Class<?>, Optional<T>> finder, @NotNull Set<Class<?>> visited) {
		if (aClass == null || !visited.add(aClass)) return Optional.empty();
		Optional<T> maybeResult = finder.apply(aClass);
		if (maybeResult.isPresent()) return maybeResult;
		for (Class<?> iface : aClass.getInterfaces()) {
			maybeResult = walkClassHierarchy(iface, finder, visited);
			if (maybeResult.isPresent()) return maybeResult;
		}
		return walkClassHierarchy(aClass.getSuperclass(), finder, visited);
	}

	/**
	 * Builds string representation of annotation with its elements.
	 * The string looks differently depending on the number of elements, that an annotation has.
	 * If annotation has no elements, string looks like this : "AnnotationName"
	 * If annotation has a single element with the name "value", string looks like this : "AnnotationName(someValue)"
	 * If annotation has one or more custom elements, string looks like this : "(key1=value1,key2=value2)"
	 */
	public static String getAnnotationString(Annotation annotation) throws ReflectiveOperationException {
		Class<? extends Annotation> annotationType = annotation.annotationType();
		StringBuilder annotationString = new StringBuilder();
		Method[] annotationElements = filterNonEmptyElements(annotation);
		if (annotationElements.length == 0) {
			// annotation without elements
			annotationString.append(annotationType.getSimpleName());
			return annotationString.toString();
		}
		if (annotationElements.length == 1 && annotationElements[0].getName().equals("value")) {
			// annotation with single element which has name "value"
			annotationString.append(annotationType.getSimpleName());
			Object value = fetchAnnotationElementValue(annotation, annotationElements[0]);
			annotationString.append('(').append(value).append(')');
			return annotationString.toString();
		}
		// annotation with one or more custom elements
		annotationString.append('(');
		for (Method annotationParameter : annotationElements) {
			Object value = fetchAnnotationElementValue(annotation, annotationParameter);
			String nameKey = annotationParameter.getName();
			String nameValue = value.toString();
			annotationString.append(nameKey).append('=').append(nameValue).append(',');
		}

		assert annotationString.substring(annotationString.length() - 1).equals(",");

		annotationString = new StringBuilder(annotationString.substring(0, annotationString.length() - 1));
		annotationString.append(')');
		return annotationString.toString();
	}

	private static Method[] filterNonEmptyElements(Annotation annotation) throws ReflectiveOperationException {
		List<Method> filtered = new ArrayList<>();
		for (Method method : annotation.annotationType().getDeclaredMethods()) {
			Object elementValue = fetchAnnotationElementValue(annotation, method);
			if (elementValue instanceof String) {
				String stringValue = (String) elementValue;
				if (stringValue.length() == 0) {
					// skip this element, because it is empty string
					continue;
				}
			}
			filtered.add(method);
		}
		return filtered.toArray(new Method[0]);
	}

	/**
	 * Returns values if it is not null, otherwise throws exception
	 */
	public static Object fetchAnnotationElementValue(Annotation annotation, Method element) throws ReflectiveOperationException {
		Object value = element.invoke(annotation);
		if (value == null) {
			String errorMsg = "@" + annotation.annotationType().getName() + "." +
					element.getName() + "() returned null";
			throw new NullPointerException(errorMsg);
		}
		return value;
	}

	@SuppressWarnings("EqualsWhichDoesntCheckParameterClass") // other Object is always MethodSignature
	private static final class MethodSignature {
		final Method method;

		MethodSignature(Method method) {
			this.method = method;
		}

		public Method getMethod() {
			return method;
		}

		@Override
		public boolean equals(Object obj) {
			Method otherMethod = ((MethodSignature) obj).method;
			return method.getName().equals(otherMethod.getName()) &&
					Arrays.equals(method.getParameterTypes(), otherMethod.getParameterTypes());
		}

		@Override
		public int hashCode() {
			return method.getName().hashCode() ^ Arrays.hashCode(method.getParameterTypes());
		}
	}
}
