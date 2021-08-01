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

import io.activej.types.TypeUtils;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static io.activej.types.TypeUtils.bind;
import static java.util.Map.Entry;
import static java.util.stream.Collectors.toMap;

/**
 * This class contains reflection utilities to work with Java types.
 * Its main use is for method {@link TypeUtils#parameterizedType Types.parameterized}.
 * However, just like with {@link ReflectionUtils}, other type utility
 * methods are pretty clean too, so they are left public.
 */
public final class Types {

	public static boolean isInheritedFrom(Type type, Type from) {
		return isInheritedFrom(type, from, new HashMap<>());
	}

	public static boolean isInheritedFrom(Type type, Type from, Map<Type, Type> dejaVu) {
		if (from == Object.class) {
			return true;
		}
		if (matches(type, from, dejaVu) || matches(from, type, dejaVu)) {
			return true;
		}
		if (!(type instanceof Class || type instanceof ParameterizedType || type instanceof GenericArrayType)) {
			return false;
		}
		Class<?> rawType = TypeUtils.getRawType(type);

		Type superclass = rawType.getGenericSuperclass();
		if (superclass != null && isInheritedFrom(superclass, from, dejaVu)) {
			return true;
		}
		return Arrays.stream(rawType.getGenericInterfaces()).anyMatch(iface -> isInheritedFrom(iface, from, dejaVu));
	}

	public static boolean matches(Type strict, Type pattern) {
		return matches(strict, pattern, new HashMap<>());
	}

	private static boolean matches(Type strict, Type pattern, Map<Type, Type> dejaVu) {
		if (strict.equals(pattern) || dejaVu.get(strict) == pattern) {
			return true;
		}
		dejaVu.put(strict, pattern);
		try {
			if (pattern instanceof WildcardType) {
				WildcardType wildcard = (WildcardType) pattern;
				return Arrays.stream(wildcard.getUpperBounds()).allMatch(bound -> isInheritedFrom(strict, bound, dejaVu))
						&& Arrays.stream(wildcard.getLowerBounds()).allMatch(bound -> isInheritedFrom(bound, strict, dejaVu));
			}
			if (pattern instanceof TypeVariable) {
				TypeVariable<?> typevar = (TypeVariable<?>) pattern;
				return Arrays.stream(typevar.getBounds()).allMatch(bound -> isInheritedFrom(strict, bound, dejaVu));
			}
			if (strict instanceof GenericArrayType && pattern instanceof GenericArrayType) {
				return matches(((GenericArrayType) strict).getGenericComponentType(), ((GenericArrayType) pattern).getGenericComponentType(), dejaVu);
			}
			if (!(strict instanceof ParameterizedType) || !(pattern instanceof ParameterizedType)) {
				return false;
			}
			ParameterizedType parameterizedStrict = (ParameterizedType) strict;
			ParameterizedType parameterizedPattern = (ParameterizedType) pattern;

			if (parameterizedPattern.getOwnerType() != null) {
				if (parameterizedStrict.getOwnerType() == null) {
					return false;
				}
				if (!matches(parameterizedPattern.getOwnerType(), parameterizedStrict.getOwnerType(), dejaVu)) {
					return false;
				}
			}
			if (!matches(parameterizedPattern.getRawType(), parameterizedStrict.getRawType(), dejaVu)) {
				return false;
			}

			Type[] strictParams = parameterizedStrict.getActualTypeArguments();
			Type[] patternParams = parameterizedPattern.getActualTypeArguments();
			if (strictParams.length != patternParams.length) {
				return false;
			}
			for (int i = 0; i < strictParams.length; i++) {
				if (!matches(strictParams[i], patternParams[i], dejaVu)) {
					return false;
				}
			}
			return true;
		} finally {
			dejaVu.remove(strict);
		}
	}

	public static boolean contains(Type type, Type sub) {
		if (type.equals(sub)) {
			return true;
		}
		if (type instanceof GenericArrayType) {
			return contains(((GenericArrayType) type).getGenericComponentType(), sub);
		}
		if (!(type instanceof ParameterizedType)) {
			return false;
		}
		ParameterizedType parameterized = (ParameterizedType) type;
		if (contains(parameterized.getRawType(), sub)) {
			return true;
		}
		if (parameterized.getOwnerType() != null && contains(parameterized.getOwnerType(), sub)) {
			return true;
		}
		return Arrays.stream(parameterized.getActualTypeArguments())
				.anyMatch(argument -> contains(argument, sub));
	}

	// pattern = Map<K, List<V>>
	// real    = Map<String, List<Integer>>
	//
	// result  = {K -> String, V -> Integer}
	public static Map<TypeVariable<?>, Type> extractMatchingGenerics(Type pattern, Type real) {
		Map<TypeVariable<?>, Type> result = new HashMap<>();
		extractMatchingGenerics(pattern, real, result);
		return result;
	}

	private static void extractMatchingGenerics(Type pattern, Type real, Map<TypeVariable<?>, Type> result) {
		if (pattern instanceof TypeVariable) {
			result.put((TypeVariable<?>) pattern, real);
			return;
		}
		if (pattern.equals(real)) {
			return;
		}
		if (pattern instanceof GenericArrayType && real instanceof GenericArrayType) {
			extractMatchingGenerics(((GenericArrayType) pattern).getGenericComponentType(), ((GenericArrayType) real).getGenericComponentType(), result);
			return;
		}
		if (!(pattern instanceof ParameterizedType) || !(real instanceof ParameterizedType)) {
			return;
		}
		ParameterizedType parameterizedPattern = (ParameterizedType) pattern;
		ParameterizedType parameterizedReal = (ParameterizedType) real;
		if (!parameterizedPattern.getRawType().equals(parameterizedReal.getRawType())) {
			return;
		}
		extractMatchingGenerics(parameterizedPattern.getRawType(), parameterizedReal.getRawType(), result);
		if (!Objects.equals(parameterizedPattern.getOwnerType(), parameterizedReal.getOwnerType())) {
			return;
		}
		if (parameterizedPattern.getOwnerType() != null) {
			extractMatchingGenerics(parameterizedPattern.getOwnerType(), parameterizedReal.getOwnerType(), result);
		}
		Type[] patternTypeArgs = parameterizedPattern.getActualTypeArguments();
		Type[] realTypeArgs = parameterizedReal.getActualTypeArguments();
		if (patternTypeArgs.length != realTypeArgs.length) {
			return;
		}
		for (int i = 0; i < patternTypeArgs.length; i++) {
			extractMatchingGenerics(patternTypeArgs[i], realTypeArgs[i], result);
		}
	}

	private static final Map<Type, Map<TypeVariable<?>, Type>> genericMappingCache = new ConcurrentHashMap<>();

	public static Map<TypeVariable<?>, Type> getGenericTypeMapping(Type container) {
		return getGenericTypeMapping(container, null);
	}

	public static Map<TypeVariable<?>, Type> getGenericTypeMapping(Type container, @Nullable Object containerInstance) {
		return genericMappingCache.computeIfAbsent(
				containerInstance != null ? containerInstance.getClass() : container,
				type -> {
					Map<TypeVariable<?>, @Nullable Type> mapping = new HashMap<>();
					getGenericTypeMappingImpl(type, mapping);
					return mapping.entrySet().stream()
							.filter(e -> e.getValue() != null)
							.collect(toMap(Entry::getKey, Entry::getValue));
				});
	}

	private static void getGenericTypeMappingImpl(Type type, Map<TypeVariable<?>, @Nullable Type> mapping) {
		Class<?> cls = TypeUtils.getRawType(type);

		if (type instanceof ParameterizedType) {
			Type[] typeArguments = ((ParameterizedType) type).getActualTypeArguments();
			if (typeArguments.length != 0) {
				TypeVariable<? extends Class<?>>[] typeVariables = cls.getTypeParameters();
				for (int i = 0; i < typeArguments.length; i++) {
					Type typeArgument = typeArguments[i];
					mapping.put(typeVariables[i], typeArgument instanceof TypeVariable ? mapping.get(typeArgument) : typeArgument);
				}
			}
		}

		Stream.concat(Stream.of(cls.getGenericSuperclass()).filter(Objects::nonNull), Arrays.stream(cls.getGenericInterfaces()))
				.forEach(supertype -> getGenericTypeMappingImpl(supertype, mapping));
	}

	public static Type resolveTypeVariables(Type type, Type container, @Nullable Object containerInstance) {
		return bind(type, getGenericTypeMapping(container, containerInstance)::get);
	}

}
