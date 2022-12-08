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

import io.activej.types.Types;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.*;
import java.util.*;

import static io.activej.types.IsAssignableUtils.isAssignable;

/**
 * This class contains reflection utilities to work with Java types.
 * Its main use is for method {@link io.activej.types.Types#parameterizedType Types.parameterized}.
 * However, just like with {@link ReflectionUtils}, other type utility
 * methods are pretty clean too, so they are left public.
 */
public final class TypeUtils {

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
		Class<?> rawType = Types.getRawType(type);

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
			if (pattern instanceof WildcardType wildcard) {
				return Arrays.stream(wildcard.getUpperBounds()).allMatch(bound -> isInheritedFrom(strict, bound, dejaVu))
						&& Arrays.stream(wildcard.getLowerBounds()).allMatch(bound -> isInheritedFrom(bound, strict, dejaVu));
			}
			if (pattern instanceof TypeVariable<?> typevar) {
				return Arrays.stream(typevar.getBounds()).allMatch(bound -> isInheritedFrom(strict, bound, dejaVu));
			}
			if (strict instanceof GenericArrayType && pattern instanceof GenericArrayType) {
				return matches(((GenericArrayType) strict).getGenericComponentType(), ((GenericArrayType) pattern).getGenericComponentType(), dejaVu);
			}
			if (!(strict instanceof ParameterizedType parameterizedStrict) || !(pattern instanceof ParameterizedType parameterizedPattern)) {
				return false;
			}

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
		if (!(type instanceof ParameterizedType parameterized)) {
			return false;
		}
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
		if (!(pattern instanceof ParameterizedType parameterizedPattern) || !(real instanceof ParameterizedType parameterizedReal)) {
			return;
		}
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

	public static Type simplifyType(Type original) {
		if (original instanceof Class) {
			return original;
		}

		if (original instanceof GenericArrayType) {
			Type componentType = ((GenericArrayType) original).getGenericComponentType();
			Type repackedComponentType = simplifyType(componentType);
			if (componentType != repackedComponentType) {
				return Types.genericArrayType(repackedComponentType);
			}
			return original;
		}

		if (original instanceof ParameterizedType parameterizedType) {
			Type[] typeArguments = parameterizedType.getActualTypeArguments();
			Type[] repackedTypeArguments = simplifyTypes(typeArguments);

			if (isAllObjects(repackedTypeArguments)) {
				return parameterizedType.getRawType();
			}

			if (typeArguments != repackedTypeArguments) {
				return Types.parameterizedType(
						parameterizedType.getOwnerType(),
						parameterizedType.getRawType(),
						repackedTypeArguments
				);
			}
			return original;
		}

		if (original instanceof TypeVariable) {
			throw new IllegalArgumentException("Key should not contain a type variable: " + original);
		}

		if (original instanceof WildcardType wildcardType) {
			Type[] upperBounds = wildcardType.getUpperBounds();
			if (upperBounds.length == 1) {
				Type upperBound = upperBounds[0];
				if (upperBound != Object.class) {
					return simplifyType(upperBound);
				}
			} else if (upperBounds.length > 1) {
				throw new IllegalArgumentException("Multiple upper bounds not supported: " + original);
			}

			Type[] lowerBounds = wildcardType.getLowerBounds();
			if (lowerBounds.length == 1) {
				return simplifyType(lowerBounds[0]);
			} else if (lowerBounds.length > 1) {
				throw new IllegalArgumentException("Multiple lower bounds not supported: " + original);
			}
			return Object.class;
		}

		return original;
	}

	private static Type[] simplifyTypes(Type[] original) {
		int length = original.length;
		for (int i = 0; i < length; i++) {
			Type typeArgument = original[i];
			Type repackTypeArgument = simplifyType(typeArgument);
			if (repackTypeArgument != typeArgument) {
				Type[] repackedTypeArguments = new Type[length];
				System.arraycopy(original, 0, repackedTypeArguments, 0, i);
				repackedTypeArguments[i++] = repackTypeArgument;
				for (; i < length; i++) {
					repackedTypeArguments[i] = simplifyType(original[i]);
				}
				return repackedTypeArguments;
			}
		}
		return original;
	}

	private static boolean isAllObjects(Type[] types) {
		for (Type type : types) {
			if (type != Object.class) return false;
		}
		return true;
	}

	public static @Nullable Type match(Type type, Collection<Type> patterns) {
		Type best = null;
		for (Type found : patterns) {
			if (isAssignable(found, type)) {
				if (best == null || isAssignable(best, found)) {
					if (best != null && !best.equals(found) && isAssignable(found, best)) {
						throw new IllegalArgumentException("Conflicting types: " + type + " " + best);
					}
					best = found;
				}
			}
		}
		return best;
	}
}
