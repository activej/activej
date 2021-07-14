package io.activej.inject.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public class TypeUtils {
	public static final Type[] NO_TYPES = new Type[0];

	public static Class<?> getRawClass(Type type) {
		Class<?> typeClazz;

		if (type instanceof Class) {
			typeClazz = (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			typeClazz = (Class<?>) parameterizedType.getRawType();
		} else {
			throw new IllegalArgumentException("Unsupported type: " + type);
		}
		return typeClazz;
	}

	public static Type[] getTypeArguments(Type type) {
		Type[] typeArguments;

		if (type instanceof Class) {
			typeArguments = NO_TYPES;
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			typeArguments = parameterizedType.getActualTypeArguments();
		} else {
			throw new IllegalArgumentException("Unsupported type: " + type);
		}
		return typeArguments;
	}

	public static Map<TypeVariable<?>, Type> getTypeParameters(Type type) {
		Class<?> typeClazz = getRawClass(type);
		Type[] typeArguments = getTypeArguments(type);
		Map<TypeVariable<?>, Type> map = new LinkedHashMap<>();
		TypeVariable<?>[] typeVariables = typeClazz.getTypeParameters();
		for (int i = 0; i < typeVariables.length; i++) {
			map.put(typeVariables[i], typeArguments[i]);
		}
		return map;
	}

	@NotNull
	public static Type bind(Type type, Function<TypeVariable<?>, Type> bindings) {
		if (type instanceof TypeVariable) {
			Type actualType = bindings.apply((TypeVariable<?>) type);
			if (actualType == null) throw new IllegalArgumentException("Type not found: " + type);
			return actualType;
		}
		if (type instanceof ParameterizedType) {
			return typeOf(getRawClass(type),
					Arrays.stream(((ParameterizedType) type).getActualTypeArguments())
							.map(argument -> bind(argument, bindings))
							.toArray(Type[]::new));
		}
		return type;
	}

	@NotNull
	public static Type typeOf(Class<?> clazz, Type[] typeArguments) {
		if (typeArguments.length == 0) return clazz;
		return new ParameterizedTypeImpl(clazz, typeArguments);
	}

	public static boolean isAssignable(@NotNull Type to, @NotNull Type from) {
		// shortcut
		if (to instanceof Class && from instanceof Class) return ((Class<?>) to).isAssignableFrom((Class<?>) from);
		return isAssignable(to, from, false);
	}

	public static boolean isAssignable(Type to, Type from, boolean strict) {
		if (to instanceof WildcardType || from instanceof WildcardType) {
			Type[] toUppers, toLowers;
			if (to instanceof WildcardType) {
				WildcardType wildcardTo = (WildcardType) to;
				toUppers = wildcardTo.getUpperBounds();
				toLowers = wildcardTo.getLowerBounds();
			} else {
				toUppers = new Type[]{to};
				toLowers = strict ? toUppers : NO_TYPES;
			}

			Type[] fromUppers, fromLowers;
			if (from instanceof WildcardType) {
				WildcardType wildcardTo = (WildcardType) from;
				fromUppers = wildcardTo.getUpperBounds();
				fromLowers = wildcardTo.getLowerBounds();
			} else {
				fromUppers = new Type[]{from};
				fromLowers = strict ? fromUppers : NO_TYPES;
			}

			for (Type toUpper : toUppers) {
				for (Type fromUpper : fromUppers) {
					if (!isAssignable(toUpper, fromUpper, false)) return false;
				}
			}
			if (toLowers.length == 0) return true;
			if (fromLowers.length == 0) return false;
			for (Type toLower : toLowers) {
				for (Type fromLower : fromLowers) {
					if (!isAssignable(fromLower, toLower, false)) return false;
				}
			}
			return true;
		}
		Class<?> toRawClazz = getRawClass(to);
		Type[] toTypeArguments = getTypeArguments(to);
		if (!strict && toRawClazz == Object.class) return true;
		return isAssignable(toRawClazz, toTypeArguments, from, strict);
	}

	private static boolean isAssignable(Class<?> toRawClazz, Type[] toTypeArguments, Type from, boolean strict) {
		Class<?> fromRawClazz = getRawClass(from);
		if (strict && !toRawClazz.equals(fromRawClazz)) return false;
		if (!strict && !toRawClazz.isAssignableFrom(fromRawClazz)) return false;
		if (toRawClazz.isArray()) return true;
		Type[] fromTypeArguments = getTypeArguments(from);
		if (toRawClazz == fromRawClazz) {
			if (toTypeArguments.length > fromTypeArguments.length) return false;
			for (int i = 0; i < toTypeArguments.length; i++) {
				if (!isAssignable(toTypeArguments[i], fromTypeArguments[i], true)) return false;
			}
			return true;
		}
		Map<TypeVariable<?>, Type> typeParameters = getTypeParameters(from);
		for (Type anInterface : fromRawClazz.getGenericInterfaces()) {
			if (isAssignable(toRawClazz, toTypeArguments, bind(anInterface, typeParameters::get), strict)) {
				return true;
			}
		}
		return isAssignable(toRawClazz, toTypeArguments, bind(fromRawClazz.getGenericSuperclass(), typeParameters::get), strict);
	}

	@Nullable
	public static Type match(Type type, Collection<Type> patterns) {
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

	private static final class ParameterizedTypeImpl implements ParameterizedType {
		private final Class<?> rawType;
		private final Type[] actualTypeArguments;

		private ParameterizedTypeImpl(Class<?> rawType, Type[] actualTypeArguments) {
			this.rawType = rawType;
			this.actualTypeArguments = actualTypeArguments;
		}

		@NotNull
		@Override
		public Type getRawType() {
			return rawType;
		}

		@NotNull
		@Override
		public Type[] getActualTypeArguments() {
			return actualTypeArguments;
		}

		@Nullable
		@Override
		public Type getOwnerType() {
			return null;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ParameterizedTypeImpl that = (ParameterizedTypeImpl) o;
			if (!rawType.equals(that.rawType)) return false;
			return Arrays.equals(actualTypeArguments, that.actualTypeArguments);
		}

		@Override
		public int hashCode() {
			int result = rawType.hashCode();
			result = 31 * result + Arrays.hashCode(actualTypeArguments);
			return result;
		}

		@Override
		public String toString() {
			return rawType.getCanonicalName() +
					Arrays.stream(actualTypeArguments).map(Objects::toString).collect(joining(", ", "<", ">"));
		}

	}

}
