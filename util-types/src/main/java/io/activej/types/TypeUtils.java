package io.activej.types;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.*;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;

public class TypeUtils {
	public static final Type[] NO_TYPES = new Type[0];
	public static final WildcardType WILDCARD_TYPE_ANY = new WildcardTypeImpl(new Type[]{Object.class}, new Type[0]);

	public static Class<?> getRawClass(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			return (Class<?>) parameterizedType.getRawType();
		} else if (type instanceof WildcardType) {
			WildcardType wildcardType = (WildcardType) type;
			Type[] upperBounds = wildcardType.getUpperBounds();
			if (upperBounds.length == 1 && wildcardType.getLowerBounds().length == 0) {
				return getRawClass(upperBounds[0]);
			}
		}
		throw new IllegalArgumentException("Unsupported type: " + type);
	}

	public static Type[] getActualTypeArguments(Type type) {
		Type[] typeArguments;

		if (type instanceof Class) {
			Class<?> clazz = (Class<?>) type;
			TypeVariable<? extends Class<?>>[] typeParameters = clazz.getTypeParameters();
			if (typeParameters.length == 0) return NO_TYPES;
			typeArguments = new Type[typeParameters.length];
			Arrays.fill(typeArguments, WILDCARD_TYPE_ANY);
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			typeArguments = parameterizedType.getActualTypeArguments();
		} else {
			throw new IllegalArgumentException("Unsupported type: " + type);
		}
		return typeArguments;
	}

	public static Map<TypeVariable<?>, Type> getTypeBindings(Type type) {
		Class<?> typeClazz = getRawClass(type);
		Type[] typeArguments = getActualTypeArguments(type);
		if (typeArguments.length == 0) return emptyMap();
		Map<TypeVariable<?>, Type> map = new LinkedHashMap<>();
		TypeVariable<?>[] typeVariables = typeClazz.getTypeParameters();
		for (int i = 0; i < typeVariables.length; i++) {
			map.put(typeVariables[i], typeArguments[i]);
		}
		return map;
	}

	@NotNull
	public static Type bind(Type type, Function<TypeVariable<?>, Type> bindings) {
		if (type instanceof Class) return type;
		if (type instanceof TypeVariable) {
			TypeVariable<?> typeVariable = (TypeVariable<?>) type;
			Type actualType = bindings.apply(typeVariable);
			if (actualType == null) throw new IllegalArgumentException("Type not found: " + type);
			return actualType;
		}
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			Class<?> clazz = getRawClass(type);
			Type[] typeArguments = parameterizedType.getActualTypeArguments();
			Type[] typeArguments2 = new Type[typeArguments.length];
			for (int i = 0; i < typeArguments.length; i++) {
				typeArguments2[i] = bind(typeArguments[i], bindings);
			}
			return new ParameterizedTypeImpl(clazz, typeArguments2);
		}
		if (type instanceof GenericArrayType) {
			GenericArrayType genericArrayType = (GenericArrayType) type;
			Type componentType = genericArrayType.getGenericComponentType();
			return new GenericArrayTypeImpl(bind(componentType, bindings));
		}
		if (type instanceof WildcardType) {
			WildcardType wildcardType = (WildcardType) type;
			Type[] upperBounds = wildcardType.getUpperBounds();
			Type[] upperBounds2 = new Type[upperBounds.length];
			for (int i = 0; i < upperBounds.length; i++) {
				upperBounds2[i] = bind(upperBounds[i], bindings);
			}
			Type[] lowerBounds = wildcardType.getLowerBounds();
			Type[] lowerBounds2 = new Type[lowerBounds.length];
			for (int i = 0; i < lowerBounds.length; i++) {
				lowerBounds2[i] = bind(lowerBounds[i], bindings);
			}
			return new WildcardTypeImpl(upperBounds2, lowerBounds2);
		}
		throw new IllegalArgumentException("Unsupported type: " + type);
	}

	static final class ParameterizedTypeImpl implements ParameterizedType {
		private final Class<?> rawType;
		private final Type[] actualTypeArguments;

		ParameterizedTypeImpl(Class<?> rawType, Type[] actualTypeArguments) {
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

	static class WildcardTypeImpl implements WildcardType {
		private final Type[] upperBounds;
		private final Type[] lowerBounds;

		WildcardTypeImpl(Type[] upperBounds, Type[] lowerBounds) {
			this.upperBounds = upperBounds;
			this.lowerBounds = lowerBounds;
		}

		@Override
		public Type[] getUpperBounds() {
			return upperBounds;
		}

		@Override
		public Type[] getLowerBounds() {
			return lowerBounds;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			WildcardTypeImpl type = (WildcardTypeImpl) o;
			if (!Arrays.equals(upperBounds, type.upperBounds)) return false;
			if (!Arrays.equals(lowerBounds, type.lowerBounds)) return false;
			return true;
		}

		@Override
		public int hashCode() {
			int result = 0;
			result = 31 * result + Arrays.hashCode(upperBounds);
			result = 31 * result + Arrays.hashCode(lowerBounds);
			return result;
		}
	}

	static final class GenericArrayTypeImpl implements GenericArrayType {
		private final Type genericComponentType;

		GenericArrayTypeImpl(Type genericComponentType) {
			this.genericComponentType = genericComponentType;
		}

		@Override
		public Type getGenericComponentType() {
			return genericComponentType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			GenericArrayTypeImpl type = (GenericArrayTypeImpl) o;
			return genericComponentType.equals(type.genericComponentType);
		}

		@Override
		public int hashCode() {
			return genericComponentType.hashCode();
		}
	}
}
