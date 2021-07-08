package io.activej.serializer.reflection.scanner;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public class TypeUtils {
	public static final Type[] NO_TYPES = new Type[0];
	public static final Annotation[] NO_ANNOTATIONS = new Annotation[0];
	public static final AnnotatedType[] NO_ANNOTATED_TYPES = new AnnotatedType[0];
	public static final WildcardType WILDCARD_TYPE_ANY = new WildcardTypeImpl(new Type[]{Object.class}, new Type[0]);

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

	public static Class<?> getRawClass(AnnotatedType type) {
		return getRawClass(type.getType());
	}

	public static Type[] getTypeArguments(Type type) {
		Type[] typeArguments;

		if (type instanceof Class) {
			typeArguments = new Type[((Class<?>) type).getTypeParameters().length];
			Arrays.fill(typeArguments, WILDCARD_TYPE_ANY);
		} else if (type instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) type;
			typeArguments = parameterizedType.getActualTypeArguments();
		} else {
			throw new IllegalArgumentException("Unsupported type: " + type);
		}
		return typeArguments;
	}

	public static AnnotatedType[] getTypeArguments(AnnotatedType annotatedType) {
		if (annotatedType instanceof AnnotatedParameterizedType) {
			return ((AnnotatedParameterizedType) annotatedType).getAnnotatedActualTypeArguments();
		}
		if (annotatedType instanceof AnnotatedArrayType) {
			AnnotatedArrayType annotatedArrayType = (AnnotatedArrayType) annotatedType;
			return new AnnotatedType[]{annotatedArrayType.getAnnotatedGenericComponentType()};
		}
		return NO_ANNOTATED_TYPES;
	}

	public static Map<TypeVariable<?>, Type> getTypeBindings(Type type) {
		Class<?> typeClazz = getRawClass(type);
		Type[] typeArguments = getTypeArguments(type);
		if (typeArguments.length == 0) return Collections.emptyMap();
		Map<TypeVariable<?>, Type> map = new LinkedHashMap<>();
		TypeVariable<?>[] typeVariables = typeClazz.getTypeParameters();
		for (int i = 0; i < typeVariables.length; i++) {
			map.put(typeVariables[i], typeArguments[i]);
		}
		return map;
	}

	public static Map<TypeVariable<?>, AnnotatedType> getTypeBindings(AnnotatedType type) {
		Class<?> typeClazz = getRawClass(type);
		AnnotatedType[] typeArguments = getTypeArguments(type);
		if (typeArguments.length == 0) return Collections.emptyMap();
		Map<TypeVariable<?>, AnnotatedType> map = new LinkedHashMap<>();
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

	public static AnnotatedType bind(AnnotatedType annotatedType, Function<TypeVariable<?>, AnnotatedType> bindings) {
		return bind(annotatedType, bindings, TypeUtils::overrideAnnotations);
	}

	@NotNull
	public static AnnotatedType bind(AnnotatedType annotatedType, Function<TypeVariable<?>, AnnotatedType> bindings,
			BiFunction<Annotation[], Annotation[], Annotation[]> annotationCombinerFn) {
		Annotation[] annotations = annotatedType.getAnnotations();
		if (annotatedType instanceof AnnotatedTypeVariable) {
			AnnotatedType actualType = bindings.apply((TypeVariable<?>) annotatedType.getType());
			if (actualType == null) throw new IllegalArgumentException("Type not found: " + annotatedType);
			if (annotations.length == 0) return actualType;
			return annotatedTypeOf(actualType.getType(), annotationCombinerFn.apply(actualType.getAnnotations(), annotations));
		}
		if (annotatedType instanceof AnnotatedParameterizedType) {
			AnnotatedType[] annotatedTypes = Arrays.stream(((AnnotatedParameterizedType) annotatedType).getAnnotatedActualTypeArguments())
					.map(actualType -> bind(actualType, bindings, annotationCombinerFn))
					.toArray(AnnotatedType[]::new);
			return new AnnotatedParameterizedTypeImpl(
					new ParameterizedTypeImpl((Class<?>) ((ParameterizedType) annotatedType.getType()).getRawType(),
							Arrays.stream(annotatedTypes).map(AnnotatedType::getType).toArray(Type[]::new)),
					annotations, annotatedTypes);
		}
		return annotatedType;
	}

	public static Annotation[] appendAnnotations(Annotation[] oldAnnotations, Annotation[] annotations) {
		if (oldAnnotations.length == 0) return annotations;
		if (annotations.length == 0) return oldAnnotations;
		Annotation[] result = Arrays.copyOf(oldAnnotations, annotations.length + oldAnnotations.length);
		System.arraycopy(annotations, 0, result, oldAnnotations.length, annotations.length);
		return result;
	}

	public static Annotation[] overrideAnnotations(Annotation[] oldAnnotations, Annotation[] annotations) {
		if (oldAnnotations.length == 0) return annotations;
		if (annotations.length == 0) return oldAnnotations;
		Annotation[] result = Arrays.copyOf(annotations, annotations.length + oldAnnotations.length);
		int idx = annotations.length;
		L:
		//noinspection ForLoopReplaceableByForEach
		for (int i = 0; i < oldAnnotations.length; i++) {
			Annotation oldAnnotation = oldAnnotations[i];
			Class<? extends Annotation> oldAnnotationClass = oldAnnotation.getClass();
			for (int j = 0; j < annotations.length; j++) {
				if (result[j].getClass() == oldAnnotationClass) {
					continue L;
				}
			}
			result[idx++] = oldAnnotation;
		}
		return idx == result.length ? result : Arrays.copyOf(result, idx);
	}

	@NotNull
	public static AnnotatedType annotatedTypeOf(Type type) {
		return annotatedTypeOf(type, NO_ANNOTATIONS);
	}

	@NotNull
	public static AnnotatedType annotatedTypeOf(Type type, Annotation[] annotations) {
		if (type instanceof Class) {
			return new AnnotatedTypeImpl(type, annotations);
		}
		if (type instanceof ParameterizedType) {
			return new AnnotatedParameterizedTypeImpl((ParameterizedType) type, annotations,
					Arrays.stream(((ParameterizedType) type).getActualTypeArguments()).map(TypeUtils::annotatedTypeOf).toArray(AnnotatedType[]::new));
		}
		if (type instanceof WildcardType) {
			return new AnnotatedWildcardTypeImpl((WildcardType) type, annotations,
					Arrays.stream(((WildcardType) type).getUpperBounds()).map(TypeUtils::annotatedTypeOf).toArray(AnnotatedType[]::new),
					Arrays.stream(((WildcardType) type).getLowerBounds()).map(TypeUtils::annotatedTypeOf).toArray(AnnotatedType[]::new));
		}
		throw new IllegalArgumentException("Type is not supported: " + type);
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
		if (!strict && to instanceof Class) {
			return ((Class<?>) to).isAssignableFrom(getRawClass(from));
		}
		Class<?> toRawClazz = getRawClass(to);
		Type[] toTypeArguments = getTypeArguments(to);
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
		Map<TypeVariable<?>, Type> typeBindings = getTypeBindings(from);
		for (Type anInterface : fromRawClazz.getGenericInterfaces()) {
			if (isAssignable(toRawClazz, toTypeArguments, bind(anInterface, typeBindings::get), strict)) {
				return true;
			}
		}
		Type superclass = fromRawClazz.getGenericSuperclass();
		return superclass != null && isAssignable(toRawClazz, toTypeArguments, bind(superclass, typeBindings::get), strict);
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

	private static class WildcardTypeImpl implements WildcardType {
		private final Type[] upperBounds;
		private final Type[] lowerBounds;

		private WildcardTypeImpl(Type[] upperBounds, Type[] lowerBounds) {
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

	private static class AnnotatedTypeImpl implements AnnotatedType {
		protected final Type type;
		protected final Annotation[] annotations;

		public AnnotatedTypeImpl(Type type, Annotation[] annotations) {
			this.type = type;
			this.annotations = annotations;
		}

		@Override
		public Type getType() {
			return type;
		}

		public AnnotatedType getAnnotatedOwnerType() {
			return null;
		}

		@SuppressWarnings({"unchecked"})
		@Override
		public <T extends Annotation> T getAnnotation(@NotNull Class<T> annotationClass) {
			return (T) Arrays.stream(annotations).filter(a -> a.getClass() == annotationClass).findFirst().orElse(null);
		}

		@Override
		public Annotation[] getAnnotations() {
			return annotations;
		}

		@Override
		public Annotation[] getDeclaredAnnotations() {
			return annotations;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AnnotatedTypeImpl type1 = (AnnotatedTypeImpl) o;
			if (!type.equals(type1.type)) return false;
			return Arrays.equals(annotations, type1.annotations);
		}

		@Override
		public int hashCode() {
			int result = type.hashCode();
			result = 31 * result + Arrays.hashCode(annotations);
			return result;
		}

		@Override
		public String toString() {
			return "" +
					(annotations.length == 0 ? "" :
							Arrays.stream(annotations).map(Objects::toString).collect(joining(", ", "", " "))) +
					getRawClass(type).getCanonicalName();
		}
	}

	private static class AnnotatedParameterizedTypeImpl extends AnnotatedTypeImpl implements AnnotatedParameterizedType {
		protected final AnnotatedType[] typeArguments;

		public AnnotatedParameterizedTypeImpl(ParameterizedType type, Annotation[] annotations, AnnotatedType[] typeArguments) {
			super(type, annotations);
			this.typeArguments = typeArguments;
		}

		@Override
		public AnnotatedType[] getAnnotatedActualTypeArguments() {
			return typeArguments;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AnnotatedParameterizedTypeImpl type = (AnnotatedParameterizedTypeImpl) o;
			return Arrays.equals(typeArguments, type.typeArguments);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(typeArguments);
		}

		@Override
		public String toString() {
			return "" +
					(annotations.length == 0 ? "" :
							Arrays.stream(annotations).map(Objects::toString).collect(joining(", ", "", " "))) +
					getRawClass(type).getCanonicalName() +
					(typeArguments.length == 0 ? "" :
							Arrays.stream(typeArguments).map(Objects::toString).collect(joining(", ", "<", ">")));
		}
	}

	private static class AnnotatedWildcardTypeImpl extends AnnotatedTypeImpl implements AnnotatedWildcardType {
		private final AnnotatedType[] upperBounds;
		private final AnnotatedType[] lowerBounds;

		public AnnotatedWildcardTypeImpl(WildcardType type, Annotation[] annotations, AnnotatedType[] upperBounds, AnnotatedType[] lowerBounds) {
			super(type, annotations);
			this.upperBounds = upperBounds;
			this.lowerBounds = lowerBounds;
		}

		@Override
		public AnnotatedType[] getAnnotatedUpperBounds() {
			return upperBounds;
		}

		@Override
		public AnnotatedType[] getAnnotatedLowerBounds() {
			return lowerBounds;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AnnotatedWildcardTypeImpl type = (AnnotatedWildcardTypeImpl) o;
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

		@Override
		public String toString() {
			return "" +
					(annotations.length == 0 ? "" :
							Arrays.stream(annotations).map(Objects::toString).collect(joining(", ", "", " ")) + " ") +
					"?" +
					" extends " + Arrays.stream(upperBounds).map(Objects::toString).collect(joining(", ")) +
					(lowerBounds.length == 0 ? "" :
							" super " + Arrays.stream(lowerBounds).map(Objects::toString).collect(joining(", ")));
		}
	}

}
