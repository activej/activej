package io.activej.serializer.util;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public class AnnotatedTypeUtils {
	public static final Annotation[] NO_ANNOTATIONS = new Annotation[0];
	public static final AnnotatedType[] NO_ANNOTATED_TYPES = new AnnotatedType[0];

	public static Class<?> getRawClass(AnnotatedType type) {
		return TypeUtils.getRawClass(type.getType());
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

	public static AnnotatedType bind(AnnotatedType annotatedType, Function<TypeVariable<?>, AnnotatedType> bindings) {
		return bind(annotatedType, bindings, (annotation1, annotation2) -> annotation2);
	}

	@NotNull
	public static AnnotatedType bind(AnnotatedType annotatedType, Function<TypeVariable<?>, AnnotatedType> bindings,
			BiFunction<Annotation[], Annotation[], Annotation[]> annotationCombinerFn) {
		if (annotatedType.getType() instanceof Class) return annotatedType;
		Annotation[] annotations = annotatedType.getAnnotations();
		if (annotatedType instanceof AnnotatedTypeVariable) {
			AnnotatedTypeVariable annotatedTypeVariable = (AnnotatedTypeVariable) annotatedType;
			AnnotatedType actualType = bindings.apply((TypeVariable<?>) annotatedTypeVariable.getType());
			if (actualType == null) throw new IllegalArgumentException("Type not found: " + annotatedType);
			if (annotations.length == 0) return actualType;
			return annotatedTypeOf(actualType.getType(), annotationCombinerFn.apply(actualType.getAnnotations(), annotations));
		}
		if (annotatedType instanceof AnnotatedParameterizedType) {
			AnnotatedParameterizedType annotatedParameterizedType = (AnnotatedParameterizedType) annotatedType;
			AnnotatedType[] annotatedTypeArguments = annotatedParameterizedType.getAnnotatedActualTypeArguments();
			AnnotatedType[] annotatedTypeArguments2 = new AnnotatedType[annotatedTypeArguments.length];
			Type[] typeArguments2 = new Type[annotatedTypeArguments.length];
			for (int i = 0; i < annotatedTypeArguments.length; i++) {
				annotatedTypeArguments2[i] = bind(annotatedTypeArguments[i], bindings, annotationCombinerFn);
				typeArguments2[i] = annotatedTypeArguments2[i].getType();
			}
			return new AnnotatedParameterizedTypeImpl(
					new TypeUtils.ParameterizedTypeImpl(
							(Class<?>) ((ParameterizedType) annotatedType.getType()).getRawType(),
							typeArguments2),
					annotations,
					annotatedTypeArguments2);
		}
		if (annotatedType instanceof AnnotatedArrayType) {
			AnnotatedArrayType annotatedArrayType = ((AnnotatedArrayType) annotatedType);
			AnnotatedType annotatedGenericComponentType = annotatedArrayType.getAnnotatedGenericComponentType();
			AnnotatedType annotatedGenericComponentType2 = bind(annotatedGenericComponentType, bindings);
			return new AnnotatedArrayTypeImpl(
					new TypeUtils.GenericArrayTypeImpl(annotatedGenericComponentType2.getType()),
					annotations,
					annotatedGenericComponentType2);
		}
		if (annotatedType instanceof AnnotatedWildcardType) {
			AnnotatedWildcardType annotatedWildcardType = ((AnnotatedWildcardType) annotatedType);
			AnnotatedType[] annotatedLowerBounds = annotatedWildcardType.getAnnotatedLowerBounds();
			AnnotatedType[] annotatedLowerBounds2 = new AnnotatedType[annotatedLowerBounds.length];
			Type[] lowerBounds2 = new Type[annotatedLowerBounds.length];
			for (int i = 0; i < annotatedLowerBounds.length; i++) {
				annotatedLowerBounds2[i] = bind(annotatedLowerBounds[i], bindings, annotationCombinerFn);
				lowerBounds2[i] = annotatedLowerBounds2[i].getType();
			}
			AnnotatedType[] annotatedUpperBounds = annotatedWildcardType.getAnnotatedUpperBounds();
			AnnotatedType[] annotatedUpperBounds2 = new AnnotatedType[annotatedUpperBounds.length];
			Type[] upperBounds2 = new Type[annotatedUpperBounds.length];
			for (int i = 0; i < annotatedUpperBounds.length; i++) {
				annotatedUpperBounds2[i] = bind(annotatedUpperBounds[i], bindings, annotationCombinerFn);
				upperBounds2[i] = annotatedUpperBounds2[i].getType();
			}
			return new AnnotatedWildcardTypeImpl(
					new TypeUtils.WildcardTypeImpl(lowerBounds2, upperBounds2),
					annotations,
					annotatedUpperBounds2, annotatedLowerBounds2);
		}
		throw new IllegalArgumentException("Unsupported type: " + annotatedType);
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
		if (type instanceof TypeVariable) {
			return new AnnotatedTypeVariableImpl(type, annotations,
					Arrays.stream(((TypeVariable<?>) type).getBounds()).map(AnnotatedTypeUtils::annotatedTypeOf).toArray(AnnotatedType[]::new));
		}
		if (type instanceof ParameterizedType) {
			return new AnnotatedParameterizedTypeImpl(type, annotations,
					Arrays.stream(((ParameterizedType) type).getActualTypeArguments()).map(AnnotatedTypeUtils::annotatedTypeOf).toArray(AnnotatedType[]::new));
		}
		if (type instanceof GenericArrayType) {
			return new AnnotatedArrayTypeImpl(type, annotations,
					annotatedTypeOf(type, annotations));
		}
		if (type instanceof WildcardType) {
			return new AnnotatedWildcardTypeImpl(type, annotations,
					Arrays.stream(((WildcardType) type).getUpperBounds()).map(AnnotatedTypeUtils::annotatedTypeOf).toArray(AnnotatedType[]::new),
					Arrays.stream(((WildcardType) type).getLowerBounds()).map(AnnotatedTypeUtils::annotatedTypeOf).toArray(AnnotatedType[]::new));
		}
		throw new IllegalArgumentException("Type is not supported: " + type);
	}

	static class AnnotatedTypeImpl implements AnnotatedType {
		protected final Type type;
		protected final Annotation[] annotations;

		AnnotatedTypeImpl(Type type, Annotation[] annotations) {
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
					type;
		}
	}

	static class AnnotatedTypeVariableImpl extends AnnotatedTypeImpl implements AnnotatedTypeVariable {
		private final AnnotatedType[] bounds;

		public AnnotatedTypeVariableImpl(Type type, Annotation[] annotations, AnnotatedType[] bounds) {
			super(type, annotations);
			this.bounds = bounds;
		}

		@Override
		public AnnotatedType[] getAnnotatedBounds() {
			return bounds;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			if (!super.equals(o)) return false;
			AnnotatedTypeVariableImpl variable = (AnnotatedTypeVariableImpl) o;
			return Arrays.equals(bounds, variable.bounds);
		}

		@Override
		public int hashCode() {
			int result = super.hashCode();
			result = 31 * result + Arrays.hashCode(bounds);
			return result;
		}
	}

	static class AnnotatedParameterizedTypeImpl extends AnnotatedTypeImpl implements AnnotatedParameterizedType {
		protected final AnnotatedType[] typeArguments;

		AnnotatedParameterizedTypeImpl(Type type, Annotation[] annotations, AnnotatedType[] typeArguments) {
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
					TypeUtils.getRawClass(type).getCanonicalName() +
					(typeArguments.length == 0 ? "" :
							Arrays.stream(typeArguments).map(Objects::toString).collect(joining(", ", "<", ">")));
		}
	}

	static class AnnotatedArrayTypeImpl extends AnnotatedTypeImpl implements AnnotatedArrayType {
		private final AnnotatedType componentType;

		AnnotatedArrayTypeImpl(Type type, Annotation[] annotations, AnnotatedType componentType) {
			super(type, annotations);
			this.componentType = componentType;
		}

		@Override
		public AnnotatedType getAnnotatedGenericComponentType() {
			return componentType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			if (!super.equals(o)) return false;

			AnnotatedArrayTypeImpl type = (AnnotatedArrayTypeImpl) o;

			return componentType.equals(type.componentType);
		}

		@Override
		public int hashCode() {
			int result = super.hashCode();
			result = 31 * result + componentType.hashCode();
			return result;
		}
	}

	static class AnnotatedWildcardTypeImpl extends AnnotatedTypeImpl implements AnnotatedWildcardType {
		private final AnnotatedType[] upperBounds;
		private final AnnotatedType[] lowerBounds;

		AnnotatedWildcardTypeImpl(Type type, Annotation[] annotations, AnnotatedType[] upperBounds, AnnotatedType[] lowerBounds) {
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
