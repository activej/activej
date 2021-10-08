package io.activej.types;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

/**
 * Various utility methods to operate on annotated types
 */
public class AnnotatedTypes {
	/**
	 * An empty array of {@link Annotation}s
	 */
	public static final Annotation[] NO_ANNOTATIONS = new Annotation[0];

	/**
	 * An empty array of {@link AnnotatedType}s
	 */
	public static final AnnotatedType[] NO_ANNOTATED_TYPES = new AnnotatedType[0];

	/**
	 * Returns a raw {@link Class} for a given {@link AnnotatedType}
	 *
	 * @see Types#getRawType(Type)
	 */
	public static Class<?> getRawType(AnnotatedType type) {
		return Types.getRawType(type.getType());
	}

	/**
	 * Returns an array of annotated type arguments for a given {@link AnnotatedType}
	 *
	 * @param annotatedType annotated type whose annotated type arguments should be retrieved
	 * @return an array of annotated type arguments for a given {@link AnnotatedType}
	 */
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

	/**
	 * Returns a map of type bindings for a given {@link AnnotatedType}
	 */
	public static Map<TypeVariable<?>, AnnotatedType> getTypeBindings(AnnotatedType type) {
		Class<?> typeClazz = getRawType(type);
		AnnotatedType[] typeArguments = getTypeArguments(type);
		if (typeArguments.length == 0) return Collections.emptyMap();
		Map<TypeVariable<?>, AnnotatedType> map = new LinkedHashMap<>();
		TypeVariable<?>[] typeVariables = typeClazz.getTypeParameters();
		for (int i = 0; i < typeVariables.length; i++) {
			map.put(typeVariables[i], typeArguments[i]);
		}
		return map;
	}

	/**
	 * Binds a given annotated type with actual annotated type arguments
	 *
	 * @param annotatedType an annotated type to be bound
	 * @param bindings      a lookup function for actual annotated types
	 */
	public static AnnotatedType bind(AnnotatedType annotatedType, Function<TypeVariable<?>, AnnotatedType> bindings) {
		return bind(annotatedType, bindings, (annotation1, annotation2) -> annotation2);
	}

	/**
	 * Binds a given annotated type with actual annotated type arguments
	 *
	 * @param annotatedType        an annotated type to be bound
	 * @param bindings             a lookup function for actual annotated types
	 * @param annotationCombinerFn a combiner function to combine annotations of a given annotated type
	 *                             with annotations of bound actual annotated type
	 */
	public static @NotNull AnnotatedType bind(AnnotatedType annotatedType, Function<TypeVariable<?>, AnnotatedType> bindings,
			BinaryOperator<Annotation[]> annotationCombinerFn) {
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
					new Types.ParameterizedTypeImpl(
							((ParameterizedType) annotatedType.getType()).getOwnerType(),
							((ParameterizedType) annotatedType.getType()).getRawType(),
							typeArguments2),
					annotations,
					annotatedTypeArguments2);
		}
		if (annotatedType instanceof AnnotatedArrayType) {
			AnnotatedArrayType annotatedArrayType = ((AnnotatedArrayType) annotatedType);
			AnnotatedType annotatedGenericComponentType = annotatedArrayType.getAnnotatedGenericComponentType();
			AnnotatedType annotatedGenericComponentType2 = bind(annotatedGenericComponentType, bindings);
			return new AnnotatedArrayTypeImpl(
					new Types.GenericArrayTypeImpl(annotatedGenericComponentType2.getType()),
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
					new Types.WildcardTypeImpl(upperBounds2, lowerBounds2),
					annotations,
					annotatedUpperBounds2, annotatedLowerBounds2);
		}
		throw new IllegalArgumentException("Unsupported type: " + annotatedType);
	}

	/**
	 * Constructs an {@link AnnotatedType} from a given {@link Type}.
	 * Resulting annotated type has no annotations
	 *
	 * @param type a type to construct annotated type from
	 * @return a new instance of {@link  AnnotatedType}
	 */
	public static @NotNull AnnotatedType annotatedTypeOf(Type type) {
		return annotatedTypeOf(type, NO_ANNOTATIONS);
	}

	/**
	 * Constructs an {@link AnnotatedType} from a given {@link Type} and an array of annotations.
	 *
	 * @param type        a type to construct annotated type from
	 * @param annotations an array of annotations for a given type
	 * @return a new instance of {@link  AnnotatedType}
	 */
	public static @NotNull AnnotatedType annotatedTypeOf(Type type, Annotation[] annotations) {
		return annotatedTypeOf(type, ($, ints) -> ints.length == 0 ? annotations : NO_ANNOTATIONS);
	}

	/**
	 * Constructs an {@link AnnotatedType} from a given {@link Type} and
	 * a function that transforms a type and a path to the type into annotations for this type.
	 * <p>
	 * This method allows building complex annotated types
	 *
	 * @param type          a type to construct annotated type from
	 * @param annotationsFn a function that transforms a type and a path to the type into annotations for this type
	 * @return a new instance of {@link  AnnotatedType}
	 */
	public static @NotNull AnnotatedType annotatedTypeOf(Type type, BiFunction<Type, int[], Annotation[]> annotationsFn) {
		return annotatedTypeOf(type, new int[]{}, annotationsFn);
	}

	private static @NotNull AnnotatedType annotatedTypeOf(Type type, int[] path, BiFunction<Type, int[], Annotation[]> annotationsFn) {
		Annotation[] annotations = annotationsFn.apply(type, path);
		if (type instanceof Class) {
			if (((Class<?>) type).isArray()) {
				int[] newPath = newPath(path);
				Type componentType = ((Class<?>) type).getComponentType();
				AnnotatedType annotatedComponentType = annotatedTypeOf(componentType, newPath, annotationsFn);
				return new AnnotatedArrayTypeImpl(type, annotations, annotatedComponentType);
			}
			return new AnnotatedTypeImpl(type, annotations);
		}
		if (type instanceof TypeVariable) {
			int idx = 0;
			int[] newPath = newPath(path);
			Type[] bounds = ((TypeVariable<?>) type).getBounds();
			AnnotatedType[] annotatedBounds = new AnnotatedType[bounds.length];
			for (Type bound : bounds) {
				annotatedBounds[idx] = annotatedTypeOf(bound, newPath(newPath, idx), annotationsFn);
				idx++;
			}
			return new AnnotatedTypeVariableImpl(type, annotations, annotatedBounds);
		}
		if (type instanceof ParameterizedType) {
			int idx = 0;
			int[] newPath = newPath(path);
			Type[] typeArguments = ((ParameterizedType) type).getActualTypeArguments();
			AnnotatedType[] annotatedTypeArguments = new AnnotatedType[typeArguments.length];
			for (Type typeArgument : typeArguments) {
				annotatedTypeArguments[idx] = annotatedTypeOf(typeArgument, newPath(newPath, idx), annotationsFn);
				idx++;
			}
			return new AnnotatedParameterizedTypeImpl(type, annotations, annotatedTypeArguments);
		}
		if (type instanceof GenericArrayType) {
			int[] newPath = newPath(path);
			Type componentType = ((GenericArrayType) type).getGenericComponentType();
			AnnotatedType annotatedComponentType = annotatedTypeOf(componentType, newPath, annotationsFn);
			return new AnnotatedArrayTypeImpl(type, annotations, annotatedComponentType);
		}
		if (type instanceof WildcardType) {
			int idx = 0;
			int[] newPath = newPath(path);
			Type[] upperBounds = ((WildcardType) type).getUpperBounds();
			AnnotatedType[] annotatedUpperBounds = new AnnotatedType[upperBounds.length];
			for (Type upperBound : upperBounds) {
				annotatedUpperBounds[idx] = annotatedTypeOf(upperBound, newPath(newPath, idx), annotationsFn);
				idx++;
			}
			Type[] lowerBounds = ((WildcardType) type).getLowerBounds();
			AnnotatedType[] annotatedLowerBounds = new AnnotatedType[lowerBounds.length];
			for (Type lowerBound : lowerBounds) {
				annotatedLowerBounds[idx] = annotatedTypeOf(lowerBound, newPath(newPath, idx), annotationsFn);
				idx++;
			}
			return new AnnotatedWildcardTypeImpl(type, annotations, annotatedUpperBounds, annotatedLowerBounds);
		}
		throw new IllegalArgumentException("Type is not supported: " + type);
	}

	private static int[] newPath(int[] path) {
		return Arrays.copyOf(path, path.length + 1);
	}

	private static int[] newPath(int[] path, int idx) {
		path[path.length - 1] = idx;
		return path;
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

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Annotation> T getAnnotation(@NotNull Class<T> annotationClass) {
			return (T) Arrays.stream(annotations).filter(a -> a.annotationType() == annotationClass).findFirst().orElse(null);
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
		public String toString() {
			return "" +
					(annotations.length == 0 ? "" :
							Arrays.stream(annotations).map(Objects::toString).collect(joining(", ", "", " "))) +
					Types.getRawType(type).getCanonicalName() +
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
