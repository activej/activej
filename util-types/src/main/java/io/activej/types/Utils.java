package io.activej.types;

import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.function.Function;
import java.util.function.Predicate;

@SuppressWarnings("ForLoopReplaceableByForEach")
public class Utils {

	@SuppressWarnings("ForLoopReplaceableByForEach")
	public static boolean hasAnnotation(Annotation[] annotations, Class<? extends Annotation> type) {
		for (int i = 0; i < annotations.length; i++) {
			Class<? extends Annotation> annotationType = annotations[i].annotationType();
			if (type == annotationType) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("ForLoopReplaceableByForEach")
	public static boolean hasAnnotation(Annotation[] annotations, Predicate<Class<Annotation>> type) {
		for (int i = 0; i < annotations.length; i++) {
			//noinspection unchecked
			Class<Annotation> annotationType = (Class<Annotation>) annotations[i].annotationType();
			if (type.test(annotationType)) {
				return true;
			}
		}
		return false;
	}

	public static <A extends Annotation> @Nullable A getAnnotation(Annotation[] annotations, Class<A> type) {
		return getAnnotation(annotations, type, Function.identity());
	}

	public static <R, A extends Annotation> @Nullable R getAnnotation(Annotation[] annotations, Class<A> type, Function<A, R> extractor) {
		for (int i = 0; i < annotations.length; i++) {
			Annotation annotation = annotations[i];
			if (annotation.annotationType() == type) {
				//noinspection unchecked
				return extractor.apply((A) annotation);
			}
		}
		return null;
	}

	public static <R> @Nullable R getAnnotation(Annotation[] annotations, Function<Annotation, @Nullable R> extractor) {
		for (int i = 0; i < annotations.length; i++) {
			Annotation annotation = annotations[i];
			@Nullable R data = extractor.apply(annotation);
			if (data != null) {
				return data;
			}
		}
		return null;
	}

}
