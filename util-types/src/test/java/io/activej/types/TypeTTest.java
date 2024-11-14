package io.activej.types;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;

import static io.activej.types.Types.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class TypeTTest {

	private static boolean atLeastJava9;

	@BeforeClass
	public static void beforeClass() {
		atLeastJava9 = !System.getProperty("java.specification.version").contains(".");
	}

	@Test
	public void regularClass() {
		TypeT<Integer> typeT = new TypeT<@A("Integer") Integer>() {};
		assertTypeT(Integer.class, "Integer", Integer.class, typeT);
	}

	@Test
	public void parameterized() {
		TypeT<List<Integer>> typeT = new TypeT<@A("List") List<Integer>>() {};
		assertTypeT(parameterizedType(List.class, Integer.class), "List", List.class, typeT);

		TypeT<List<? extends Integer>> typeTWildcard = new TypeT<@A("List extends") List<? extends Integer>>() {};
		assertTypeT(parameterizedType(List.class, wildcardTypeExtends(Integer.class)), "List extends", List.class, typeTWildcard);
	}

	@Test
	public void genericArray() {
		TypeT<List<Integer>[]> typeT = new TypeT<List<Integer> @A("Array of lists") []>() {};
		assertTypeT(genericArrayType(parameterizedType(List.class, Integer.class)), "Array of lists", List[].class, typeT);

		TypeT<List<? extends Integer>[]> typeTWildcard = new TypeT<List<? extends Integer> @A("Array of lists extends") []>() {};
		assertTypeT(genericArrayType(parameterizedType(List.class, wildcardTypeExtends(Integer.class))), "Array of lists extends", List[].class, typeTWildcard);
	}

	@Test
	public void typeVariable() {
		TypeTHolder<?> holder = new TypeTHolder<>();
		TypeT<?> typeT = holder.typeT;

		assertThat(typeT.getType(), instanceOf(TypeVariable.class));
		assertAnnotation("Variable", typeT.getAnnotatedType());
		assertEquals(Integer.class, typeT.getRawType());
//		assertEquals(typeT.getAnnotatedType(), TypeT.ofAnnotatedType(typeT.getAnnotatedType()).getAnnotatedType());
	}

	private static <T> void assertTypeT(Type type, String annotationValue, Class<?> rawType, TypeT<T> typeT) {
		assertEquals(type, typeT.getType());
		assertAnnotation(annotationValue, typeT.getAnnotatedType());
		assertEquals(rawType, typeT.getRawType());
//		assertEquals(typeT.getAnnotatedType(), TypeT.ofAnnotatedType(typeT.getAnnotatedType()).getAnnotatedType());
	}

	private static void assertAnnotation(String annotationValue, AnnotatedType annotatedType) {
		if (atLeastJava9) {
			assertEquals(annotationValue, annotatedType.getAnnotation(A.class).value());
		} else {
			assertEquals(0, annotatedType.getAnnotations().length);
		}
	}

	private static class TypeTHolder<T extends Integer> {
		private final TypeT<T> typeT = new TypeT<@A("Variable") T>() {};
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE_USE)
	public @interface A {
		String value() default "";
	}
}
