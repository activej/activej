package io.activej.types.scanner;

import io.activej.types.TypeT;
import org.junit.Test;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.activej.types.AnnotatedTypes.*;
import static org.junit.Assert.assertEquals;

public class TypeScannerRegistryTest {
	@Test
	@SuppressWarnings("rawtypes")
	public void test1() {
		TypeScannerRegistry<String> registry = TypeScannerRegistry.<String>create()
				.with(int.class, ctx -> "int")
				.with(Integer.class, ctx -> "Integer")
				.with(CharSequence.class, ctx -> "CharSequence")
				.with(String.class, ctx -> "String")
				.with(List.class, ctx -> "List<" + ctx.scanTypeArgument(0) + ">")
				.with(Map.class, ctx -> "Map<" + ctx.scanTypeArgument(0) + ", " + ctx.scanTypeArgument(1) + ">")
				.with(Optional.class, ctx -> "Optional0<" + (ctx.hasTypeArguments() ? ctx.scanTypeArgument(0) : "") + ">")
				.with(new TypeT<Optional<CharSequence>>() {}, ctx -> "Optional1<" + ctx.scanTypeArgument(0) + ">")
				.with(new TypeT<Optional<? extends CharSequence>>() {}, ctx -> "Optional2<" + ctx.scanTypeArgument(0) + ">")
				.with(Object.class, ctx -> {
					scan(ctx.getAnnotatedType());
					return "*";
				})
				.with(Enum.class, ctx -> ctx.getRawType().getSimpleName())
				.with(new TypeT<Object[]>() {}, ctx -> ctx.scanTypeArgument(0) + "[]")
				.with(new TypeT<int[]>() {}, ctx -> ctx.scanTypeArgument(0) + "[]");

		TypeScanner<String> scanner = registry.scanner();
		assertEquals("List<String>", scanner.scan(new TypeT<List<String>>() {}));
		assertEquals("Map<Integer, String>", scanner.scan(new TypeT<Map<Integer, String>>() {}));
		assertEquals("Integer[]", scanner.scan(new TypeT<Integer[]>() {}));
		assertEquals("Integer[]", scanner.scan(new TypeT<@Annotation1 Integer @Annotation2 []>() {}));
		assertEquals("int[]", scanner.scan(new TypeT<@Annotation1 int @Annotation2 []>() {}));
		assertEquals("TestEnum1", scanner.scan(new TypeT<TestEnum1>() {}));
		assertEquals("Optional0<>", scanner.scan(new TypeT<Optional>() {}));
		assertEquals("Optional0<Integer>", scanner.scan(new TypeT<Optional<Integer>>() {}));
		assertEquals("Optional1<CharSequence>", scanner.scan(new TypeT<Optional<CharSequence>>() {}));
		assertEquals("Optional2<String>", scanner.scan(new TypeT<Optional<String>>() {}));
		assertEquals("*", scanner.scan(new TypeT<@Annotation1 TestClass2<@Annotation1 String, Integer>>() {}.getAnnotatedType()));
	}

	public static void scan(AnnotatedType annotatedType) {
		while (annotatedType.getType() != Object.class) {
			Class<?> typeClazz = getRawType(annotatedType);

			System.out.println();
			System.out.println(annotatedType);
			Field[] fields = typeClazz.getDeclaredFields();
			Map<TypeVariable<?>, AnnotatedType> typeBindings = getTypeBindings(annotatedType);
			for (Field field : fields) {
				AnnotatedType fieldActualType = bind(field.getAnnotatedType(), typeBindings::get);
				System.out.println(field.getName() + " : " + fieldActualType);
			}

			annotatedType = bind(typeClazz.getAnnotatedSuperclass(), typeBindings::get);
		}
	}

	@Test
	public void test2() {
		scan(new TypeT<@Annotation1 TestClass2<@Annotation1 String, Integer>>() {}.getAnnotatedType());
	}

}
