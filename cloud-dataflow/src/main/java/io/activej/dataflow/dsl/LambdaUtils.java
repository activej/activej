package io.activej.dataflow.dsl;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.dsl.LambdaParser.LambdaExpression;

import java.lang.reflect.Method;
import java.util.function.Function;
import java.util.function.Predicate;

public final class LambdaUtils {
	private static final DefiningClassLoader DEFINING_CLASS_LOADER = DefiningClassLoader.create();

	@SuppressWarnings("unchecked")
	public static <T, R> Function<T, R> generateMapper(LambdaExpression expression) {
		try {
			return ClassBuilder.create(DEFINING_CLASS_LOADER, Function.class)
					.withClassKey(expression)
					.withMethod("apply", expression.compile(Object.class)) // stub
					.build()
					.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Predicate<T> generatePredicate(LambdaExpression expression, Class<T> argumentType) {
		try {
			return ClassBuilder.create(DEFINING_CLASS_LOADER, Predicate.class)
					.withClassKey(expression)
					.withMethod("test", expression.compile(argumentType))
					.build()
					.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getReturnType(Function<?, T> function) {
		for (Method method : function.getClass().getMethods()) {
			if (method.getName().equals("apply") && !method.isBridge()) {
				return (Class<T>) method.getReturnType();
			}
		}
		throw new IllegalArgumentException("Funciton had no non-bridge apply method implemented");
	}
}
