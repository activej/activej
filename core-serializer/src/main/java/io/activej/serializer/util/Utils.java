package io.activej.serializer.util;

import io.activej.codegen.expression.Expression;

import java.util.function.Supplier;

import static io.activej.codegen.expression.Expressions.*;

public class Utils {
	public static <T> T get(Supplier<T> supplier) {
		return supplier.get();
	}

	public static Expression hashInitialSize(Expression length) {
		return mul(div(add(length, value(2)), value(3)), value(4));
	}
}
