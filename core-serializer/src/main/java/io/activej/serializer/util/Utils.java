package io.activej.serializer.util;

import io.activej.codegen.expression.Expression;

import static io.activej.codegen.expression.Expressions.*;

public class Utils {
	public static Expression initialCapacity(Expression initialSize) {
		return mul(div(add(initialSize, value(2)), value(3)), value(4));
	}
}
