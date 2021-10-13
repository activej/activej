package io.activej.serializer.examples;

import io.activej.codegen.expression.AbstractExpressionIteratorForEach;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.VarLocal;

import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.property;

public final class ForEachHppcCollection extends AbstractExpressionIteratorForEach {
	public ForEachHppcCollection(Expression collection, Class<?> type, UnaryOperator<Expression> action) {
		super(collection, type, action);
	}

	@Override
	protected Expression getValue(VarLocal varIt) {
		return property(varIt, "value");
	}
}
