package io.activej.serializer.examples;

import io.activej.codegen.AbstractExpressionIteratorForEach;
import io.activej.codegen.Expression;
import io.activej.codegen.VarLocal;

import java.util.function.Function;

import static io.activej.codegen.Expressions.property;

public final class ForEachHppcCollection extends AbstractExpressionIteratorForEach {
	public ForEachHppcCollection(Expression collection, Class<?> type, Function<Expression, Expression> forEach) {
		super(collection, type, forEach);
	}

	@Override
	protected Expression getValue(VarLocal varIt) {
		return property(varIt, "value");
	}
}
