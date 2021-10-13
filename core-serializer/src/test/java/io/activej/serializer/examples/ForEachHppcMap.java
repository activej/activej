package io.activej.serializer.examples;

import io.activej.codegen.expression.AbstractExpressionMapForEach;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.VarLocal;

import java.util.function.BinaryOperator;

import static io.activej.codegen.expression.Expressions.property;

public final class ForEachHppcMap extends AbstractExpressionMapForEach {

	public ForEachHppcMap(Expression collection, BinaryOperator<Expression> action, Class<?> entryType) {
		super(collection, action, entryType);
	}

	@Override
	protected Expression getEntries() {
		return collection;
	}

	@Override
	protected Expression getKey(VarLocal entry) {
		return property(entry, "key");
	}

	@Override
	protected Expression getValue(VarLocal entry) {
		return property(entry, "value");
	}
}
