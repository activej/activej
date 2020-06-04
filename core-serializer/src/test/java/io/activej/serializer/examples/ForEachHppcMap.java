package io.activej.serializer.examples;

import io.activej.codegen.expression.AbstractExpressionMapForEach;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.VarLocal;

import java.util.function.Function;

import static io.activej.codegen.expression.Expressions.property;

public final class ForEachHppcMap extends AbstractExpressionMapForEach {

	public ForEachHppcMap(Expression collection, Function<Expression, Expression> forEachValue, Function<Expression, Expression> forEachKey, Class<?> entryType) {
		super(collection, forEachKey, forEachValue, entryType);
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
