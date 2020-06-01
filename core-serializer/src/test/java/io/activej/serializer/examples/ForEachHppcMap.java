package io.activej.serializer.examples;

import io.activej.codegen.AbstractExpressionMapForEach;
import io.activej.codegen.Expression;
import io.activej.codegen.VarLocal;

import java.util.function.Function;

import static io.activej.codegen.Expressions.property;

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
