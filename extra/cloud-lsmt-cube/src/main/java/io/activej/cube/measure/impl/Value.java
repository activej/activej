package io.activej.cube.measure.impl;

import io.activej.aggregation.measure.Measure;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.common.annotation.ExposedInternals;
import io.activej.cube.measure.ComputedMeasure;
import io.activej.types.Primitives;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

@ExposedInternals
public final class Value implements ComputedMeasure {
	public final Object value;

	public Value(Object value) {this.value = value;}

	@Override
	public Class<?> getType(Map<String, io.activej.aggregation.measure.Measure> storedMeasures) {
		return Primitives.unwrap(value.getClass());
	}

	@Override
	public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
		return Expressions.value(value);
	}

	@Override
	public Set<String> getMeasureDependencies() {
		return Set.of();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Value other = (Value) o;
		return Objects.equals(value, other.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}
}
