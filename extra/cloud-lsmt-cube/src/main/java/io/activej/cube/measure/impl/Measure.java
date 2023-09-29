package io.activej.cube.measure.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.common.annotation.ExposedInternals;
import io.activej.cube.measure.ComputedMeasure;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

@ExposedInternals
public final class Measure implements ComputedMeasure {
	public final String measureId;

	public Measure(String measureId) {this.measureId = measureId;}

	@Override
	public Class<?> getType(Map<String, io.activej.cube.aggregation.measure.Measure> storedMeasures) {
		return (Class<?>) storedMeasures.get(measureId).getFieldType().getDataType();
	}

	@Override
	public Expression getExpression(Expression record, Map<String, io.activej.cube.aggregation.measure.Measure> storedMeasures) {
		return storedMeasures.get(measureId).valueOfAccumulator(Expressions.property(record, measureId));
	}

	@Override
	public Set<String> getMeasureDependencies() {
		return Set.of(measureId);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Measure other = (Measure) o;
		return measureId.equals(other.measureId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(measureId);
	}
}
