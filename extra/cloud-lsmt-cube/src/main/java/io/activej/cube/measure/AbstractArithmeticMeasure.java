package io.activej.cube.measure;

import io.activej.aggregation.measure.Measure;
import io.activej.codegen.expression.Expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractArithmeticMeasure extends AbstractComputedMeasure {
	public AbstractArithmeticMeasure(ComputedMeasure... dependencies) {
		super(dependencies);
	}

	@Override
	public final Class<?> getType(Map<String, Measure> storedMeasures) {
		List<Class<?>> types = new ArrayList<>();
		for (ComputedMeasure dependency : dependencies) {
			types.add(dependency.getType(storedMeasures));
		}
		return Expressions.unifyArithmeticTypes(types);
	}
}
