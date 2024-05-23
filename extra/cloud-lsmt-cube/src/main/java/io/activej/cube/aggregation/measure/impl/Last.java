package io.activej.cube.aggregation.measure.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.util.Utils.ValueWithTimestamp;
import io.activej.types.Primitives;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.cube.aggregation.util.Utils.defaultValue;

@ExposedInternals
public final class Last extends Measure {
	public final @Nullable CurrentTimeProvider timeProvider;

	@SuppressWarnings("rawtypes")
	public Last(FieldType fieldType, @Nullable CurrentTimeProvider timeProvider) {
		super(fieldType);
		this.timeProvider = timeProvider;
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return property(accumulator, "value");
	}

	@Override
	public Expression zeroAccumulator(Variable accumulator) {
		return set(accumulator, constructor(ValueWithTimestamp.class, sanitizeValue(defaultValue(fieldType.getClass())), value(0L)));
	}

	@Override
	public Expression initAccumulatorWithAccumulator(Variable accumulator, Expression firstAccumulator) {
		return set(accumulator, firstAccumulator);
	}

	@Override
	public Expression initAccumulatorWithValue(Variable accumulator, Variable firstValue) {
		return set(accumulator, constructor(ValueWithTimestamp.class, sanitizeValue(firstValue),
			timeProvider == null ?
				staticCall(System.class, "currentTimeMillis") :
				call(value(timeProvider), "currentTimeMillis")));
	}

	@Override
	public Expression reduce(Variable accumulator, Variable nextAccumulator) {
		return Expressions.ifGe(
			property(nextAccumulator, "timestamp"),
			property(accumulator, "timestamp"),
			Expressions.set(accumulator, nextAccumulator),
			voidExp());
	}

	@Override
	public Expression accumulate(Variable accumulator, Variable nextValue) {
		return set(property(accumulator, "value"), nextValue);
	}

	private Expression sanitizeValue(Expression firstValue) {
		Type dataType = fieldType.getDataType();
		return Primitives.isPrimitiveType(dataType) ?
			cast(firstValue, Primitives.wrap((Class<?>) dataType)) :
			firstValue;
	}
}
