package io.activej.cube.aggregation.measure.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.util.Utils.ValueWithTimestamp;
import io.activej.types.Primitives;
import org.jetbrains.annotations.Nullable;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;

@ExposedInternals
public final class LastNotNull extends Measure {
	public final @Nullable CurrentTimeProvider timeProvider;

	@SuppressWarnings("rawtypes")
	public LastNotNull(FieldType fieldType, @Nullable CurrentTimeProvider timeProvider) {
		super(fieldType);
		checkArgument(!Primitives.isPrimitiveType(fieldType.getDataType()), "Primitive types cannot be nullable");
		this.timeProvider = timeProvider;
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return property(accumulator, "value");
	}

	@Override
	public Expression zeroAccumulator(Variable accumulator) {
		return constructor(ValueWithTimestamp.class, nullRef(fieldType.getClass()), value(0L));
	}

	@Override
	public Expression initAccumulatorWithAccumulator(Variable accumulator, Expression firstAccumulator) {
		return set(accumulator, firstAccumulator);
	}

	@Override
	public Expression initAccumulatorWithValue(Variable accumulator, Variable firstValue) {
		return set(accumulator, constructor(ValueWithTimestamp.class, firstValue,
			timeProvider == null ?
				staticCall(System.class, "currentTimeMillis") :
				call(value(timeProvider), "currentTimeMillis")));
	}

	@Override
	public Expression reduce(Variable accumulator, Variable nextAccumulator) {
		return ifNonNull(property(nextAccumulator, "value"),
			ifNull(property(accumulator, "value"),
				set(accumulator, nextAccumulator),
				ifGe(
					property(nextAccumulator, "timestamp"),
					property(accumulator, "timestamp"),
					set(accumulator, nextAccumulator),
					voidExp())
			),
			voidExp()
		);
	}

	@Override
	public Expression accumulate(Variable accumulator, Variable nextValue) {
		return ifNonNull(nextValue,
			set(property(accumulator, "value"), nextValue),
			voidExp()
		);
	}
}
