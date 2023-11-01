/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.cube.aggregation.measure.impl;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.common.annotation.ExposedInternals;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.cube.aggregation.util.Utils.isArithmeticType;
import static io.activej.cube.aggregation.util.Utils.needsWideningToInt;
import static io.activej.types.Primitives.wrap;

@ExposedInternals
public final class Min extends Measure {
	@SuppressWarnings("rawtypes")
	public Min(FieldType fieldType) {
		super(fieldType);
		checkArgument(isArithmeticType(fieldType.getInternalDataType()), "Not arithmetic type");
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return accumulator;
	}

	@Override
	public Expression zeroAccumulator(Variable accumulator) {
		//noinspection unchecked
		Class<?> wrapped = wrap(fieldType.getInternalDataType());
		return set(accumulator, staticField(wrapped, "MAX_VALUE"));
	}

	@Override
	public Expression initAccumulatorWithAccumulator(Variable accumulator, Expression firstAccumulator) {
		return set(accumulator, firstAccumulator);
	}

	@Override
	public Expression reduce(Variable accumulator, Variable nextAccumulator) {
		if (needsWideningToInt(fieldType.getInternalDataType())) {
			return set(accumulator, staticCall(Math.class, "min",
				cast(accumulator, int.class),
				cast(nextAccumulator, int.class)
			));
		}
		return set(accumulator, staticCall(Math.class, "min", accumulator, nextAccumulator));
	}

	@Override
	public Expression initAccumulatorWithValue(Variable accumulator, Variable firstValue) {
		return set(accumulator, firstValue);
	}

	@Override
	public Expression accumulate(Variable accumulator, Variable nextValue) {
		return reduce(accumulator, nextValue);
	}
}
