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

package io.activej.aggregation.measure;

import io.activej.aggregation.fieldtype.FieldTypes;
import io.activej.codegen.Context;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.common.initializer.WithInitializer;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.codegen.util.TypeChecks.checkType;
import static io.activej.codegen.util.TypeChecks.isNotThrow;
import static io.activej.codegen.util.Utils.isWrapperType;
import static org.objectweb.asm.Type.*;

public final class MeasureHyperLogLog extends Measure implements WithInitializer<MeasureHyperLogLog> {
	private final int registers;

	private MeasureHyperLogLog(int registers) {
		super(FieldTypes.ofHyperLogLog());
		this.registers = registers;
	}

	public static MeasureHyperLogLog create(int registers) {
		return new MeasureHyperLogLog(registers);
	}

	@Override
	public Expression valueOfAccumulator(Expression accumulator) {
		return call(accumulator, "estimate");
	}

	@Override
	public Expression zeroAccumulator(Variable accumulator) {
		return set(accumulator, constructor(HyperLogLog.class, value(registers)));
	}

	@Override
	public Expression initAccumulatorWithAccumulator(Variable accumulator, Expression firstAccumulator) {
		return sequence(
				set(accumulator, constructor(HyperLogLog.class, value(registers))),
				call(accumulator, "union", firstAccumulator));
	}

	@Override
	public Expression reduce(Variable accumulator,
			Variable nextAccumulator) {
		return call(accumulator, "union", nextAccumulator);
	}

	@Override
	public Expression initAccumulatorWithValue(Variable accumulator,
			Variable firstValue) {
		return sequence(
				set(accumulator, constructor(HyperLogLog.class, value(registers))),
				add(accumulator, firstValue));
	}

	@Override
	public Expression accumulate(Variable accumulator, Variable nextValue) {
		return add(accumulator, nextValue);
	}

	private static Expression add(Expression accumulator, Expression value) {
		return new ExpressionHyperLogLog(value, accumulator);
	}

	private static class ExpressionHyperLogLog implements Expression {
		private final Expression value;
		private final Expression accumulator;

		public ExpressionHyperLogLog(Expression value, Expression accumulator) {
			this.value = value;
			this.accumulator = accumulator;
		}

		@Override
		public Type load(Context ctx) {
			GeneratorAdapter g = ctx.getGeneratorAdapter();
			Type accumulatorType = accumulator.load(ctx);
			checkType(accumulatorType, isNotThrow());

			Type valueType = value.load(ctx);
			checkType(valueType, isNotThrow());

			String methodName;
			Type methodParameterType;
			if (valueType == LONG_TYPE || valueType.getClassName().equals(Long.class.getName())) {
				methodName = "addLong";
				methodParameterType = LONG_TYPE;
			} else if (valueType == INT_TYPE || valueType.getClassName().equals(Integer.class.getName())) {
				methodName = "addInt";
				methodParameterType = INT_TYPE;
			} else {
				methodName = "addObject";
				methodParameterType = getType(Object.class);
			}

			if (isWrapperType(valueType)) {
				g.unbox(methodParameterType);
			}

			ctx.invoke(accumulatorType, methodName, methodParameterType);

			return VOID_TYPE;
		}
	}
}
