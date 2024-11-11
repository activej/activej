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

package io.activej.cube.measure;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.common.annotation.StaticFactories;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.measure.impl.Value;

import java.util.Map;

@StaticFactories(ComputedMeasure.class)
public class ComputedMeasures {

	private static final class E extends Expressions {}

	public static ComputedMeasure value(Object value) {
		return new Value(value);
	}

	public static ComputedMeasure measure(String measureId) {
		return new io.activej.cube.measure.impl.Measure(measureId);
	}

	public static ComputedMeasure add(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.add(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure sub(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.sub(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure div(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractComputedMeasure(measure1, measure2) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.let(
					E.cast(measure2.getExpression(record, storedMeasures), double.class),
					value -> E.ifNe(value, E.value(0.0),
						E.div(measure1.getExpression(record, storedMeasures), value),
						E.value(0.0)));
			}
		};
	}

	public static ComputedMeasure mul(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.mul(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure sqrt(ComputedMeasure measure) {
		return new AbstractComputedMeasure(measure) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.let(
					E.cast(measure.getExpression(record, storedMeasures), double.class),
					value ->
						E.ifLe(value, E.value(0.0d),
							E.value(0.0d),
							E.staticCall(Math.class, "sqrt", value)));
			}
		};
	}

	public static ComputedMeasure sqr(ComputedMeasure measure) {
		return new AbstractComputedMeasure(measure) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return E.let(E.cast(measure.getExpression(record, storedMeasures), double.class), value ->
					E.mul(value, value));
			}
		};
	}

	public static ComputedMeasure stddev(ComputedMeasure sum, ComputedMeasure sumOfSquares, ComputedMeasure count) {
		return sqrt(variance(sum, sumOfSquares, count));
	}

	public static ComputedMeasure variance(ComputedMeasure sum, ComputedMeasure sumOfSquares, ComputedMeasure count) {
		return sub(div(sumOfSquares, count), sqr(div(sum, count)));
	}

	public static ComputedMeasure percent(ComputedMeasure measure) {
		return mul(measure, value(100));
	}

	public static ComputedMeasure percent(ComputedMeasure numerator, ComputedMeasure denominator) {
		return mul(div(numerator, denominator), value(100));
	}

}
