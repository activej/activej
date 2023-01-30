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

package io.activej.cube;

import io.activej.aggregation.measure.Measure;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Expressions;
import io.activej.types.Primitives;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class ComputedMeasures {
	public static final class E extends Expressions {}

	public abstract static class AbstractComputedMeasure implements ComputedMeasure {
		protected final Set<ComputedMeasure> dependencies;

		protected AbstractComputedMeasure(ComputedMeasure... dependencies) {
			this.dependencies = new LinkedHashSet<>(List.of(dependencies));
		}

		@Override
		public @Nullable Class<?> getType(Map<String, Measure> storedMeasures) {
			return null;
		}

		@Override
		public final Set<String> getMeasureDependencies() {
			Set<String> result = new LinkedHashSet<>();
			for (ComputedMeasure dependency : dependencies) {
				result.addAll(dependency.getMeasureDependencies());
			}
			return result;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AbstractComputedMeasure other = (AbstractComputedMeasure) o;
			return dependencies.equals(other.dependencies);
		}

		@Override
		public int hashCode() {
			return Objects.hash(dependencies);
		}
	}

	public abstract static class AbstractArithmeticMeasure extends AbstractComputedMeasure {
		public AbstractArithmeticMeasure(ComputedMeasure... dependencies) {
			super(dependencies);
		}

		@Override
		public final Class<?> getType(Map<String, Measure> storedMeasures) {
			List<Class<?>> types = new ArrayList<>();
			for (ComputedMeasure dependency : dependencies) {
				types.add(dependency.getType(storedMeasures));
			}
			return E.unifyArithmeticTypes(types);
		}
	}

	public static ComputedMeasure value(Object value) {
		return new ValueComputedMeasure(value);
	}

	public static ComputedMeasure measure(String measureId) {
		return new MeasureComputedMeasure(measureId);
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
				Expression value2 = E.cast(measure2.getExpression(record, storedMeasures), double.class);
				return E.ifNe(value2, E.value(0.0),
						E.div(measure1.getExpression(record, storedMeasures), value2),
						E.value(0.0));
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

	public static final class ValueComputedMeasure implements ComputedMeasure {
		private final Object value;

		public ValueComputedMeasure(Object value) {this.value = value;}

		@Override
		public Class<?> getType(Map<String, Measure> storedMeasures) {
			return Primitives.unwrap(value.getClass());
		}

		@Override
		public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
			return E.value(value);
		}

		@Override
		public Set<String> getMeasureDependencies() {
			return Set.of();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ValueComputedMeasure other = (ValueComputedMeasure) o;
			return Objects.equals(value, other.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(value);
		}
	}

	public static final class MeasureComputedMeasure implements ComputedMeasure {
		private final String measureId;

		public MeasureComputedMeasure(String measureId) {this.measureId = measureId;}

		@Override
		public Class<?> getType(Map<String, Measure> storedMeasures) {
			return (Class<?>) storedMeasures.get(measureId).getFieldType().getDataType();
		}

		@Override
		public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
			return storedMeasures.get(measureId).valueOfAccumulator(E.property(record, measureId));
		}

		@Override
		public Set<String> getMeasureDependencies() {
			return Set.of(measureId);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			MeasureComputedMeasure other = (MeasureComputedMeasure) o;
			return measureId.equals(other.measureId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(measureId);
		}
	}
}
