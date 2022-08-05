package io.activej.dataflow.calcite.aggregation;

import io.activej.record.Record;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import org.jetbrains.annotations.NotNull;

public class AvgReducer extends FieldReducer<Number, Double, AvgReducer.AvgAccumulator> {
	public AvgReducer(int fieldIndex) {
		super(fieldIndex);
	}

	@Override
	public Class<Double> getResultClass(Class<AvgAccumulator> accumulatorClass) {
		return double.class;
	}

	@Override
	public Class<AvgAccumulator> getAccumulatorClass(Class<Number> inputClass) {
		return AvgAccumulator.class;
	}

	@Override
	public String getName(String fieldName) {
		return "AVG(" + fieldName + ')';
	}

	@Override
	public AvgAccumulator createAccumulator(Record key) {
		return new AvgAccumulator(0d, 0L);
	}

	@Override
	protected AvgAccumulator doAccumulate(AvgAccumulator accumulator, @NotNull Number fieldValue) {
		accumulator.add(fieldValue.doubleValue());
		return accumulator;
	}

	@Override
	public AvgAccumulator combine(AvgAccumulator accumulator, AvgAccumulator anotherAccumulator) {
		return new AvgAccumulator(
				accumulator.sum + anotherAccumulator.sum,
				accumulator.count + anotherAccumulator.count
		);
	}

	@Override
	public Double produceResult(AvgAccumulator accumulator) {
		return accumulator.getAvg();
	}

	public static final class AvgAccumulator {
		private double sum;
		private long count;

		public AvgAccumulator(@Deserialize("sum") double sum, @Deserialize("count") long count) {
			this.sum = sum;
			this.count = count;
		}

		private void add(double value) {
			sum += value;
			count++;
		}

		public double getAvg() {
			if (count == 0) return 0d;
			return sum / count;
		}

		@Serialize(order = 1)
		public double getSum() {
			return sum;
		}

		@Serialize(order = 2)
		public long getCount() {
			return count;
		}
	}
}
