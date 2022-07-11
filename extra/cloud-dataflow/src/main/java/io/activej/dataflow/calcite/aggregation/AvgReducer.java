package io.activej.dataflow.calcite.aggregation;

import io.activej.record.RecordScheme;
import org.jetbrains.annotations.NotNull;

public class AvgReducer extends FieldReducer<Number, Double, AvgReducer.AvgAccumulator> {
	public AvgReducer(int fieldIndex) {
		super(fieldIndex);
	}

	@Override
	public Class<Double> getResultClass(Class<Number> inputClass) {
		return double.class;
	}

	@Override
	public String getName() {
		return "AVG";
	}

	@Override
	public AvgAccumulator createAccumulator(RecordScheme key) {
		return new AvgAccumulator();
	}

	@Override
	protected AvgAccumulator doAccumulate(AvgAccumulator accumulator, @NotNull Number fieldValue) {
		accumulator.add(fieldValue.doubleValue());
		return accumulator;
	}

	@Override
	public Double produceResult(AvgAccumulator accumulator) {
		return accumulator.getAvg();
	}

	public static final class AvgAccumulator {
		private double sum;
		private long count;

		private void add(double value) {
			sum += value;
			count++;
		}

		public double getAvg() {
			if (count == 0) return 0d;
			return sum / count;
		}
	}
}
