package dto;

import io.activej.datastream.processor.reducer.ReducerToAccumulator;

public final class StringCountReducer extends ReducerToAccumulator<String, StringCount, StringCount> {
	@Override
	public StringCount createAccumulator(String key) {
		return new StringCount(key, 0);
	}

	@Override
	public StringCount accumulate(StringCount accumulator, StringCount value) {
		accumulator.count += value.count;
		return accumulator;
	}

	@Override
	public StringCount combine(StringCount accumulator, StringCount anotherAccumulator) {
		accumulator.count += anotherAccumulator.count;
		return accumulator;
	}
}
