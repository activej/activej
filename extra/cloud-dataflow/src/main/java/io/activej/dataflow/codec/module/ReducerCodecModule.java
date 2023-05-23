package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.datastream.processor.reducer.ReducerToResult;
import io.activej.datastream.processor.reducer.impl.*;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.serializer.stream.StreamInput;
import io.activej.serializer.stream.StreamOutput;

import java.io.IOException;

public final class ReducerCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<Merge<?, ?>> mergeReducer() {
		return StreamCodecs.singleton(new Merge<>());
	}

	@Provides
	@Subtype(1)
	StreamCodec<Deduplicate<?, ?>> deduplicateReducer() {
		return StreamCodecs.singleton(new Deduplicate<>());
	}

	@Provides
	@Subtype(2)
	StreamCodec<InputToAccumulator<?, ?, ?, ?>> inputToAccumulator(
		OptionalDependency<StreamCodec<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer
	) {
		StreamCodec<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, InputToAccumulator<?, ?, ?, ?> item) throws IOException {
				reducerToResultSerializer.encode(output, item.reducerToResult);
			}

			@Override
			public InputToAccumulator<?, ?, ?, ?> decode(StreamInput input) throws IOException {
				return new InputToAccumulator<>(reducerToResultSerializer.decode(input));
			}
		};
	}

	@Provides
	@Subtype(3)
	StreamCodec<InputToOutput<?, ?, ?, ?>> inputToOutput(
		OptionalDependency<StreamCodec<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer
	) {
		StreamCodec<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput out, InputToOutput<?, ?, ?, ?> item) throws IOException {
				reducerToResultSerializer.encode(out, item.reducerToResult);
			}

			@Override
			public InputToOutput<?, ?, ?, ?> decode(StreamInput in) throws IOException {
				return new InputToOutput<>(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	@Subtype(4)
	StreamCodec<AccumulatorToAccumulator<?, ?, ?, ?>> accumulatorToAccumulator(
		OptionalDependency<StreamCodec<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer
	) {
		StreamCodec<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput out, AccumulatorToAccumulator<?, ?, ?, ?> item) throws IOException {
				reducerToResultSerializer.encode(out, item.reducerToResult);
			}

			@Override
			public AccumulatorToAccumulator<?, ?, ?, ?> decode(StreamInput in) throws IOException {
				return new AccumulatorToAccumulator<>(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	@Subtype(5)
	StreamCodec<AccumulatorToOutput<?, ?, ?, ?>> accumulatorToOutput(
		OptionalDependency<StreamCodec<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer
	) {
		StreamCodec<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput out, AccumulatorToOutput<?, ?, ?, ?> item) throws IOException {
				reducerToResultSerializer.encode(out, item.reducerToResult);
			}

			@Override
			public AccumulatorToOutput<?, ?, ?, ?> decode(StreamInput in) throws IOException {
				return new AccumulatorToOutput<>(reducerToResultSerializer.decode(in));
			}
		};
	}
}
