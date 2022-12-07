package io.activej.dataflow.codec.module;

import io.activej.dataflow.codec.Subtype;
import io.activej.datastream.processor.StreamReducers.DeduplicateReducer;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;
import io.activej.streamcodecs.StreamInput;
import io.activej.streamcodecs.StreamOutput;

import java.io.IOException;

final class ReducerCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<MergeReducer<?, ?>> mergeReducer() {
		return StreamCodecs.singleton(new MergeReducer<>());
	}

	@Provides
	@Subtype(1)
	StreamCodec<DeduplicateReducer<?, ?>> deduplicateReducer() {
		return StreamCodecs.singleton(new DeduplicateReducer<>());
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
				reducerToResultSerializer.encode(output, item.getReducerToResult());
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
				reducerToResultSerializer.encode(out, item.getReducerToResult());
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
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public AccumulatorToAccumulator<?, ?, ?, ?> decode(StreamInput in) throws IOException {
				return new AccumulatorToAccumulator<>(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	@Subtype(5)
	StreamCodec<ReducerToResult.AccumulatorToOutput<?, ?, ?, ?>> accumulatorToOutput(
			OptionalDependency<StreamCodec<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer
	) {
		StreamCodec<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput out, ReducerToResult.AccumulatorToOutput<?, ?, ?, ?> item) throws IOException {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public ReducerToResult.AccumulatorToOutput<?, ?, ?, ?> decode(StreamInput in) throws IOException {
				return new ReducerToResult.AccumulatorToOutput<>(reducerToResultSerializer.decode(in));
			}
		};
	}
}
