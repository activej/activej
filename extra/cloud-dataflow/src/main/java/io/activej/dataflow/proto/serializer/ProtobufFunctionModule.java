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

package io.activej.dataflow.proto.serializer;

import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.DeduplicateReducer;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.proto.serializer.ProtobufUtils.ofObject;

public final class ProtobufFunctionModule extends AbstractModule {

	private ProtobufFunctionModule() {
	}

	public static ProtobufFunctionModule create() {
		return new ProtobufFunctionModule();
	}

	@Provides
	FunctionSerializer serializer(
			OptionalDependency<BinarySerializer<Function<?, ?>>> functionSerializer,
			OptionalDependency<BinarySerializer<Predicate<?>>> predicateSerializer,
			OptionalDependency<BinarySerializer<Comparator<?>>> comparatorSerializer,
			OptionalDependency<BinarySerializer<Reducer<?, ?, ?, ?>>> reducerSerializer,
			OptionalDependency<BinarySerializer<Joiner<?, ?, ?, ?>>> joinerSerializer
	) {
		FunctionSerializer serializer = new FunctionSerializer();
		if (functionSerializer.isPresent()) serializer.setFunctionSerializer(functionSerializer.get());
		if (predicateSerializer.isPresent()) serializer.setPredicateSerializer(predicateSerializer.get());
		if (comparatorSerializer.isPresent()) serializer.setComparatorSerializer(comparatorSerializer.get());
		if (reducerSerializer.isPresent()) serializer.setReducerSerializer(reducerSerializer.get());
		if (joinerSerializer.isPresent()) serializer.setJoinerSerializer(joinerSerializer.get());
		return serializer;
	}

	@Provides
	BinarySerializer<DeduplicateReducer<?, ?>> mergeDistinctReducer() {
		return ofObject(DeduplicateReducer::new);
	}

	@Provides
	BinarySerializer<MergeReducer<?, ?>> mergeSortReducer() {
		return ofObject(MergeReducer::new);
	}

	@Provides
	BinarySerializer<InputToAccumulator<?, ?, ?, ?>> inputToAccumulator(OptionalDependency<BinarySerializer<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer) {
		BinarySerializer<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, InputToAccumulator<?, ?, ?, ?> item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public InputToAccumulator<?, ?, ?, ?> decode(BinaryInput in) throws CorruptedDataException {
				return new InputToAccumulator<>(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	BinarySerializer<InputToOutput<?, ?, ?, ?>> inputToOutput(OptionalDependency<BinarySerializer<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer) {
		BinarySerializer<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, InputToOutput<?, ?, ?, ?> item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public InputToOutput<?, ?, ?, ?> decode(BinaryInput in) throws CorruptedDataException {
				return new InputToOutput<>(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	BinarySerializer<AccumulatorToAccumulator<?, ?, ?, ?>> accumulatorToAccumulator(OptionalDependency<BinarySerializer<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer) {
		BinarySerializer<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, AccumulatorToAccumulator<?, ?, ?, ?> item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public AccumulatorToAccumulator<?, ?, ?, ?> decode(BinaryInput in) throws CorruptedDataException {
				return new AccumulatorToAccumulator<>(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	BinarySerializer<AccumulatorToOutput<?, ?, ?, ?>> accumulatorToOutput(OptionalDependency<BinarySerializer<ReducerToResult<?, ?, ?, ?>>> optionalReducerToResultSerializer) {
		BinarySerializer<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer = optionalReducerToResultSerializer.get();
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, AccumulatorToOutput<?, ?, ?, ?> item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public AccumulatorToOutput<?, ?, ?, ?> decode(BinaryInput in) throws CorruptedDataException {
				return new AccumulatorToOutput<>(reducerToResultSerializer.decode(in));
			}
		};
	}
}
