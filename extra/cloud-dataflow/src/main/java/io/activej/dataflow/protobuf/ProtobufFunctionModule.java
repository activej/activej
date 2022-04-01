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

package io.activej.dataflow.protobuf;

import io.activej.datastream.processor.StreamJoin;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamReducers.DeduplicateReducer;
import io.activej.datastream.processor.StreamReducers.MergeReducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.protobuf.ProtobufUtils.ofObject;
import static io.activej.types.Types.parameterizedType;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class ProtobufFunctionModule extends AbstractModule {

	private static final Comparator<?> NATURAL_ORDER = Comparator.naturalOrder();
	private static final Class<?> NATURAL_ORDER_CLASS = NATURAL_ORDER.getClass();

	private ProtobufFunctionModule() {
	}

	public static ProtobufFunctionModule create() {
		return new ProtobufFunctionModule();
	}

	public interface FunctionNameFactory {
		@Nullable String getName(Class<?> functionType);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void configure() {
		bind(Key.ofType(parameterizedType(BinarySerializer.class, NATURAL_ORDER_CLASS)))
				.toInstance(ofObject(() -> NATURAL_ORDER));

		generate(BinarySerializer.class, (bindings, scope, key) -> {
			Class<Object> type = key.getTypeParameter(0).getRawType();
			return Binding.to(args -> {
						Injector injector = (Injector) args[0];
						FunctionNameFactory names = ((OptionalDependency<FunctionNameFactory>) args[1]).orElse($ -> null);

						Set<Class<?>> subtypes = new HashSet<>();

						Injector i = injector;
						while (i != null) {
							for (Key<?> k : i.getBindings().keySet()) {
								if (k.getRawType() != BinarySerializer.class) {
									continue;
								}
								Class<?> subtype = k.getTypeParameter(0).getRawType();
								if (type != subtype && type.isAssignableFrom(subtype)) {
									subtypes.add(subtype);
								}
							}
							i = i.getParent();
						}

						FunctionSubtypeSerializer<Object> subtypeSerializer = FunctionSubtypeSerializer.create();
						for (Class<?> subtype : subtypes) {
							BinarySerializer<?> codec = injector.getInstance(Key.ofType(parameterizedType(BinarySerializer.class, subtype)));
							String name = names.getName(subtype);
							if (name != null) {
								subtypeSerializer.setSubtypeCodec(subtype, name, codec);
							} else {
								subtypeSerializer.setSubtypeCodec(subtype, codec);
							}
						}
						return subtypeSerializer;
					}, new Key<?>[]{
							Key.of(Injector.class),
							new Key<OptionalDependency<FunctionNameFactory>>() {}}
			);
		});
	}

	@Provides
	FunctionSerializer serializer(
			BinarySerializer<Function<?, ?>> functionSerializer,
			BinarySerializer<Predicate<?>> predicateSerializer,
			BinarySerializer<Comparator<?>> comparatorSerializer,
			BinarySerializer<StreamReducers.Reducer<?, ?, ?, ?>> reducerSerializer,
			BinarySerializer<StreamJoin.Joiner<?, ?, ?, ?>> joinerSerializer
	) {
		return new FunctionSerializer(functionSerializer, predicateSerializer, comparatorSerializer, reducerSerializer, joinerSerializer);
	}

	@Provides
	FunctionNameFactory functionNameFactory() {
		return subtype -> subtype == NATURAL_ORDER_CLASS ? "Comparator.naturalOrder" : null;
	}

	@Provides
	BinarySerializer<DeduplicateReducer> mergeDistinctReducer() {
		return ofObject(DeduplicateReducer::new);
	}

	@Provides
	BinarySerializer<MergeReducer> mergeSortReducer() {
		return ofObject(MergeReducer::new);
	}

	@Provides
	BinarySerializer<InputToAccumulator> inputToAccumulator(BinarySerializer<ReducerToResult> reducerToResultSerializer) {
		return new BinarySerializer<InputToAccumulator>() {
			@Override
			public void encode(BinaryOutput out, InputToAccumulator item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public InputToAccumulator decode(BinaryInput in) throws CorruptedDataException {
				return new InputToAccumulator(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	BinarySerializer<InputToOutput> inputToOutput(BinarySerializer<ReducerToResult> reducerToResultSerializer) {
		return new BinarySerializer<InputToOutput>() {
			@Override
			public void encode(BinaryOutput out, InputToOutput item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public InputToOutput decode(BinaryInput in) throws CorruptedDataException {
				return new InputToOutput(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	BinarySerializer<AccumulatorToAccumulator> accumulatorToAccumulator(BinarySerializer<ReducerToResult> reducerToResultSerializer) {
		return new BinarySerializer<AccumulatorToAccumulator>() {
			@Override
			public void encode(BinaryOutput out, AccumulatorToAccumulator item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public AccumulatorToAccumulator decode(BinaryInput in) throws CorruptedDataException {
				return new AccumulatorToAccumulator(reducerToResultSerializer.decode(in));
			}
		};
	}

	@Provides
	BinarySerializer<AccumulatorToOutput> accumulatorToOutput(BinarySerializer<ReducerToResult> reducerToResultSerializer) {
		return new BinarySerializer<AccumulatorToOutput>() {
			@Override
			public void encode(BinaryOutput out, AccumulatorToOutput item) {
				reducerToResultSerializer.encode(out, item.getReducerToResult());
			}

			@Override
			public AccumulatorToOutput decode(BinaryInput in) throws CorruptedDataException {
				return new AccumulatorToOutput(reducerToResultSerializer.decode(in));
			}
		};
	}
}
