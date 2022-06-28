package io.activej.dataflow.calcite.inject;

import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.calcite.RecordSerializer;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.dataflow.calcite.join.RecordKeyFunction;
import io.activej.dataflow.calcite.join.RecordKeyFunctionSerializer;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.dataflow.proto.FunctionSubtypeSerializer;
import io.activej.datastream.processor.StreamJoin;
import io.activej.datastream.processor.StreamReducers;
import io.activej.inject.Key;
import io.activej.inject.KeyPattern;
import io.activej.inject.module.AbstractModule;
import io.activej.record.Record;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.proto.ProtobufUtils.ofObject;

public final class SerializersModule extends AbstractModule {
	@Override
	@SuppressWarnings({"rawtypes", "NullableProblems", "unchecked"})
	protected void configure() {
		transform(new KeyPattern<BinarySerializer<Function<?, ?>>>() {}, (bindings, scope, key, binding) -> binding
				.addDependencies(SerializerBuilder.class)
				.mapInstance(List.of(Key.of(SerializerBuilder.class)), (deps, serializer) -> {
					FunctionSubtypeSerializer<Function<?, ?>> subtypeSerializer = (FunctionSubtypeSerializer<Function<?, ?>>) serializer;
					subtypeSerializer.setSubtypeCodec((Class) RecordKeyFunction.class, new RecordKeyFunctionSerializer<>());
					subtypeSerializer.setSubtypeCodec(RecordProjectionFn.class, ((SerializerBuilder) deps[0]).build(RecordProjectionFn.class));
					return subtypeSerializer;
				}));

		bind(new Key<BinarySerializer<Predicate<?>>>() {}).to(serializerBuilder -> ((BinarySerializer) serializerBuilder.build(WherePredicate.class)),
				SerializerBuilder.class);

		bind(new Key<BinarySerializer<Comparator<?>>>() {}).toInstance(ofObject(Comparator::naturalOrder));

		bind(new Key<BinarySerializer<StreamReducers.Reducer<?, ?, ?, ?>>>() {}).to((inputToAccumulator, accumulatorToOutput, mergeReducer) -> {
					FunctionSubtypeSerializer<StreamReducers.Reducer> serializer = FunctionSubtypeSerializer.create();
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.InputToAccumulator.class, inputToAccumulator);
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.AccumulatorToOutput.class, accumulatorToOutput);
					serializer.setSubtypeCodec(StreamReducers.MergeReducer.class, mergeReducer);
					return (BinarySerializer) serializer;
				},
				new Key<BinarySerializer<StreamReducers.ReducerToResult.InputToAccumulator>>() {},
				new Key<BinarySerializer<StreamReducers.ReducerToResult.AccumulatorToOutput>>() {},
				new Key<BinarySerializer<StreamReducers.MergeReducer>>() {});

		bind(new Key<BinarySerializer<StreamJoin.Joiner<?, ?, ?, ?>>>() {}).to(() -> {
			FunctionSubtypeSerializer<StreamJoin.Joiner> serializer = FunctionSubtypeSerializer.create();
			serializer.setSubtypeCodec(RecordInnerJoiner.class, ofObject(RecordInnerJoiner::new));
			return (BinarySerializer) serializer;
		});

		bind(new Key<BinarySerializer<StreamReducers.ReducerToResult>>() {}).to(FunctionSubtypeSerializer::create);

		bind(new Key<BinarySerializer<Record>>() {}).to(RecordSerializer::create, BinarySerializerModule.BinarySerializerLocator.class).asTransient();

		bind(SerializerBuilder.class).to(SerializerBuilder::create);
	}
}
