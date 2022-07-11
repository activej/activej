package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.calcite.RecordSerializer;
import io.activej.dataflow.calcite.aggregation.EqualObjectComparator;
import io.activej.dataflow.calcite.aggregation.RecordReducer;
import io.activej.dataflow.calcite.aggregation.RecordSchemeFunction;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.dataflow.calcite.join.RecordKeyFunction;
import io.activej.dataflow.calcite.join.RecordKeyFunctionSerializer;
import io.activej.dataflow.calcite.sort.RecordComparator;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.dataflow.proto.FunctionSubtypeSerializer;
import io.activej.dataflow.proto.calcite.ReducerSerializer;
import io.activej.dataflow.proto.calcite.WherePredicateSerializer;
import io.activej.datastream.processor.StreamJoin;
import io.activej.datastream.processor.StreamReducers;
import io.activej.inject.Key;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.record.Record;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.proto.ProtobufUtils.ofObject;

public final class SerializersModule extends AbstractModule {
	@Override
	@SuppressWarnings({"rawtypes", "NullableProblems", "unchecked"})
	protected void configure() {
		bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to((schema, serializerBuilder) -> {
					FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();
					for (DataflowTable<?> table : schema.getDataflowTableMap().values()) {
						Class<? extends Function<?, ?>> recordFunctionClass = (Class<? extends Function<?, ?>>) table.getRecordFunction().getClass();
						serializer.setSubtypeCodec(recordFunctionClass, table.getRecordFunctionSerializer());
					}
					serializer.setSubtypeCodec((Class) RecordKeyFunction.class, new RecordKeyFunctionSerializer<>());
					serializer.setSubtypeCodec((Class) Function.identity().getClass(), "identity", ofObject(Function::identity));
					serializer.setSubtypeCodec(RecordProjectionFn.class, serializerBuilder.build(RecordProjectionFn.class));
					serializer.setSubtypeCodec(RecordSchemeFunction.class, ofObject(RecordSchemeFunction::new));
					return serializer;
				},
				DataflowSchema.class, SerializerBuilder.class);

		bind(new Key<BinarySerializer<Predicate<?>>>() {}).to(optionalClassLoader -> (BinarySerializer)
						(optionalClassLoader.isPresent() ?
								new WherePredicateSerializer(optionalClassLoader.get()) :
								new WherePredicateSerializer()),
				new Key<OptionalDependency<DefiningClassLoader>>() {});

		bind(new Key<BinarySerializer<Comparator<?>>>() {}).to(serializerBuilder -> {
					FunctionSubtypeSerializer<Comparator> serializer = FunctionSubtypeSerializer.create();

					serializer.setSubtypeCodec(Comparator.naturalOrder().getClass(), "natural", ofObject(Comparator::naturalOrder));
					serializer.setSubtypeCodec(Comparator.reverseOrder().getClass(), "reverse", ofObject(Comparator::reverseOrder));

					serializer.setSubtypeCodec(RecordComparator.class, serializerBuilder.build(RecordComparator.class));
					serializer.setSubtypeCodec(EqualObjectComparator.class, ofObject(EqualObjectComparator::new));

					return (BinarySerializer) serializer;
				},
				SerializerBuilder.class);

		bind(new Key<BinarySerializer<StreamReducers.Reducer<?, ?, ?, ?>>>() {}).to((inputToAccumulator, inputToOutput, accumulatorToOutput, mergeReducer) -> {
					FunctionSubtypeSerializer<StreamReducers.Reducer> serializer = FunctionSubtypeSerializer.create();
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.InputToAccumulator.class, inputToAccumulator);
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.InputToOutput.class, inputToOutput);
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.AccumulatorToOutput.class, accumulatorToOutput);
					serializer.setSubtypeCodec(StreamReducers.MergeReducer.class, mergeReducer);
					return (BinarySerializer) serializer;
				},
				new Key<BinarySerializer<StreamReducers.ReducerToResult.InputToAccumulator>>() {},
				new Key<BinarySerializer<StreamReducers.ReducerToResult.InputToOutput>>() {},
				new Key<BinarySerializer<StreamReducers.ReducerToResult.AccumulatorToOutput>>() {},
				new Key<BinarySerializer<StreamReducers.MergeReducer>>() {});

		bind(new Key<BinarySerializer<StreamReducers.ReducerToResult<?, ?, ?, ?>>>() {}).to(() -> {
					FunctionSubtypeSerializer<StreamReducers.ReducerToResult> serializer = FunctionSubtypeSerializer.create();
					serializer.setSubtypeCodec(RecordReducer.class, new ReducerSerializer());
					return (BinarySerializer) serializer;
				});


		bind(new Key<BinarySerializer<StreamJoin.Joiner<?, ?, ?, ?>>>() {}).to(() -> {
			FunctionSubtypeSerializer<StreamJoin.Joiner> serializer = FunctionSubtypeSerializer.create();
			serializer.setSubtypeCodec(RecordInnerJoiner.class, ofObject(RecordInnerJoiner::new));
			return (BinarySerializer) serializer;
		});

		bind(new Key<BinarySerializer<Record>>() {}).to(RecordSerializer::create, BinarySerializerModule.BinarySerializerLocator.class).asTransient();

		bind(SerializerBuilder.class).to(SerializerBuilder::create);
	}
}
