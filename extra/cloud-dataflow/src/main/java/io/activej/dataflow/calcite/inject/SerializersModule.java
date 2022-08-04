package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.*;
import io.activej.dataflow.calcite.aggregation.RecordReducer;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.dataflow.calcite.join.RecordKeyFunction;
import io.activej.dataflow.calcite.join.RecordKeyFunctionSerializer;
import io.activej.dataflow.calcite.utils.EqualObjectComparator;
import io.activej.dataflow.calcite.utils.RecordComparator;
import io.activej.dataflow.calcite.utils.RecordSchemeFunction;
import io.activej.dataflow.calcite.utils.ToZeroFunction;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.dataflow.proto.FunctionSubtypeSerializer;
import io.activej.dataflow.proto.calcite.RecordProjectionFnSerializer;
import io.activej.dataflow.proto.calcite.ReducerSerializer;
import io.activej.dataflow.proto.calcite.WherePredicateSerializer;
import io.activej.datastream.processor.StreamJoin;
import io.activej.datastream.processor.StreamReducers;
import io.activej.inject.Key;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.*;

import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.proto.ProtobufUtils.ofObject;

public final class SerializersModule extends AbstractModule {
	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	protected void configure() {
		bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to((schema, recordProjectionFnSerializer) -> {
					FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();
					for (DataflowTable<?> table : schema.getDataflowTableMap().values()) {
						Class<? extends Function<?, ?>> recordFunctionClass = (Class<? extends Function<?, ?>>) table.getRecordFunction().getClass();
						serializer.setSubtypeCodec(recordFunctionClass, table.getRecordFunctionSerializer());
					}
					serializer.setSubtypeCodec((Class) RecordKeyFunction.class, new RecordKeyFunctionSerializer<>());
					serializer.setSubtypeCodec((Class) Function.identity().getClass(), "identity", ofObject(Function::identity));
					serializer.setSubtypeCodec(RecordProjectionFn.class, recordProjectionFnSerializer);
					serializer.setSubtypeCodec(RecordSchemeFunction.class, ofObject(RecordSchemeFunction::getInstance));
					serializer.setSubtypeCodec(ToZeroFunction.class, ofObject(ToZeroFunction::getInstance));
					return serializer;
				},
				Key.of(DataflowSchema.class), new Key<BinarySerializer<RecordProjectionFn>>() {});

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
					serializer.setSubtypeCodec(EqualObjectComparator.class, ofObject(EqualObjectComparator::getInstance));

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


		bind(new Key<BinarySerializer<StreamJoin.Joiner<?, ?, ?, ?>>>() {}).to((schemeSerializer, definingClassLoader) -> {
			FunctionSubtypeSerializer<StreamJoin.Joiner> serializer = FunctionSubtypeSerializer.create();
			serializer.setSubtypeCodec(RecordInnerJoiner.class, new BinarySerializer<RecordInnerJoiner<?>>() {

				@Override
				public void encode(BinaryOutput out, RecordInnerJoiner<?> item) {
					RecordScheme scheme = item.getScheme();
					SerializableRecordScheme serializableRecordScheme = SerializableRecordScheme.fromRecordScheme(scheme);
					schemeSerializer.encode(out, serializableRecordScheme);
				}

				@Override
				public RecordInnerJoiner<?> decode(BinaryInput in) throws CorruptedDataException {
					SerializableRecordScheme serializableRecordScheme = schemeSerializer.decode(in);
					RecordScheme scheme = serializableRecordScheme.toRecordScheme(definingClassLoader);
					return RecordInnerJoiner.create(scheme);
				}
			});
			return (BinarySerializer) serializer;
		}, new Key<BinarySerializer<SerializableRecordScheme>>(){}, Key.of(DefiningClassLoader.class));

		bind(new Key<BinarySerializer<Record>>() {}).to(RecordSerializer::create, DefiningClassLoader.class, BinarySerializerModule.BinarySerializerLocator.class).asTransient();

		bind(new Key<BinarySerializer<RecordProjectionFn>>() {}).to(optionalClassLoader ->
						optionalClassLoader.isPresent() ?
								new RecordProjectionFnSerializer(optionalClassLoader.get()) :
								new RecordProjectionFnSerializer(),
				new Key<OptionalDependency<DefiningClassLoader>>() {});

		bind(SerializerBuilder.class).to(classLoader -> SerializerBuilder.create(classLoader)
				.with(Type.class, ctx -> new SerializerDefType()),
				DefiningClassLoader.class);

		bind(DefiningClassLoader.class).to(DefiningClassLoader::create);
	}
}
