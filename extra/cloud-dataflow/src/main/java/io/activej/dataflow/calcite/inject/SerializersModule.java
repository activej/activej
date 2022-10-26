package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.RecordProjectionFn;
import io.activej.dataflow.calcite.RecordSerializer;
import io.activej.dataflow.calcite.aggregation.RecordReducer;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.dataflow.calcite.join.RecordKeyFunction;
import io.activej.dataflow.calcite.join.RecordKeyFunctionSerializer;
import io.activej.dataflow.calcite.operand.OperandIfNull;
import io.activej.dataflow.calcite.operand.OperandListGet;
import io.activej.dataflow.calcite.operand.OperandMapGet;
import io.activej.dataflow.calcite.utils.*;
import io.activej.dataflow.calcite.utils.time.SqlDateBinarySerializer;
import io.activej.dataflow.calcite.utils.time.SqlTimeBinarySerializer;
import io.activej.dataflow.calcite.utils.time.SqlTimestampBinarySerializer;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.dataflow.jdbc.driver.time.TimeUtils;
import io.activej.dataflow.proto.calcite.serializer.*;
import io.activej.dataflow.proto.serializer.CustomNodeSerializer;
import io.activej.dataflow.proto.serializer.FunctionSubtypeSerializer;
import io.activej.datastream.processor.StreamJoin;
import io.activej.datastream.processor.StreamReducers;
import io.activej.inject.Key;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.proto.serializer.ProtobufUtils.ofObject;

public final class SerializersModule extends AbstractModule {

	static {
		OperandIfNull.register();
		OperandListGet.register();
		OperandMapGet.register();
		TimeUtils.registerTimeSerializers();
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	protected void configure() {
		bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to((schema, recordProjectionFnSerializer) -> {
					FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();
					serializer.setSubtypeCodec((Class) NamedRecordFunction.class, new NamedRecordFunctionSerializer(schema));
					serializer.setSubtypeCodec((Class) RecordKeyFunction.class, new RecordKeyFunctionSerializer<>());
					serializer.setSubtypeCodec((Class) Function.identity().getClass(), "identity", ofObject(Function::identity));
					serializer.setSubtypeCodec(RecordProjectionFn.class, recordProjectionFnSerializer);
					return serializer;
				},
				Key.of(DataflowSchema.class), new Key<BinarySerializer<RecordProjectionFn>>() {});

		bind(new Key<BinarySerializer<Predicate<?>>>() {}).to(optionalClassLoader -> (BinarySerializer)
						(optionalClassLoader.isPresent() ?
								new WherePredicateSerializer(optionalClassLoader.get()) :
								new WherePredicateSerializer()),
				new Key<OptionalDependency<DefiningClassLoader>>() {});

		bind(new Key<BinarySerializer<Comparator<?>>>() {}).to(serializerLocator -> {
					FunctionSubtypeSerializer<Comparator> serializer = FunctionSubtypeSerializer.create();

					serializer.setSubtypeCodec(Comparator.naturalOrder().getClass(), "natural", ofObject(Comparator::naturalOrder));
					serializer.setSubtypeCodec(Comparator.reverseOrder().getClass(), "reverse", ofObject(Comparator::reverseOrder));

					serializer.setSubtypeCodec(RecordSortComparator.class, serializerLocator.get(RecordSortComparator.class));
					serializer.setSubtypeCodec(RecordKeyComparator.class, ofObject(RecordKeyComparator::getInstance));

					return (BinarySerializer) serializer;
				},
				BinarySerializerModule.BinarySerializerLocator.class);

		bind(new Key<BinarySerializer<StreamReducers.Reducer<?, ?, ?, ?>>>() {}).to((schema, inputToAccumulator, inputToOutput, accumulatorToOutput, mergeReducer, deduplicateReducer) -> {
					FunctionSubtypeSerializer<StreamReducers.Reducer> serializer = FunctionSubtypeSerializer.create();
					serializer.setSubtypeCodec(NamedReducer.class, new NamedReducerFunctionSerializer(schema));
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.InputToAccumulator.class, inputToAccumulator);
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.InputToOutput.class, inputToOutput);
					serializer.setSubtypeCodec(StreamReducers.ReducerToResult.AccumulatorToOutput.class, accumulatorToOutput);
					serializer.setSubtypeCodec(StreamReducers.MergeReducer.class, mergeReducer);
					serializer.setSubtypeCodec(StreamReducers.DeduplicateReducer.class, deduplicateReducer);
					return (BinarySerializer) serializer;
				},
				Key.of(DataflowSchema.class),
				new Key<BinarySerializer<StreamReducers.ReducerToResult.InputToAccumulator>>() {},
				new Key<BinarySerializer<StreamReducers.ReducerToResult.InputToOutput>>() {},
				new Key<BinarySerializer<StreamReducers.ReducerToResult.AccumulatorToOutput>>() {},
				new Key<BinarySerializer<StreamReducers.MergeReducer>>() {},
				new Key<BinarySerializer<StreamReducers.DeduplicateReducer>>() {}
		);

		bind(new Key<BinarySerializer<StreamReducers.ReducerToResult<?, ?, ?, ?>>>() {}).to(optionalClassLoader -> {
			FunctionSubtypeSerializer<StreamReducers.ReducerToResult> serializer = FunctionSubtypeSerializer.create();
			serializer.setSubtypeCodec(RecordReducer.class, optionalClassLoader.isPresent() ?
					new ReducerSerializer(optionalClassLoader.get()) :
					new ReducerSerializer());
			return (BinarySerializer) serializer;
		}, new Key<OptionalDependency<DefiningClassLoader>>() {});


		bind(new Key<BinarySerializer<StreamJoin.Joiner<?, ?, ?, ?>>>() {}).to(schemeSerializer -> {
			FunctionSubtypeSerializer<StreamJoin.Joiner> serializer = FunctionSubtypeSerializer.create();
			serializer.setSubtypeCodec(RecordInnerJoiner.class, new BinarySerializer<RecordInnerJoiner<?>>() {

				@Override
				public void encode(BinaryOutput out, RecordInnerJoiner<?> item) {
					schemeSerializer.encode(out, item.getScheme());
					schemeSerializer.encode(out, item.getLeft());
					schemeSerializer.encode(out, item.getRight());
				}

				@Override
				public RecordInnerJoiner<?> decode(BinaryInput in) throws CorruptedDataException {
					RecordScheme scheme = schemeSerializer.decode(in);
					RecordScheme left = schemeSerializer.decode(in);
					RecordScheme right = schemeSerializer.decode(in);
					return RecordInnerJoiner.create(scheme, left, right);
				}
			});
			return (BinarySerializer) serializer;
		}, new Key<BinarySerializer<RecordScheme>>() {});

		bind(new Key<BinarySerializer<RecordScheme>>() {}).to(RecordSchemeSerializer::new, DefiningClassLoader.class);
		bind(new Key<BinarySerializer<Record>>() {}).to(RecordSerializer::create, BinarySerializerModule.BinarySerializerLocator.class).asTransient();

		bind(new Key<BinarySerializer<RecordProjectionFn>>() {}).to(optionalClassLoader ->
						optionalClassLoader.isPresent() ?
								new RecordProjectionFnSerializer(optionalClassLoader.get()) :
								new RecordProjectionFnSerializer(),
				new Key<OptionalDependency<DefiningClassLoader>>() {});

		bind(CustomNodeSerializer.class).to(optionalClassLoader ->
						optionalClassLoader.isPresent() ?
								new CalciteNodeSerializer(optionalClassLoader.get()) :
								new CalciteNodeSerializer(),
				new Key<OptionalDependency<DefiningClassLoader>>() {});

		bind(new Key<BinarySerializer<Date>>() {}).toInstance(SqlDateBinarySerializer.getInstance()).asEager();
		bind(new Key<BinarySerializer<Time>>() {}).toInstance(SqlTimeBinarySerializer.getInstance()).asEager();
		bind(new Key<BinarySerializer<Timestamp>>() {}).toInstance(SqlTimestampBinarySerializer.getInstance()).asEager();

		bind(new Key<BinarySerializer<BigDecimal>>() {}).toInstance(BigDecimalSerializer.getInstance()).asEager();

		bind(DefiningClassLoader.class).to(DefiningClassLoader::create);
	}
}
