import dto.CreateStringCountFunction;
import dto.ExtractStringFunction;
import dto.StringCountReducer;
import io.activej.dataflow.proto.serializer.FunctionSubtypeSerializer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.BinarySerializer;

import java.util.Comparator;
import java.util.function.Function;

import static io.activej.dataflow.proto.serializer.ProtobufUtils.ofObject;

public final class DataflowSerializersModule extends AbstractModule {

	@Provides
	BinarySerializer<CreateStringCountFunction> createStringCountFunctionSerializer() {
		return ofObject(CreateStringCountFunction::new);
	}

	@Provides
	BinarySerializer<ExtractStringFunction> extractStringFunctionSerializer() {
		return ofObject(ExtractStringFunction::new);
	}

	@Provides
	BinarySerializer<Function<?, ?>> functionSerializer(
			BinarySerializer<CreateStringCountFunction> createStringCountFunctionSerializer,
			BinarySerializer<ExtractStringFunction> extractStringFunctionSerializer
	) {
		FunctionSubtypeSerializer<Function<?, ?>> serializer = FunctionSubtypeSerializer.create();

		serializer.setSubtypeCodec(CreateStringCountFunction.class, createStringCountFunctionSerializer);
		serializer.setSubtypeCodec(ExtractStringFunction.class, extractStringFunctionSerializer);

		return serializer;
	}

	@Provides
	@SuppressWarnings("rawtypes")
	BinarySerializer<ReducerToResult> reducerToResultSerializer() {
		return ofObject(StringCountReducer::new);
	}

	@Provides
	BinarySerializer<Comparator<?>> comparatorSerializer() {
		return ofObject(Comparator::naturalOrder);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Provides
	BinarySerializer<Reducer<?, ?, ?, ?>> reducerSerializer(
			BinarySerializer<InputToAccumulator> inputToAccumulatorSerializer,
			BinarySerializer<AccumulatorToOutput> accumulatorToOutputSerializer
	) {
		FunctionSubtypeSerializer<Reducer> serializer = FunctionSubtypeSerializer.create();

		serializer.setSubtypeCodec(InputToAccumulator.class, inputToAccumulatorSerializer);
		serializer.setSubtypeCodec(AccumulatorToOutput.class, accumulatorToOutputSerializer);

		return (BinarySerializer<Reducer<?, ?, ?, ?>>) (BinarySerializer) serializer;
	}
}
