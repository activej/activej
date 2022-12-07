import dto.CreateStringCountFunction;
import dto.ExtractStringFunction;
import dto.StringCountReducer;
import io.activej.dataflow.codec.Subtype;
import io.activej.datastream.processor.StreamReducers.ReducerToResult;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.streamcodecs.StreamCodec;
import io.activej.streamcodecs.StreamCodecs;

import java.util.Comparator;

public final class DataflowSerializersModule extends AbstractModule {

	@Provides
	@Subtype(0)
	StreamCodec<CreateStringCountFunction> createStringCountFunctionSerializer() {
		return StreamCodecs.singleton(new CreateStringCountFunction());
	}

	@Provides
	@Subtype(1)
	StreamCodec<ExtractStringFunction> extractStringFunctionSerializer() {
		return StreamCodecs.singleton(new ExtractStringFunction());
	}

	@Provides
	StreamCodec<ReducerToResult<?, ?, ?, ?>> reducerToResultSerializer() {
		return StreamCodecs.singleton(new StringCountReducer());
	}

	@Provides
	StreamCodec<Comparator<?>> comparatorSerializer() {
		return StreamCodecs.singleton(Comparator.naturalOrder());
	}
}
